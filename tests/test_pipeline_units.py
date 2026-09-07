"""Tests unitaires des règles critiques du pipeline, sans accès réseau."""

import json
import logging
import sys
import tempfile
import unittest
from collections import Counter
from io import StringIO
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from extract_fakeddit import (
    FIRST_DATA_LINE,
    iter_tsv_rows,
    read_fakeddit_cursor,
    write_fakeddit_cursor,
)
from init_airflow_env import build_configuration

from collect_kpis import (
    count_multimodal_publications,
    manifest_counts,
    parse_task_log,
)
from common import write_json_atomic
from load_postgres import (
    create_table_sql,
    upsert_sql,
    validate_table_name,
)
from transform_data import (
    FINAL_COLUMNS,
    RecordValidationError,
    make_dataframe,
    normalize_datetime,
    resolve_input_files,
    transform_batch,
    validate_configuration,
)


class CommonTests(unittest.TestCase):
    """Vérifie les utilitaires partagés."""

    def test_write_json_atomic_writes_complete_payload(self) -> None:
        """L'écriture JSON produit le contenu attendu sans fichier temporaire."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"
            write_json_atomic({"clé": "valeur"}, path, sort_keys=True)

            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8")),
                {"clé": "valeur"},
            )
            self.assertFalse(path.with_suffix(".json.part").exists())


class TransformationTests(unittest.TestCase):
    """Vérifie normalisation, paramètres et contrat final."""

    def test_normalize_datetime_supports_iso_rss_and_unix(self) -> None:
        """Les trois formats de date utilisés finissent en UTC."""
        values = (
            "2026-09-02T12:00:00Z",
            "Wed, 02 Sep 2026 12:00:00 +0000",
            "1788350400",
        )
        for value in values:
            with self.subTest(value=value):
                self.assertTrue(normalize_datetime(value).endswith("Z"))

    def test_normalize_datetime_rejects_missing_value(self) -> None:
        """Une publication sans date est rejetée explicitement."""
        with self.assertRaises(RecordValidationError):
            normalize_datetime(None)

    def test_validate_configuration_rejects_unknown_policy(self) -> None:
        """Un appel programmatique ne peut pas contourner les choix de la CLI."""
        with self.assertRaisesRegex(ValueError, "duplicate_policy"):
            validate_configuration("inconnue", "both")

    def test_resolve_input_files_requires_current_batch_for_each_source(self) -> None:
        """Seuls les quatre lots explicitement fournis composent le run."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = []
            for source in ("theconversation", "fakeddit", "newsdata", "politifact"):
                path = root / f"{source}.json"
                path.write_text(
                    json.dumps({"source": source, "records": []}),
                    encoding="utf-8",
                )
                paths.append(path)

            selected = resolve_input_files(paths)

        self.assertEqual(
            [source for source, _ in selected],
            ["newsdata", "politifact", "fakeddit", "theconversation"],
        )

    def test_resolve_input_files_rejects_incomplete_run(self) -> None:
        """Un run incomplet ne peut pas alimenter le contrat consolidé."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "newsdata.json"
            path.write_text(
                json.dumps({"source": "newsdata", "records": []}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "exactement un lot courant"):
                resolve_input_files([path])

    def test_resolve_input_files_rejects_two_batches_for_one_source(self) -> None:
        """Un run incrémental refuse un ensemble qui ne couvre pas quatre sources."""
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.json"
            second = Path(directory) / "second.json"
            payload = json.dumps({"source": "newsdata", "records": []})
            first.write_text(payload, encoding="utf-8")
            second.write_text(payload, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exactement un lot courant"):
                resolve_input_files([first, second])

    def test_make_dataframe_keeps_exact_contract(self) -> None:
        """Le DataFrame respecte l'ordre des 18 colonnes."""
        record = {column: None for column in FINAL_COLUMNS}
        record.update(
            {
                "publication_id": "source_1",
                "image_size_bytes": 1,
                "published_at": "2026-09-02T12:00:00Z",
                "collected_at": "2026-09-02T12:01:00Z",
            }
        )
        dataframe = make_dataframe([record])
        self.assertEqual(tuple(dataframe.columns), FINAL_COLUMNS)

    def test_transform_batch_rejects_non_object_record(self) -> None:
        """Un élément JSON invalide est isolé sans interrompre le lot."""
        payload = {
            "source": "newsdata",
            "collected_at": "2026-09-02T12:00:00Z",
            "count": 1,
            "records": ["record invalide"],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            logger = logging.getLogger("test_transform_batch")
            logger.disabled = True
            candidates, rejected, audit = transform_batch(
                "newsdata",
                path,
                batch_index=0,
                metrics=Counter(),
                logger=logger,
            )

        self.assertEqual(candidates, [])
        self.assertEqual(rejected[0]["reason"], "record_brut_invalide")
        self.assertEqual(audit["validation_rejected_count"], 1)


class FakedditIncrementalTests(unittest.TestCase):
    """Vérifie le curseur utilisé entre deux extractions Fakeddit."""

    def test_cursor_defaults_then_persists_next_line(self) -> None:
        """Un nouveau pipeline commence après l'en-tête puis garde son avance."""
        with tempfile.TemporaryDirectory() as directory:
            cursor_path = Path(directory) / "cursor.json"
            self.assertEqual(read_fakeddit_cursor(cursor_path), FIRST_DATA_LINE)

            write_fakeddit_cursor(42, cursor_path)

            self.assertEqual(read_fakeddit_cursor(cursor_path), 42)

    def test_tsv_iterator_skips_lines_before_cursor(self) -> None:
        """Le second lot ne reparcourt pas les lignes du premier lot."""
        file = StringIO("id\ttitle\nfirst\tA\nsecond\tB\nthird\tC\n")

        rows = list(iter_tsv_rows(file, start_line=3))

        self.assertEqual([line for line, _ in rows], [3, 4])
        self.assertEqual([row["id"] for _, row in rows], ["second", "third"])


class AirflowConfigurationTests(unittest.TestCase):
    """Verrouille la cadence et les volumes attendus par le DAG local."""

    def test_defaults_balance_sources_and_schedule_every_fifteen_minutes(self) -> None:
        """La configuration générée garde des lots de 10 à 20 publications."""
        configuration = build_configuration("test-key")

        self.assertEqual(configuration["CHECKITAI_SCHEDULE"], "*/15 * * * *")
        self.assertEqual(configuration["CHECKITAI_NEWSDATA_LIMIT"], "10")
        self.assertEqual(configuration["CHECKITAI_POLITIFACT_LIMIT"], "20")
        self.assertEqual(configuration["CHECKITAI_FAKEDDIT_LIMIT"], "20")
        self.assertEqual(configuration["CHECKITAI_THECONVERSATION_LIMIT"], "10")


class LoadTests(unittest.TestCase):
    """Vérifie les protections et la requête du chargement."""

    def test_table_name_blocks_sql_injection(self) -> None:
        """Seuls les identifiants SQL simples sont acceptés."""
        with self.assertRaises(ValueError):
            validate_table_name("publication; DROP TABLE publication")

    def test_sql_contains_primary_key_constraints_and_upsert(self) -> None:
        """Le DDL et l'upsert portent les garanties essentielles."""
        ddl = create_table_sql("publication_multimodale")
        statement = upsert_sql("publication_multimodale")
        self.assertIn("publication_id TEXT PRIMARY KEY", ddl)
        self.assertIn("publication_source_image_unique", ddl)
        self.assertIn("ON CONFLICT (publication_id) DO UPDATE", statement)


class MonitoringTests(unittest.TestCase):
    """Vérifie les calculs KPI indépendamment de Streamlit."""

    def test_manifest_counts_distinguishes_duplicates_and_invalid_rows(self) -> None:
        """Les doublons ne sont pas additionnés aux rejets de validation."""
        counts = manifest_counts(
            {
                "schema_version": "1.3",
                "parameters": {"processing_mode": "incremental_exact_batches"},
                "inputs": [
                    {
                        "source": "newsdata",
                        "raw_count": 10,
                        "validated_count": 9,
                        "validation_rejected_count": 1,
                        "duplicate_discarded_count": 3,
                    },
                    {"source": "politifact"},
                    {"source": "fakeddit"},
                    {"source": "theconversation"},
                ]
            }
        )
        self.assertEqual(counts["raw_count"], 10)
        self.assertEqual(counts["validation_rejected_count"], 1)
        self.assertEqual(counts["duplicate_count"], 3)

    def test_manifest_counts_rejects_another_processing_mode(self) -> None:
        """Le dashboard refuse un manifeste produit par un autre workflow."""
        with self.assertRaisesRegex(ValueError, "pipeline incrémental courant"):
            manifest_counts({"parameters": {"processing_mode": "autre"}})

    def test_count_multimodal_requires_text_and_complete_image_proof(self) -> None:
        """Une URL d'image seule ne suffit pas à valider une publication."""
        valid_hash = "a" * 64
        dataframe = pd.DataFrame(
            [
                {
                    "title": "Titre",
                    "text": None,
                    "image_url": "https://example.org/image.jpg",
                    "image_path": "data/images/source/1.jpg",
                    "image_sha256": valid_hash,
                    "image_size_bytes": 42,
                },
                {
                    "title": "Incomplète",
                    "text": None,
                    "image_url": "https://example.org/image.jpg",
                    "image_path": "",
                    "image_sha256": valid_hash,
                    "image_size_bytes": 42,
                },
            ]
        )
        self.assertEqual(count_multimodal_publications(dataframe), 1)

    def test_parse_task_log_uses_last_valid_attempt_timestamps(self) -> None:
        """Un log JSONL réussi fournit un statut et une durée."""
        events = (
            {"timestamp": "2026-09-02T12:00:00Z", "event": "Starting attempt"},
            {
                "timestamp": "2026-09-02T12:00:02Z",
                "event": "Done. Returned value was: 10",
            },
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "attempt=1.log"
            path.write_text(
                "\n".join(json.dumps(event) for event in events),
                encoding="utf-8",
            )
            result = parse_task_log(path)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["duration_seconds"], 2.0)


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    unittest.main()
