"""Calcule les KPI ETL à partir du manifeste, du Parquet et des logs Airflow."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from common import DATA_DIR, PROJECT_ROOT, write_json_atomic
from transform_data import SCHEMA_VERSION, SUPPORTED_SOURCES

DAG_ID = "checkitai_multimodal_etl"
EXPECTED_TASKS = {
    "extract_newsdata",
    "extract_politifact",
    "extract_fakeddit",
    "extract_theconversation",
    "transform",
    "load_postgres",
    "validate_postgres",
}
DEFAULT_MANIFEST = DATA_DIR / "processed" / "transformation_manifest.json"
DEFAULT_PARQUET = DATA_DIR / "processed" / "publications.parquet"
DEFAULT_AIRFLOW_LOGS = PROJECT_ROOT / "airflow" / "logs"
REQUIRED_KPI_COLUMNS = {
    "source_name",
    "title",
    "text",
    "image_url",
    "image_path",
    "image_size_bytes",
    "image_sha256",
    "source_label_raw",
}


def read_json(path: Path) -> dict[str, Any]:
    """Lit un objet JSON et signale clairement un fichier absent ou invalide."""
    if not path.is_file():
        raise FileNotFoundError(f"Fichier absent : {path}")
    with path.open(encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise TypeError(f"Objet JSON attendu dans {path}")
    return value


def parse_timestamp(value: str) -> datetime:
    """Convertit un timestamp ISO Airflow en date UTC."""
    return datetime.fromisoformat(value).astimezone(UTC)


def parse_task_log(path: Path) -> dict[str, Any]:
    """Extrait l'état et la durée d'une tentative depuis son log JSONL."""
    timestamps: list[datetime] = []
    completed = False
    failed = False

    with path.open(encoding="utf-8", errors="replace") as file:
        for line in file:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("timestamp"):
                try:
                    timestamps.append(parse_timestamp(str(event["timestamp"])))
                except (TypeError, ValueError):
                    continue
            message = str(event.get("event", ""))
            completed = completed or "Done. Returned value was:" in message
            failed = failed or "Task failed with exception" in message

    if not timestamps:
        return {"status": "unknown", "duration_seconds": 0.0}
    status = "failed" if failed else "success" if completed else "running"
    return {
        "status": status,
        "started_at": min(timestamps).isoformat(),
        "ended_at": max(timestamps).isoformat(),
        "duration_seconds": round(
            (max(timestamps) - min(timestamps)).total_seconds(), 3
        ),
    }


def latest_attempt(task_directory: Path) -> Path | None:
    """Retourne le dernier fichier attempt=N.log d'une tâche."""
    attempts = sorted(
        task_directory.glob("attempt=*.log"),
        key=lambda path: int(path.stem.split("=")[-1]),
    )
    return attempts[-1] if attempts else None


def collect_airflow_runs(logs_directory: Path) -> list[dict[str, Any]]:
    """Construit l'historique des runs à partir des journaux locaux Airflow."""
    dag_directory = logs_directory / f"dag_id={DAG_ID}"
    runs: list[dict[str, Any]] = []
    for run_directory in sorted(dag_directory.glob("run_id=*")):
        tasks: dict[str, dict[str, Any]] = {}
        for task_directory in run_directory.glob("task_id=*"):
            attempt = latest_attempt(task_directory)
            if attempt:
                tasks[task_directory.name.removeprefix("task_id=")] = parse_task_log(
                    attempt
                )

        task_statuses = {task["status"] for task in tasks.values()}
        has_all_tasks = EXPECTED_TASKS.issubset(tasks)
        if has_all_tasks and task_statuses == {"success"}:
            status = "success"
        elif "failed" in task_statuses:
            status = "failed"
        else:
            status = "running"

        starts = [
            parse_timestamp(task["started_at"])
            for task in tasks.values()
            if task.get("started_at")
        ]
        ends = [
            parse_timestamp(task["ended_at"])
            for task in tasks.values()
            if task.get("ended_at")
        ]
        runs.append(
            {
                "run_id": run_directory.name.removeprefix("run_id="),
                "status": status,
                "started_at": min(starts).isoformat() if starts else None,
                "ended_at": max(ends).isoformat() if ends else None,
                "duration_seconds": round((max(ends) - min(starts)).total_seconds(), 3)
                if starts and ends
                else 0.0,
                "tasks": tasks,
            }
        )
    return sorted(runs, key=lambda run: run["started_at"] or "")


def directory_size(path: Path) -> int:
    """Additionne la taille des fichiers d'un répertoire."""
    if not path.exists():
        return 0
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def percentage(numerator: int, denominator: int) -> float:
    """Calcule un pourcentage protégé contre une division par zéro."""
    return round(100 * numerator / denominator, 2) if denominator else 0.0


def read_publications(parquet_path: Path) -> pd.DataFrame:
    """Lit le Parquet et contrôle les colonnes nécessaires aux KPI."""
    if not parquet_path.is_file():
        raise FileNotFoundError(f"Fichier absent : {parquet_path}")
    dataframe = pd.read_parquet(parquet_path)
    missing_columns = sorted(REQUIRED_KPI_COLUMNS - set(dataframe.columns))
    if missing_columns:
        raise ValueError(f"Colonnes absentes du Parquet : {', '.join(missing_columns)}")
    return dataframe


def manifest_counts(manifest: dict[str, Any]) -> dict[str, int]:
    """Valide le manifeste incrémental puis additionne ses compteurs."""
    parameters = manifest.get("parameters")
    if not isinstance(parameters, dict) or parameters.get("processing_mode") != (
        "incremental_exact_batches"
    ):
        raise ValueError("Le manifeste ne provient pas du pipeline incrémental courant.")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"Version de schéma attendue : {SCHEMA_VERSION}.")

    inputs = manifest.get("inputs")
    if not isinstance(inputs, list) or not all(
        isinstance(item, dict) for item in inputs
    ):
        raise ValueError("Le manifeste ne contient pas une liste 'inputs' valide.")
    input_sources = [item.get("source") for item in inputs]
    if len(input_sources) != len(SUPPORTED_SOURCES) or set(input_sources) != set(
        SUPPORTED_SOURCES
    ):
        raise ValueError("Le manifeste doit contenir un lot pour chaque source.")

    return {
        "raw_count": sum(int(item.get("raw_count", 0)) for item in inputs),
        "validated_count": sum(int(item.get("validated_count", 0)) for item in inputs),
        "validation_rejected_count": sum(
            int(item.get("validation_rejected_count", 0)) for item in inputs
        ),
        "duplicate_count": sum(
            int(item.get("duplicate_discarded_count", 0)) for item in inputs
        ),
    }


def count_multimodal_publications(dataframe: pd.DataFrame) -> int:
    """Compte les lignes finales possédant texte et preuve d'image complète."""
    has_text = dataframe["title"].fillna("").str.strip().ne("") | dataframe[
        "text"
    ].fillna("").str.strip().ne("")
    has_image = (
        dataframe["image_url"].fillna("").str.startswith(("http://", "https://"))
        & dataframe["image_path"].fillna("").str.strip().ne("")
        & dataframe["image_sha256"].fillna("").str.fullmatch(r"[0-9a-f]{64}")
        & dataframe["image_size_bytes"].fillna(0).gt(0)
    )
    return int((has_text & has_image).sum())


def collect_kpis(
    manifest_path: Path = DEFAULT_MANIFEST,
    parquet_path: Path = DEFAULT_PARQUET,
    airflow_logs: Path = DEFAULT_AIRFLOW_LOGS,
    data_directory: Path = DATA_DIR,
) -> dict[str, Any]:
    """Retourne qualité du lot courant, performance et stockage cumulé."""
    manifest = read_json(manifest_path)
    dataframe = read_publications(parquet_path)
    counts = manifest_counts(manifest)
    if int(manifest.get("accepted_count", -1)) != len(dataframe):
        raise ValueError("Le Parquet ne correspond pas au manifeste du run courant.")
    multimodal_count = count_multimodal_publications(dataframe)
    runs = collect_airflow_runs(airflow_logs)
    successful_runs = sum(run["status"] == "success" for run in runs)

    storage = {
        "Données brutes": directory_size(data_directory / "raw"),
        "Données transformées": directory_size(data_directory / "processed"),
        "Images du pipeline": directory_size(data_directory / "images"),
        "Logs Airflow": directory_size(airflow_logs),
    }
    source_counts = (
        dataframe.groupby("source_name", dropna=False)
        .size()
        .rename("publications")
        .reset_index()
        .to_dict(orient="records")
    )
    latest_run = runs[-1] if runs else None

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "quality": {
            **counts,
            "accepted_count": len(dataframe),
            "validity_percent": percentage(
                counts["validated_count"],
                counts["raw_count"],
            ),
            "multimodal_count": multimodal_count,
            "multimodal_percent": percentage(multimodal_count, len(dataframe)),
            "label_coverage_percent": percentage(
                int(dataframe["source_label_raw"].notna().sum()),
                len(dataframe),
            ),
        },
        "performance": {
            "run_count": len(runs),
            "successful_run_count": successful_runs,
            "success_percent": percentage(successful_runs, len(runs)),
            "latest_run": latest_run,
            "history": runs,
        },
        "storage": {
            "breakdown_bytes": storage,
            "total_bytes": sum(storage.values()),
        },
        "sources": source_counts,
    }


def parse_args() -> argparse.Namespace:
    """Lit les chemins configurables depuis la ligne de commande."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--parquet", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--airflow-logs", type=Path, default=DEFAULT_AIRFLOW_LOGS)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    """Affiche ou sauvegarde un instantané KPI reproductible."""
    args = parse_args()
    result = collect_kpis(
        args.manifest,
        args.parquet,
        args.airflow_logs,
        args.data_dir,
    )
    if args.output:
        write_json_atomic(result, args.output)
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
