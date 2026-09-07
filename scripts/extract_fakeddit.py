"""Extrait un échantillon texte-image depuis le TSV Fakeddit."""

import argparse
import csv
import json
import logging
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TextIO

import requests

from common import (
    DATA_DIR,
    PROJECT_ROOT,
    add_image_evidence,
    configure_logging,
    create_http_session,
    download_image,
    save_json_records,
    utc_now,
    write_json_atomic,
)

DEFAULT_TSV_PATH = (
    PROJECT_ROOT / "data" / "external" / "fakeddit" / "multimodal_train.tsv"
)
IMAGES_DIR = DATA_DIR / "images" / "fakeddit"
CURSOR_PATH = DATA_DIR / "state" / "fakeddit_cursor.json"
FIRST_DATA_LINE = 2
REQUEST_DELAY_SECONDS = 0.2


def read_fakeddit_cursor(cursor_path: Path = CURSOR_PATH) -> int:
    """Retourne la prochaine ligne TSV à lire, ou la première ligne de données."""
    if not cursor_path.exists():
        return FIRST_DATA_LINE

    try:
        payload = json.loads(cursor_path.read_text(encoding="utf-8"))
        next_line = int(payload["next_line"])
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Curseur Fakeddit invalide : {cursor_path}") from error

    if next_line < FIRST_DATA_LINE:
        raise RuntimeError(f"Curseur Fakeddit invalide : {next_line}")
    return next_line


def write_fakeddit_cursor(
    next_line: int,
    cursor_path: Path = CURSOR_PATH,
) -> None:
    """Enregistre atomiquement la prochaine ligne à lire après un lot réussi."""
    if next_line < FIRST_DATA_LINE:
        raise ValueError("next_line doit désigner une ligne de données du TSV.")
    write_json_atomic(
        {"next_line": next_line, "updated_at": utc_now()},
        cursor_path,
        sort_keys=True,
    )


def iter_tsv_rows(file: TextIO, start_line: int) -> Iterator[tuple[int, dict]]:
    """Parcourt le TSV à partir d'un numéro de ligne physique inclus."""
    reader = csv.DictReader(file, delimiter="\t")
    for line_number, row in enumerate(reader, start=FIRST_DATA_LINE):
        if line_number >= start_line:
            yield line_number, row


def extract_fakeddit(
    tsv_path: Path,
    limit: int,
    start_line: int,
    refresh_images: bool,
    logger: logging.Logger,
) -> tuple[list[dict], int]:
    """Extrait ``limit`` couples puis retourne la prochaine ligne à lire."""
    if limit < 1:
        raise ValueError("limit doit être positif.")
    if start_line < FIRST_DATA_LINE:
        raise ValueError(f"start_line doit être supérieur ou égal à {FIRST_DATA_LINE}.")
    if not tsv_path.is_file():
        raise FileNotFoundError(f"Fichier TSV absent : {tsv_path}")

    records: list[dict] = []
    failures = 0
    next_line = start_line
    logger.info("Lecture du TSV à partir de la ligne %s", start_line)

    with (
        create_http_session() as session,
        tsv_path.open(
            mode="r",
            encoding="utf-8",
            newline="",
        ) as file,
    ):
        for line_number, row in iter_tsv_rows(file, start_line):
            if len(records) >= limit:
                break
            next_line = line_number + 1

            image_id = (row.get("id") or "").strip()
            image_url = (row.get("image_url") or "").strip()
            clean_title = (row.get("clean_title") or "").strip()
            has_image = (row.get("hasImage") or "").strip().lower() == "true"

            if not image_id or not image_url or not clean_title or not has_image:
                continue

            try:
                image = download_image(
                    session,
                    image_url,
                    image_id,
                    IMAGES_DIR,
                    refresh_existing=refresh_images,
                )
            except (
                requests.RequestException,
                OSError,
                ValueError,
                RuntimeError,
            ) as error:
                failures += 1
                logger.warning(
                    "Ligne %s, image %s ignorée : %s", line_number, image_id, error
                )
                continue

            record = dict(row)
            add_image_evidence(record, image)
            records.append(record)
            logger.info(
                "[%s/%s] %s (%s, provenance=%s)",
                len(records),
                limit,
                image_id,
                "téléchargé" if image.downloaded else "déjà présent",
                image.provenance_status,
            )

            if image.downloaded:
                time.sleep(REQUEST_DELAY_SECONDS)

    logger.info("Bilan : %s couples prêts, %s échecs", len(records), failures)
    if not records:
        if next_line == start_line:
            raise RuntimeError(
                "Fin du TSV Fakeddit atteinte ; réinitialisez le curseur pour repartir."
            )
        raise RuntimeError("Aucun couple texte-image Fakeddit n'a été extrait.")
    if len(records) < limit:
        logger.warning("Fin du TSV atteinte avant la limite de %s couples.", limit)
    return records, next_line


def parse_args() -> argparse.Namespace:
    """Lit la limite et le chemin TSV depuis la ligne de commande."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--tsv", type=Path, default=DEFAULT_TSV_PATH)
    parser.add_argument("--cursor", type=Path, default=CURSOR_PATH)
    parser.add_argument(
        "--reset-cursor",
        action="store_true",
        help="Repart de la première ligne de données avant l'extraction.",
    )
    parser.add_argument(
        "--refresh-images",
        action="store_true",
        help="Retélécharge les images même si une copie locale existe.",
    )
    return parser.parse_args()


def main() -> None:
    """Point d'entrée autonome du script."""
    args = parse_args()
    logger = configure_logging("fakeddit")

    try:
        if args.reset_cursor:
            write_fakeddit_cursor(FIRST_DATA_LINE, args.cursor)
        start_line = read_fakeddit_cursor(args.cursor)
        records, next_line = extract_fakeddit(
            args.tsv,
            args.limit,
            start_line,
            args.refresh_images,
            logger,
        )
        output_path = save_json_records(records, "fakeddit")
        write_fakeddit_cursor(next_line, args.cursor)
        logger.info(
            "Extraction terminée : %s couples dans %s ; prochain départ ligne %s",
            len(records),
            output_path,
            next_line,
        )
    except Exception:
        logger.exception("Échec de l'extraction Fakeddit")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
