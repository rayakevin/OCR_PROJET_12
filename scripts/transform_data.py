"""Normalise, contrôle et déduplique les extractions multimodales brutes."""

import argparse
import hashlib
import html
import json
import logging
import re
import unicodedata
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
from bs4 import BeautifulSoup

from common import (
    DATA_DIR,
    PROJECT_ROOT,
    configure_logging,
    file_sha256,
    relative_path,
    utc_now,
    write_json_atomic,
)

SUPPORTED_SOURCES = ("newsdata", "politifact", "fakeddit", "theconversation")
SCHEMA_VERSION = "1.3"
TRUSTED_IMAGE_PROVENANCE = {"downloaded", "metadata_verified"}

FINAL_COLUMNS = (
    "publication_id",
    "source_name",
    "source_domain",
    "source_url",
    "title",
    "text",
    "image_url",
    "image_path",
    "image_size_bytes",
    "image_sha256",
    "image_provenance_status",
    "published_at",
    "language",
    "author",
    "source_label_raw",
    "source_label_scheme",
    "label_provenance",
    "collected_at",
)

STRING_COLUMNS = tuple(
    column
    for column in FINAL_COLUMNS
    if column not in {"image_size_bytes", "published_at", "collected_at"}
)

LANGUAGE_CODES = {
    "english": "en",
    "en": "en",
    "french": "fr",
    "fr": "fr",
}

PAID_CONTENT_MARKERS = {
    "ONLY AVAILABLE IN PAID PLANS",
    "ONLY AVAILABLE IN PROFESSIONAL AND CORPORATE PLANS",
}


class RecordValidationError(ValueError):
    """Signale une donnée source invalide sans arrêter tout le lot."""


@dataclass(frozen=True)
class ImageEvidence:
    """Champs bruts utilisés pour prouver l'origine d'une image locale."""

    expected_stem: str
    raw_size: object
    raw_sha256: object
    downloaded_from_url: object
    provenance_status: object


@dataclass(frozen=True)
class Candidate:
    """Publication validée en attente de la décision de dédoublonnage."""

    record: dict
    source: str
    input_path: Path
    raw_record_id: str
    batch_index: int


def clean_text(value: object) -> str | None:
    """Nettoie espaces et Unicode sans modifier le sens du texte."""
    if value is None:
        return None

    text = html.unescape(str(value))
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def clean_html(html_content: object) -> tuple[str | None, str | None]:
    """Extrait le texte et le verdict PolitiFact d'un fragment HTML."""
    if not html_content:
        return None, None

    soup = BeautifulSoup(str(html_content), "html.parser")
    for unwanted in soup(["script", "style"]):
        unwanted.decompose()

    verdict_image = soup.find(
        "img",
        src=lambda value: isinstance(value, str) and "flat-meter-" in value,
        alt=True,
    )
    verdict = clean_text(verdict_image.get("alt")) if verdict_image else None
    return clean_text(soup.get_text(" ", strip=True)), verdict


def normalize_author(value: object) -> str | None:
    """Convertit un auteur ou une liste d'auteurs en chaîne homogène."""
    if isinstance(value, list):
        authors = [clean_text(author) for author in value]
        return ", ".join(author for author in authors if author) or None
    return clean_text(value)


def normalize_language(value: object, default: str | None = None) -> str | None:
    """Convertit les langues connues vers un code ISO 639-1."""
    language = clean_text(value) or default
    if not language:
        return None
    return LANGUAGE_CODES.get(language.lower(), language.lower())


def normalize_datetime(value: object) -> str:
    """Convertit une date ISO, RSS ou Unix en date ISO 8601 UTC."""
    text = clean_text(value)
    if not text:
        raise RecordValidationError("date_absente")

    try:
        if re.fullmatch(r"\d{9,}(?:\.\d+)?", text):
            parsed = datetime.fromtimestamp(float(text), tz=UTC)
        else:
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                parsed = parsedate_to_datetime(text)
    except (OverflowError, TypeError, ValueError) as error:
        raise RecordValidationError(f"date_invalide:{text}") from error

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def normalize_domain(value: object) -> str | None:
    """Extrait un nom de domaine en minuscules et sans préfixe www."""
    text = clean_text(value)
    if not text:
        return None

    parsed = urlsplit(text if "://" in text else f"//{text}")
    hostname = parsed.hostname
    if not hostname:
        return None
    hostname = hostname.lower()
    return hostname.removeprefix("www.")


def is_http_url(value: object) -> bool:
    """Vérifie qu'une valeur est une URL HTTP(S) absolue."""
    text = clean_text(value)
    if not text:
        return False
    parsed = urlsplit(text)
    return parsed.scheme in {"http", "https"} and bool(parsed.hostname)


def detect_image_format(path: Path) -> str | None:
    """Détecte les formats d'image acceptés à partir de leur signature binaire."""
    with path.open("rb") as file:
        header = file.read(12)

    if header.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if header.startswith((b"GIF87a", b"GIF89a")):
        return "gif"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "webp"
    return None


def validate_image(
    image_path: object,
    image_url: object,
    evidence: ImageEvidence,
    metrics: Counter,
) -> tuple[str, int, str, str]:
    """Valide fichier, association, hash et URL de provenance d'une image."""
    path_text = clean_text(image_path)
    if not path_text:
        raise RecordValidationError("image_path_absent")

    path = Path(path_text)
    absolute_path = (
        path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()
    )
    images_root = (DATA_DIR / "images").resolve()

    try:
        absolute_path.relative_to(images_root)
    except ValueError as error:
        raise RecordValidationError("image_hors_du_dossier_autorise") from error

    if not absolute_path.is_file():
        raise RecordValidationError("image_locale_absente")
    if absolute_path.stem != evidence.expected_stem:
        raise RecordValidationError("association_texte_image_incoherente")

    image_size = absolute_path.stat().st_size
    if image_size == 0:
        raise RecordValidationError("image_vide")

    if evidence.raw_size is not None:
        try:
            expected_size = int(evidence.raw_size)
        except (TypeError, ValueError) as error:
            raise RecordValidationError("taille_image_source_invalide") from error
        if expected_size != image_size:
            raise RecordValidationError("taille_image_modifiee_depuis_extraction")

    if detect_image_format(absolute_path) is None:
        raise RecordValidationError("signature_image_invalide")

    actual_hash = file_sha256(absolute_path)
    downloaded_from_url = clean_text(evidence.downloaded_from_url)
    raw_hash = clean_text(evidence.raw_sha256)
    raw_status = clean_text(evidence.provenance_status)
    current_url = clean_text(image_url)

    if not downloaded_from_url or not raw_hash or not raw_status:
        raise RecordValidationError("image_provenance_absente")
    if raw_status not in TRUSTED_IMAGE_PROVENANCE:
        raise RecordValidationError("image_provenance_invalide")
    if downloaded_from_url != current_url:
        raise RecordValidationError("image_url_provenance_incoherente")
    if raw_hash != actual_hash:
        raise RecordValidationError("image_hash_modifie_depuis_extraction")

    provenance_status = raw_status
    metrics["image_provenances_verified"] += 1

    metrics["images_validated"] += 1
    metrics["image_hashes_generated"] += 1
    return (
        relative_path(absolute_path),
        image_size,
        actual_hash,
        provenance_status,
    )


def image_evidence(raw: dict, expected_stem: str) -> ImageEvidence:
    """Regroupe les champs de preuve ajoutés pendant Extract."""
    return ImageEvidence(
        expected_stem=expected_stem,
        raw_size=raw.get("_image_size"),
        raw_sha256=raw.get("_image_sha256"),
        downloaded_from_url=raw.get("_downloaded_from_url"),
        provenance_status=raw.get("_image_provenance_status"),
    )


def transform_newsdata(
    raw: dict,
    collected_at: str,
    metrics: Counter,
) -> tuple[dict, ImageEvidence]:
    """Mappe un article NewsData vers le schéma commun."""
    article_id = clean_text(raw.get("article_id"))
    publication_id = f"newsdata_{article_id}" if article_id else None

    content = clean_text(raw.get("content"))
    if content in PAID_CONTENT_MARKERS:
        content = None
        metrics["newsdata_paid_content_fallbacks"] += 1
    text = (
        content
        or clean_text(raw.get("description"))
        or clean_text(raw.get("ai_summary"))
    )
    source_url = clean_text(raw.get("link"))
    language = normalize_language(raw.get("language"), default="fr")
    if clean_text(raw.get("language")) != language:
        metrics["languages_normalized"] += 1

    record = {
        "publication_id": publication_id,
        "source_name": "NewsData.io",
        "source_domain": normalize_domain(source_url or raw.get("source_url")),
        "source_url": source_url,
        "title": clean_text(raw.get("title")),
        "text": text,
        "image_url": clean_text(raw.get("image_url")),
        "image_path": clean_text(raw.get("_image_path")),
        "image_size_bytes": None,
        "image_sha256": None,
        "image_provenance_status": None,
        "published_at": normalize_datetime(raw.get("pubDate")),
        "language": language,
        "author": normalize_author(raw.get("creator")),
        "source_label_raw": None,
        "source_label_scheme": None,
        "label_provenance": None,
        "collected_at": collected_at,
    }
    return record, image_evidence(raw, publication_id or "")


def transform_politifact(
    raw: dict,
    collected_at: str,
    metrics: Counter,
) -> tuple[dict, ImageEvidence]:
    """Mappe un fact-check PolitiFact vers le schéma commun."""
    entry_id = clean_text(raw.get("id") or raw.get("link"))
    digest = hashlib.sha256((entry_id or "").encode("utf-8")).hexdigest()[:16]
    publication_id = f"politifact_{digest}" if entry_id else None
    content_text, verdict = clean_html(raw.get("content_html"))
    if raw.get("content_html"):
        metrics["html_documents_cleaned"] += 1
    source_url = clean_text(raw.get("link"))

    record = {
        "publication_id": publication_id,
        "source_name": "PolitiFact",
        "source_domain": normalize_domain(source_url),
        "source_url": source_url,
        "title": clean_text(raw.get("title")),
        "text": content_text or clean_text(raw.get("summary")),
        "image_url": clean_text(raw.get("image_url")),
        "image_path": clean_text(raw.get("_image_path")),
        "image_size_bytes": None,
        "image_sha256": None,
        "image_provenance_status": None,
        "published_at": normalize_datetime(raw.get("published")),
        "language": "en",
        "author": normalize_author(raw.get("author")),
        "source_label_raw": verdict,
        "source_label_scheme": "PolitiFact Truth-O-Meter" if verdict else None,
        "label_provenance": "PolitiFact editorial fact-check" if verdict else None,
        "collected_at": collected_at,
    }
    if verdict:
        metrics["label_sets_preserved"] += 1
    return record, image_evidence(raw, publication_id or "")


def transform_fakeddit(
    raw: dict,
    collected_at: str,
    metrics: Counter,
) -> tuple[dict, ImageEvidence]:
    """Mappe une ligne Fakeddit sans interpréter ses labels."""
    image_id = clean_text(raw.get("id"))
    publication_id = f"fakeddit_{image_id}" if image_id else None
    labels: dict[str, str] = {}
    for field in ("2_way_label", "3_way_label", "6_way_label"):
        value = clean_text(raw.get(field))
        if value is not None:
            labels[field] = value
    raw_labels = (
        json.dumps(labels, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if labels
        else None
    )

    record = {
        "publication_id": publication_id,
        "source_name": "Fakeddit",
        "source_domain": normalize_domain(raw.get("domain")) or "reddit.com",
        "source_url": f"https://www.reddit.com/comments/{image_id}"
        if image_id
        else None,
        "title": clean_text(raw.get("title") or raw.get("clean_title")),
        "text": clean_text(raw.get("clean_title") or raw.get("title")),
        "image_url": clean_text(raw.get("image_url")),
        "image_path": clean_text(raw.get("_image_path")),
        "image_size_bytes": None,
        "image_sha256": None,
        "image_provenance_status": None,
        "published_at": normalize_datetime(raw.get("created_utc")),
        "language": "en",
        "author": normalize_author(raw.get("author")),
        "source_label_raw": raw_labels,
        "source_label_scheme": "Fakeddit 2-way, 3-way and 6-way labels"
        if raw_labels
        else None,
        "label_provenance": "Fakeddit distant supervision" if raw_labels else None,
        "collected_at": collected_at,
    }
    if raw_labels:
        metrics["label_sets_preserved"] += 1
    return record, image_evidence(raw, image_id or "")


def transform_theconversation(
    raw: dict,
    collected_at: str,
    metrics: Counter,
) -> tuple[dict, ImageEvidence]:
    """Mappe un article HTML de The Conversation vers le schéma commun."""
    article_id = clean_text(raw.get("id"))
    publication_id = f"theconversation_{article_id}" if article_id else None
    source_url = clean_text(raw.get("link"))

    record = {
        "publication_id": publication_id,
        "source_name": "The Conversation France",
        "source_domain": normalize_domain(source_url),
        "source_url": source_url,
        "title": clean_text(raw.get("title")),
        "text": clean_text(raw.get("text") or raw.get("summary")),
        "image_url": clean_text(raw.get("image_url")),
        "image_path": clean_text(raw.get("_image_path")),
        "image_size_bytes": None,
        "image_sha256": None,
        "image_provenance_status": None,
        "published_at": normalize_datetime(raw.get("published")),
        "language": "fr",
        "author": normalize_author(raw.get("author")),
        "source_label_raw": None,
        "source_label_scheme": None,
        "label_provenance": None,
        "collected_at": collected_at,
    }
    metrics["theconversation_html_records_normalized"] += 1
    return record, image_evidence(raw, publication_id or "")


TRANSFORMERS: dict[
    str,
    Callable[[dict, str, Counter], tuple[dict, ImageEvidence]],
] = {
    "newsdata": transform_newsdata,
    "politifact": transform_politifact,
    "fakeddit": transform_fakeddit,
    "theconversation": transform_theconversation,
}


def validate_configuration(
    duplicate_policy: str,
    output_format: str,
) -> None:
    """Valide les options utilisées par la CLI comme par Airflow."""
    if duplicate_policy not in {"keep-first", "keep-latest"}:
        raise ValueError("duplicate_policy doit valoir 'keep-first' ou 'keep-latest'.")
    if output_format not in {"parquet", "jsonl", "both"}:
        raise ValueError("output_format doit valoir 'parquet', 'jsonl' ou 'both'.")


def validate_record(record: dict) -> None:
    """Vérifie les contraintes métier minimales du schéma commun."""
    if not record.get("publication_id"):
        raise RecordValidationError("publication_id_absent")
    if not record.get("source_name") or not record.get("source_domain"):
        raise RecordValidationError("source_absente")
    if not record.get("title") and not record.get("text"):
        raise RecordValidationError("texte_et_titre_absents")
    if not is_http_url(record.get("source_url")):
        raise RecordValidationError("source_url_invalide")
    if not is_http_url(record.get("image_url")):
        raise RecordValidationError("image_url_invalide")
    if not record.get("published_at") or not record.get("collected_at"):
        raise RecordValidationError("date_absente")
    if not re.fullmatch(r"[a-z]{2}", record.get("language") or ""):
        raise RecordValidationError("code_langue_invalide")

    label_values = (
        record.get("source_label_raw"),
        record.get("source_label_scheme"),
        record.get("label_provenance"),
    )
    if any(label_values) and not all(label_values):
        raise RecordValidationError("provenance_label_incomplete")


def load_raw_batch(path: Path, expected_source: str, logger: logging.Logger) -> dict:
    """Charge et contrôle l'enveloppe d'un lot JSON brut."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Lot JSON illisible : {path}") from error

    if not isinstance(payload, dict) or not isinstance(payload.get("records"), list):
        raise TypeError(f"Structure de lot invalide : {path}")
    if payload.get("source") != expected_source:
        raise RuntimeError(f"Source inattendue dans {path} : {payload.get('source')!r}")
    if payload.get("count") != len(payload["records"]):
        logger.warning(
            "Le count annoncé dans %s diffère du nombre réel de records.",
            path,
        )
    return payload


def raw_record_id(source: str, raw: dict, index: int) -> str:
    """Retourne un identifiant lisible pour le journal des rejets."""
    value = raw.get("article_id") if source == "newsdata" else raw.get("id")
    return clean_text(value) or f"index_{index}"


def make_rejection(
    candidate: Candidate | None,
    source: str,
    input_path: Path,
    raw_id: str,
    reason: str,
    **details: object,
) -> dict:
    """Construit une ligne stable du journal des rejets."""
    rejection = {
        "source": candidate.source if candidate else source,
        "input_file": relative_path(candidate.input_path if candidate else input_path),
        "raw_record_id": candidate.raw_record_id if candidate else raw_id,
        "reason": reason,
    }
    rejection.update(details)
    return rejection


def map_and_validate_record(
    raw: dict,
    source: str,
    collected_at: str,
    input_path: Path,
    record_index: int,
    batch_index: int,
    metrics: Counter,
) -> Candidate:
    """Mappe un record brut et valide son contrat ainsi que son image."""
    identifier = raw_record_id(source, raw, record_index)
    record, evidence = TRANSFORMERS[source](raw, collected_at, metrics)
    metrics["records_mapped"] += 1
    metrics["published_dates_normalized"] += 1
    metrics["text_records_normalized"] += 1

    validate_record(record)
    metrics["urls_validated"] += 2
    image_path, image_size, image_hash, provenance_status = validate_image(
        record["image_path"],
        record["image_url"],
        evidence,
        metrics,
    )
    record.update(
        {
            "image_path": image_path,
            "image_size_bytes": image_size,
            "image_sha256": image_hash,
            "image_provenance_status": provenance_status,
        }
    )
    return Candidate(
        record=record,
        source=source,
        input_path=input_path,
        raw_record_id=identifier,
        batch_index=batch_index,
    )


def transform_batch(
    source: str,
    input_path: Path,
    batch_index: int,
    metrics: Counter,
    logger: logging.Logger,
) -> tuple[list[Candidate], list[dict], dict]:
    """Transforme un lot brut et retourne candidats, rejets et audit du lot."""
    payload = load_raw_batch(input_path, source, logger)
    try:
        collected_at = normalize_datetime(payload.get("collected_at"))
    except RecordValidationError as error:
        raise RuntimeError(f"Date de collecte invalide dans {input_path}") from error

    audit = {
        "source": source,
        "path": relative_path(input_path),
        "sha256": file_sha256(input_path),
        "collected_at": collected_at,
        "raw_count": len(payload["records"]),
        "validated_count": 0,
        "validation_rejected_count": 0,
        "duplicate_discarded_count": 0,
        "accepted_count": 0,
    }
    metrics["input_batches_read"] += 1
    metrics["records_read"] += len(payload["records"])
    metrics["collection_dates_normalized"] += 1
    logger.info("Lot lu : %s (%s records)", input_path, len(payload["records"]))

    candidates: list[Candidate] = []
    rejected: list[dict] = []
    for record_index, raw in enumerate(payload["records"], start=1):
        if not isinstance(raw, dict):
            identifier = f"index_{record_index}"
            reason = "record_brut_invalide"
        else:
            identifier = raw_record_id(source, raw, record_index)
            try:
                candidate = map_and_validate_record(
                    raw,
                    source,
                    collected_at,
                    input_path,
                    record_index,
                    batch_index,
                    metrics,
                )
            except RecordValidationError as error:
                reason = str(error)
            else:
                candidates.append(candidate)
                audit["validated_count"] += 1
                continue

        audit["validation_rejected_count"] += 1
        metrics["validation_rejections"] += 1
        rejected.append(make_rejection(None, source, input_path, identifier, reason))
        logger.warning("%s/%s rejeté : %s", source, identifier, reason)

    return candidates, rejected, audit


def deduplicate_candidates(
    candidates: list[Candidate],
    duplicate_policy: str,
    rejected: list[dict],
    batch_audits: list[dict],
    metrics: Counter,
    logger: logging.Logger,
) -> list[dict]:
    """Garde la première ou la dernière occurrence selon une règle explicite."""
    reverse = duplicate_policy == "keep-latest"
    ordered = sorted(
        candidates,
        key=lambda candidate: (
            candidate.record["collected_at"],
            relative_path(candidate.input_path),
            candidate.raw_record_id,
        ),
        reverse=reverse,
    )
    seen_ids: dict[str, Candidate] = {}
    seen_pairs: dict[tuple[str, str], Candidate] = {}
    kept: list[Candidate] = []

    for candidate in ordered:
        publication_id = candidate.record["publication_id"]
        pair = (
            candidate.record["source_url"],
            candidate.record["image_sha256"],
        )
        duplicate_types: list[str] = []
        kept_candidate = seen_ids.get(publication_id)
        if kept_candidate:
            duplicate_types.append("publication_id")
        pair_candidate = seen_pairs.get(pair)
        if pair_candidate:
            duplicate_types.append("source_url+image_sha256")
            kept_candidate = kept_candidate or pair_candidate

        if duplicate_types:
            batch_audits[candidate.batch_index]["duplicate_discarded_count"] += 1
            metrics["duplicates_discarded"] += 1
            rejection = make_rejection(
                candidate,
                "",
                Path(),
                "",
                "duplicate_discarded",
                duplicate_policy=duplicate_policy,
                duplicate_keys=duplicate_types,
                kept_publication_id=kept_candidate.record["publication_id"],
                kept_input_file=relative_path(kept_candidate.input_path),
            )
            rejected.append(rejection)
            logger.info(
                "Doublon écarté : %s depuis %s (%s)",
                publication_id,
                candidate.input_path,
                ", ".join(duplicate_types),
            )
            continue

        seen_ids[publication_id] = candidate
        seen_pairs[pair] = candidate
        kept.append(candidate)

    for candidate in kept:
        batch_audits[candidate.batch_index]["accepted_count"] += 1

    return sorted(
        (candidate.record for candidate in kept),
        key=lambda record: record["publication_id"],
    )


def transform_inputs(
    input_files: list[tuple[str, Path]],
    duplicate_policy: str,
    logger: logging.Logger,
) -> tuple[list[dict], list[dict], list[dict], Counter]:
    """Lit, mappe, valide puis déduplique les lots explicitement fournis."""
    candidates: list[Candidate] = []
    rejected: list[dict] = []
    batch_audits: list[dict] = []
    metrics: Counter = Counter()

    logger.info("Étape 1/4 - Lecture et contrôle des lots bruts")
    for batch_index, (source, input_path) in enumerate(input_files):
        batch_candidates, batch_rejected, audit = transform_batch(
            source,
            input_path,
            batch_index,
            metrics,
            logger,
        )
        candidates.extend(batch_candidates)
        rejected.extend(batch_rejected)
        batch_audits.append(audit)

    logger.info(
        "Étape 2/4 - Nettoyage, normalisation et validation : %s candidats",
        len(candidates),
    )
    logger.info(
        "Étape 3/4 - Dédoublonnage avec la politique %s",
        duplicate_policy,
    )
    accepted = deduplicate_candidates(
        candidates,
        duplicate_policy,
        rejected,
        batch_audits,
        metrics,
        logger,
    )
    metrics["records_exported"] = len(accepted)

    for audit in batch_audits:
        logger.info(
            "Bilan lot %s : %s validés, %s doublons écartés, %s retenus, %s invalides",
            audit["path"],
            audit["validated_count"],
            audit["duplicate_discarded_count"],
            audit["accepted_count"],
            audit["validation_rejected_count"],
        )

    return accepted, rejected, batch_audits, metrics


def make_dataframe(records: list[dict]) -> pd.DataFrame:
    """Construit un DataFrame avec des types explicites et un ordre stable."""
    dataframe = pd.DataFrame(records, columns=FINAL_COLUMNS)
    for column in STRING_COLUMNS:
        dataframe[column] = dataframe[column].astype("string")
    dataframe["image_size_bytes"] = dataframe["image_size_bytes"].astype("Int64")
    for column in ("published_at", "collected_at"):
        dataframe[column] = pd.to_datetime(dataframe[column], utc=True)
    return dataframe.sort_values("publication_id").reset_index(drop=True)


def write_parquet(dataframe: pd.DataFrame, path: Path) -> None:
    """Écrit atomiquement le dataset Parquet typé."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.part")
    try:
        dataframe.to_parquet(temporary_path, engine="pyarrow", index=False)
        temporary_path.replace(path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def write_jsonl(records: list[dict], path: Path) -> None:
    """Écrit atomiquement une liste de dictionnaires au format JSONL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.part")
    try:
        with temporary_path.open("w", encoding="utf-8") as file:
            for record in records:
                file.write(
                    json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                )
        temporary_path.replace(path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def project_path(path: Path) -> Path:
    """Interprète les chemins relatifs depuis la racine du projet."""
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def resolve_input_files(
    input_paths: list[Path] | tuple[Path, ...],
) -> list[tuple[str, Path]]:
    """Exige le lot courant de chacune des quatre sources du pipeline."""
    if len(input_paths) != len(SUPPORTED_SOURCES):
        raise ValueError(
            "La transformation attend exactement un lot courant par source : "
            + ", ".join(SUPPORTED_SOURCES)
            + "."
        )

    selected: list[tuple[str, Path]] = []
    selected_sources: set[str] = set()
    selected_paths: set[Path] = set()

    for input_path in input_paths:
        resolved_path = project_path(input_path)
        if not resolved_path.is_file():
            raise FileNotFoundError(f"Lot brut absent : {resolved_path}")
        if resolved_path in selected_paths:
            raise ValueError(f"Lot fourni plusieurs fois : {resolved_path}")

        try:
            payload = json.loads(resolved_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(f"Lot JSON illisible : {resolved_path}") from error

        source = payload.get("source") if isinstance(payload, dict) else None
        if source not in SUPPORTED_SOURCES:
            raise ValueError(
                f"Source non prise en charge dans {resolved_path} : {source!r}"
            )
        if source in selected_sources:
            raise ValueError(f"Un seul lot incrémental est autorisé pour {source}.")

        selected.append((source, resolved_path))
        selected_sources.add(source)
        selected_paths.add(resolved_path)

    missing_sources = set(SUPPORTED_SOURCES) - selected_sources
    if missing_sources:
        raise ValueError(
            "Lots manquants pour : " + ", ".join(sorted(missing_sources)) + "."
        )

    order = {source: index for index, source in enumerate(SUPPORTED_SOURCES)}
    return sorted(selected, key=lambda item: order[item[0]])


def run_transformation(
    input_paths: list[Path] | tuple[Path, ...],
    duplicate_policy: str,
    processed_dir: Path,
    rejected_dir: Path,
    output_format: str,
    logger: logging.Logger,
) -> dict:
    """Transforme uniquement les lots explicitement produits par le run courant."""
    validate_configuration(duplicate_policy, output_format)
    input_files = resolve_input_files(input_paths)
    logger.info(
        "Configuration : mode=incrémental, duplicate_policy=%s",
        duplicate_policy,
    )
    for source, path in input_files:
        logger.info("Lot sélectionné pour %s : %s", source, path)

    accepted, rejected, inputs_manifest, metrics = transform_inputs(
        input_files,
        duplicate_policy,
        logger,
    )
    processed_dir = project_path(processed_dir)
    rejected_dir = project_path(rejected_dir)
    rejected_path = rejected_dir / "invalid_records.jsonl"
    write_jsonl(rejected, rejected_path)

    if not accepted:
        raise RuntimeError("Aucune publication valide après transformation.")

    logger.info("Étape 4/4 - Typage et export des données")
    dataframe = make_dataframe(accepted)
    output_paths: list[Path] = []
    if output_format in {"parquet", "both"}:
        parquet_path = processed_dir / "publications.parquet"
        write_parquet(dataframe, parquet_path)
        output_paths.append(parquet_path)
    if output_format in {"jsonl", "both"}:
        jsonl_path = processed_dir / "publications.jsonl"
        write_jsonl(accepted, jsonl_path)
        output_paths.append(jsonl_path)

    rejection_counts = Counter(rejection["reason"] for rejection in rejected)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_sha256": file_sha256(Path(__file__)),
        "transformed_at": utc_now(),
        "parameters": {
            "processing_mode": "incremental_exact_batches",
            "duplicate_policy": duplicate_policy,
            "output_format": output_format,
            "processed_dir": relative_path(processed_dir),
            "rejected_dir": relative_path(rejected_dir),
        },
        "inputs": inputs_manifest,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "rejection_counts": dict(sorted(rejection_counts.items())),
        "transformation_metrics": dict(sorted(metrics.items())),
        "columns": list(FINAL_COLUMNS),
        "outputs": [
            {
                "path": relative_path(path),
                "sha256": file_sha256(path),
            }
            for path in output_paths
        ],
        "rejected_path": relative_path(rejected_path),
    }
    manifest_path = processed_dir / "transformation_manifest.json"
    write_json_atomic(manifest, manifest_path, sort_keys=True)

    for metric, value in sorted(metrics.items()):
        logger.info("Métrique transformation | %s=%s", metric, value)
    logger.info(
        "Transformation terminée : %s publications, %s rejets.",
        len(accepted),
        len(rejected),
    )
    for path in output_paths:
        logger.info("Sortie produite : %s", path)
    logger.info("Manifeste : %s", manifest_path)
    return manifest


def parse_args() -> argparse.Namespace:
    """Lit les paramètres configurables du pipeline."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-files",
        nargs="+",
        type=Path,
        required=True,
        help="Quatre lots JSON exacts du run, un pour chacune des sources.",
    )
    parser.add_argument(
        "--duplicate-policy",
        choices=("keep-first", "keep-latest"),
        default="keep-latest",
        help="Choisit l'occurrence conservée lorsqu'un doublon est détecté.",
    )
    parser.add_argument("--processed-dir", type=Path, default=DATA_DIR / "processed")
    parser.add_argument("--rejected-dir", type=Path, default=DATA_DIR / "rejected")
    parser.add_argument(
        "--output-format",
        choices=("parquet", "jsonl", "both"),
        default="both",
    )
    return parser.parse_args()


def main() -> None:
    """Exécute lecture, transformation, dédoublonnage et export."""
    args = parse_args()
    logger = configure_logging("transform")

    try:
        run_transformation(
            input_paths=args.input_files,
            duplicate_policy=args.duplicate_policy,
            processed_dir=args.processed_dir,
            rejected_dir=args.rejected_dir,
            output_format=args.output_format,
            logger=logger,
        )
    except Exception:
        logger.exception("Échec du pipeline de transformation")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
