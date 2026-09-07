"""Fonctions partagées par les scripts du pipeline."""

import hashlib
import json
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

MAX_IMAGE_BYTES = 10 * 1024 * 1024
CONTENT_TYPE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


@dataclass(frozen=True)
class ImageDownloadResult:
    """Décrit une image locale et la preuve de son URL de téléchargement."""

    path: Path
    size_bytes: int
    downloaded: bool
    sha256: str
    source_url: str
    provenance_status: str


def configure_logging(source: str) -> logging.Logger:
    """Crée un logger écrit dans le terminal et dans ``logs/<source>.log``."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(source)
    logger.setLevel(logging.INFO)
    for handler in logger.handlers:
        handler.close()
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    # stdout évite qu'Airflow classe les messages INFO comme des erreurs stderr.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(
        LOGS_DIR / f"{source}.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


def create_http_session() -> requests.Session:
    """Retourne une session HTTP avec retries sur les erreurs temporaires."""
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET"},
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.headers.update({"User-Agent": "CheckItAI-P12/1.0"})
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def file_sha256(path: Path) -> str:
    """Calcule le SHA-256 d'un fichier sans le charger entièrement en mémoire."""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_atomic(
    payload: object,
    path: Path,
    *,
    sort_keys: bool = False,
) -> None:
    """Écrit un JSON complet sans exposer de fichier final partiel."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.part")
    try:
        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(
                payload,
                file,
                ensure_ascii=False,
                indent=2,
                sort_keys=sort_keys,
            )
        temporary_path.replace(path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def write_image_metadata(
    metadata_path: Path,
    image_id: str,
    image_url: str,
    image_path: Path,
    size_bytes: int,
    sha256: str,
    provenance_status: str,
) -> None:
    """Écrit atomiquement la preuve liant une URL au fichier image local."""
    payload = {
        "image_id": image_id,
        "source_url": image_url,
        "file_name": image_path.name,
        "size_bytes": size_bytes,
        "sha256": sha256,
        "provenance_status": provenance_status,
        "recorded_at": utc_now(),
    }
    write_json_atomic(payload, metadata_path, sort_keys=True)


def reusable_image(
    output_dir: Path,
    image_id: str,
    image_url: str,
) -> ImageDownloadResult | None:
    """Réutilise une image seulement si sa preuve locale reste cohérente."""
    metadata_path = output_dir / f"{image_id}.metadata.json"

    if not metadata_path.is_file():
        return None

    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        image_path = output_dir / metadata["file_name"]
        size_bytes = image_path.stat().st_size
        sha256 = file_sha256(image_path)
        metadata_is_valid = (
            image_path.is_file()
            and size_bytes > 0
            and metadata.get("source_url") == image_url
            and metadata.get("image_id") == image_id
            and metadata.get("size_bytes") == size_bytes
            and metadata.get("sha256") == sha256
            and metadata.get("provenance_status") in {"downloaded", "metadata_verified"}
        )
        if metadata_is_valid:
            return ImageDownloadResult(
                path=image_path,
                size_bytes=size_bytes,
                downloaded=False,
                sha256=sha256,
                source_url=image_url,
                provenance_status="metadata_verified",
            )
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        pass

    return None


def download_image(
    session: requests.Session,
    image_url: str,
    image_id: str,
    output_dir: Path,
    refresh_existing: bool = False,
) -> ImageDownloadResult:
    """Télécharge une image ou réutilise une copie dont la provenance est vérifiée."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if not refresh_existing:
        existing_result = reusable_image(output_dir, image_id, image_url)
        if existing_result:
            return existing_result

    with session.get(
        image_url,
        timeout=(5, 30),
        stream=True,
    ) as response:
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").split(";")[0].lower()
        extension = CONTENT_TYPE_EXTENSIONS.get(content_type)

        if extension is None:
            raise ValueError(f"Format d'image non accepté : {content_type or 'absent'}")

        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > MAX_IMAGE_BYTES:
            raise ValueError("L'image dépasse la taille maximale de 10 Mo.")

        destination = output_dir / f"{image_id}{extension}"
        temporary_path = destination.with_suffix(f"{destination.suffix}.part")
        downloaded_bytes = 0

        try:
            with temporary_path.open("wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if not chunk:
                        continue

                    downloaded_bytes += len(chunk)
                    if downloaded_bytes > MAX_IMAGE_BYTES:
                        raise ValueError("L'image dépasse la taille maximale de 10 Mo.")
                    file.write(chunk)

            if downloaded_bytes == 0:
                raise RuntimeError("Le serveur n'a renvoyé aucun octet.")

            # Le fichier final n'apparaît qu'après un téléchargement complet.
            temporary_path.replace(destination)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise

    sha256 = file_sha256(destination)
    write_image_metadata(
        output_dir / f"{image_id}.metadata.json",
        image_id,
        image_url,
        destination,
        downloaded_bytes,
        sha256,
        "downloaded",
    )
    return ImageDownloadResult(
        path=destination,
        size_bytes=downloaded_bytes,
        downloaded=True,
        sha256=sha256,
        source_url=image_url,
        provenance_status="downloaded",
    )


def add_image_evidence(record: dict, image: ImageDownloadResult) -> None:
    """Ajoute à un record brut les preuves produites par ``download_image``."""
    record["_image_path"] = relative_path(image.path)
    record["_image_size"] = image.size_bytes
    record["_image_sha256"] = image.sha256
    record["_downloaded_from_url"] = image.source_url
    record["_image_provenance_status"] = image.provenance_status


def relative_path(path: Path) -> str:
    """Retourne un chemin relatif au projet pour rendre le JSON portable."""
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


def utc_now() -> str:
    """Retourne la date UTC actuelle au format ISO 8601."""
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def save_json_records(records: list[dict], source: str) -> Path:
    """Sauvegarde atomiquement une extraction JSON horodatée."""
    output_dir = DATA_DIR / "raw" / source
    output_dir.mkdir(parents=True, exist_ok=True)
    collected_at = utc_now()
    filename_timestamp = collected_at.replace(":", "").replace("-", "")
    output_path = output_dir / f"extraction_{filename_timestamp}.json"
    payload = {
        "source": source,
        "collected_at": collected_at,
        "count": len(records),
        "records": records,
    }
    write_json_atomic(payload, output_path)
    return output_path
