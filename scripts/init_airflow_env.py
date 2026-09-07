"""Crée une configuration Airflow locale avec des secrets aléatoires."""

import argparse
import base64
import os
import secrets
from pathlib import Path

from dotenv import dotenv_values

from common import PROJECT_ROOT

AIRFLOW_ENV_PATH = PROJECT_ROOT / ".env.airflow"


def generate_fernet_key() -> str:
    """Génère une clé Fernet compatible avec Airflow."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")


def build_configuration(newsdata_api_key: str) -> dict[str, str]:
    """Construit les variables nécessaires sans secret codé en dur."""
    return {
        "AIRFLOW_UID": str(os.getuid()),
        "AIRFLOW_ADMIN_USERNAME": "admin",
        "AIRFLOW_ADMIN_PASSWORD": secrets.token_urlsafe(24),
        "AIRFLOW_FERNET_KEY": generate_fernet_key(),
        "AIRFLOW_JWT_SECRET": secrets.token_urlsafe(48),
        "AIRFLOW_DB_USER": "airflow",
        "AIRFLOW_DB_PASSWORD": secrets.token_urlsafe(24),
        "AIRFLOW_DB_NAME": "airflow",
        "WAREHOUSE_ADMIN_USER": "checkitai_admin",
        "WAREHOUSE_ADMIN_PASSWORD": secrets.token_urlsafe(24),
        "WAREHOUSE_USER": "checkitai_loader",
        "WAREHOUSE_PASSWORD": secrets.token_urlsafe(24),
        "WAREHOUSE_DB": "checkitai",
        "NEWSDATA_API_KEY": newsdata_api_key,
        "CHECKITAI_NEWSDATA_LIMIT": "10",
        "CHECKITAI_NEWSDATA_MAX_PAGES": "1",
        "CHECKITAI_POLITIFACT_LIMIT": "20",
        "CHECKITAI_FAKEDDIT_LIMIT": "20",
        "CHECKITAI_THECONVERSATION_LIMIT": "10",
        "CHECKITAI_REQUEST_DELAY": "1.0",
        "CHECKITAI_DUPLICATE_POLICY": "keep-latest",
        "CHECKITAI_LOAD_BATCH_SIZE": "500",
        "CHECKITAI_TABLE": "publication_multimodale",
        "CHECKITAI_SCHEDULE": "*/15 * * * *",
    }


def write_configuration(path: Path, values: dict[str, str]) -> None:
    """Écrit atomiquement le fichier privé avec des permissions 0600."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.part")
    content = "".join(f"{key}={value}\n" for key, value in values.items())
    try:
        temporary_path.write_text(content, encoding="utf-8")
        temporary_path.chmod(0o600)
        temporary_path.replace(path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def parse_args() -> argparse.Namespace:
    """Lit l'option autorisant le remplacement du fichier existant."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Remplace le fichier .env.airflow s'il existe déjà.",
    )
    return parser.parse_args()


def main() -> None:
    """Génère la configuration en réutilisant la clé NewsData locale si présente."""
    args = parse_args()
    if AIRFLOW_ENV_PATH.exists() and not args.force:
        raise SystemExit(
            f"{AIRFLOW_ENV_PATH} existe déjà ; utilisez --force pour le remplacer."
        )

    project_env = dotenv_values(PROJECT_ROOT / ".env")
    newsdata_api_key = str(project_env.get("NEWSDATA_API_KEY") or "")
    write_configuration(AIRFLOW_ENV_PATH, build_configuration(newsdata_api_key))
    print(f"Configuration créée : {AIRFLOW_ENV_PATH} (permissions 0600)")
    if not newsdata_api_key:
        print(
            "Ajoutez NEWSDATA_API_KEY dans ce fichier avant de lancer le DAG complet."
        )


if __name__ == "__main__":
    main()
