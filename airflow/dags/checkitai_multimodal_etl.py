"""Orchestre l'ETL multimodal CheckIt.AI vers PostgreSQL."""

import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DAG_ID = "checkitai_multimodal_etl"
POSTGRES_CONN_ID = "checkitai_warehouse"
EXTRACT_TASK_SOURCES = {
    "extract_newsdata": "newsdata",
    "extract_politifact": "politifact",
    "extract_fakeddit": "fakeddit",
    "extract_theconversation": "theconversation",
}


def project_root() -> Path:
    """Retourne la racine du projet montée dans le conteneur Airflow."""
    return Path(os.getenv("CHECKITAI_PROJECT_ROOT", "/opt/checkitai"))


def import_project_scripts() -> Path:
    """Rend les modules du dossier scripts importables par les tâches."""
    root = project_root()
    scripts_dir = str(root / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    return root


def positive_int_env(name: str, default: int) -> int:
    """Lit un entier positif configurable depuis l'environnement."""
    value = int(os.getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} doit être positif.")
    return value


def save_extraction_result(records: list[dict], source: str) -> dict[str, object]:
    """Sauvegarde un lot et retourne sa référence exacte pour la tâche suivante."""
    import_project_scripts()
    from common import relative_path, save_json_records

    output_path = save_json_records(records, source)
    return {
        "source": source,
        "path": relative_path(output_path),
        "count": len(records),
    }


def current_run_input_paths(ti) -> list[Path]:
    """Récupère dans XCom les quatre lots créés par le run Airflow courant."""
    root = project_root()
    task_ids = list(EXTRACT_TASK_SOURCES)
    results = ti.xcom_pull(task_ids=task_ids)
    if not isinstance(results, list) or len(results) != len(task_ids):
        raise RuntimeError("Les références des lots d'extraction sont incomplètes.")

    input_paths: list[Path] = []
    raw_root = (root / "data/raw").resolve()
    for task_id, result in zip(task_ids, results, strict=True):
        expected_source = EXTRACT_TASK_SOURCES[task_id]
        if not isinstance(result, dict) or result.get("source") != expected_source:
            raise RuntimeError(f"XCom invalide pour {task_id} : {result!r}")

        relative_batch_path = result.get("path")
        if not isinstance(relative_batch_path, str):
            raise RuntimeError(f"Chemin de lot absent dans le XCom de {task_id}.")
        batch_path = (root / relative_batch_path).resolve()
        try:
            batch_path.relative_to(raw_root)
        except ValueError as error:
            raise RuntimeError(f"Lot hors de data/raw : {batch_path}") from error
        if not batch_path.is_file():
            raise FileNotFoundError(f"Lot du run courant absent : {batch_path}")
        input_paths.append(batch_path)

    return input_paths


def extract_newsdata_task() -> dict[str, object]:
    """Extrait et sauvegarde un lot NewsData.io."""
    import_project_scripts()
    from extract_newsdata import extract_newsdata

    from common import configure_logging

    api_key = os.getenv("NEWSDATA_API_KEY")
    if not api_key:
        raise RuntimeError("NEWSDATA_API_KEY est absente de l'environnement Airflow.")
    logger = configure_logging("newsdata")
    records = extract_newsdata(
        api_key=api_key,
        limit=positive_int_env("CHECKITAI_NEWSDATA_LIMIT", 10),
        max_pages=positive_int_env("CHECKITAI_NEWSDATA_MAX_PAGES", 1),
        query=os.getenv("CHECKITAI_NEWSDATA_QUERY") or None,
        refresh_images=False,
        logger=logger,
    )
    return save_extraction_result(records, "newsdata")


def extract_politifact_task() -> dict[str, object]:
    """Extrait et sauvegarde un lot PolitiFact."""
    import_project_scripts()
    from extract_politifact import extract_politifact

    from common import configure_logging

    logger = configure_logging("politifact")
    records = extract_politifact(
        limit=positive_int_env("CHECKITAI_POLITIFACT_LIMIT", 20),
        refresh_images=False,
        logger=logger,
    )
    return save_extraction_result(records, "politifact")


def extract_fakeddit_task() -> dict[str, object]:
    """Extrait et sauvegarde un lot Fakeddit depuis le TSV monté."""
    root = import_project_scripts()
    from extract_fakeddit import (
        extract_fakeddit,
        read_fakeddit_cursor,
        write_fakeddit_cursor,
    )

    from common import configure_logging

    logger = configure_logging("fakeddit")
    tsv_path = Path(
        os.getenv(
            "CHECKITAI_FAKEDDIT_TSV",
            str(root / "data/external/fakeddit/multimodal_train.tsv"),
        )
    )
    cursor_path = root / "data/state/fakeddit_cursor.json"
    start_line = read_fakeddit_cursor(cursor_path)
    records, next_line = extract_fakeddit(
        tsv_path=tsv_path,
        limit=positive_int_env("CHECKITAI_FAKEDDIT_LIMIT", 20),
        start_line=start_line,
        refresh_images=False,
        logger=logger,
    )
    result = save_extraction_result(records, "fakeddit")
    write_fakeddit_cursor(next_line, cursor_path)
    logger.info("Curseur Fakeddit avancé de la ligne %s à %s", start_line, next_line)
    return result


def extract_theconversation_task() -> dict[str, object]:
    """Extrait et sauvegarde un lot The Conversation France."""
    import_project_scripts()
    from extract_theconversation import extract_theconversation

    from common import configure_logging

    logger = configure_logging("theconversation")
    request_delay = float(os.getenv("CHECKITAI_REQUEST_DELAY", "1.0"))
    if request_delay < 0:
        raise ValueError("CHECKITAI_REQUEST_DELAY doit être positif ou nul.")
    records = extract_theconversation(
        limit=positive_int_env("CHECKITAI_THECONVERSATION_LIMIT", 10),
        request_delay=request_delay,
        refresh_images=False,
        logger=logger,
    )
    return save_extraction_result(records, "theconversation")


def transform_task(ti) -> int:
    """Transforme uniquement les lots créés par le run Airflow courant."""
    root = import_project_scripts()
    from common import configure_logging
    from transform_data import run_transformation

    logger = configure_logging("transform")
    manifest = run_transformation(
        input_paths=current_run_input_paths(ti),
        duplicate_policy=os.getenv("CHECKITAI_DUPLICATE_POLICY", "keep-latest"),
        processed_dir=root / "data/processed",
        rejected_dir=root / "data/rejected",
        output_format="both",
        logger=logger,
    )
    return int(manifest["accepted_count"])


def load_postgres_task() -> int:
    """Charge le Parquet dans la base métier via une connexion Airflow."""
    root = import_project_scripts()
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from common import configure_logging
    from load_postgres import load_publications

    logger = configure_logging("load_postgres")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = hook.get_conn()
    try:
        return load_publications(
            connection=connection,
            parquet_path=root / "data/processed/publications.parquet",
            table_name=os.getenv("CHECKITAI_TABLE", "publication_multimodale"),
            batch_size=positive_int_env("CHECKITAI_LOAD_BATCH_SIZE", 500),
            logger=logger,
        )
    finally:
        connection.close()


def validate_postgres_task(ti) -> dict[str, int]:
    """Compare le volume chargé et contrôle les associations multimodales."""
    import_project_scripts()
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from load_postgres import validate_loaded_table

    expected_count = int(ti.xcom_pull(task_ids="transform"))
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = hook.get_conn()
    try:
        return validate_loaded_table(
            connection=connection,
            table_name=os.getenv("CHECKITAI_TABLE", "publication_multimodale"),
            minimum_count=expected_count,
        )
    finally:
        connection.close()


with DAG(
    dag_id=DAG_ID,
    description="Extraction, transformation et chargement des publications multimodales",
    schedule=os.getenv("CHECKITAI_SCHEDULE") or "*/15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "checkitai",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["etl", "multimodal", "postgresql"],
) as dag:
    extract_newsdata = PythonOperator(
        task_id="extract_newsdata",
        python_callable=extract_newsdata_task,
    )
    extract_politifact = PythonOperator(
        task_id="extract_politifact",
        python_callable=extract_politifact_task,
    )
    extract_fakeddit = PythonOperator(
        task_id="extract_fakeddit",
        python_callable=extract_fakeddit_task,
    )
    extract_theconversation = PythonOperator(
        task_id="extract_theconversation",
        python_callable=extract_theconversation_task,
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )
    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_postgres_task,
    )
    validate_postgres = PythonOperator(
        task_id="validate_postgres",
        python_callable=validate_postgres_task,
    )

    (
        [
            extract_newsdata,
            extract_politifact,
            extract_fakeddit,
            extract_theconversation,
        ]
        >> transform
        >> load_postgres
        >> validate_postgres
    )
