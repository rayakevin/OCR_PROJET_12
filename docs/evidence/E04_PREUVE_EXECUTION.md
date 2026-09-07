# Preuve d'exécution — DAG `checkitai_multimodal_etl`

Date du test : 7 septembre 2026
Version Airflow : 3.3.1  
Run ID : `scheduled__2026-09-07T09:45:00+00:00`
État final : `success`

## État des tâches retourné par Airflow

| Tâche | État | Début UTC | Fin UTC | Durée |
|---|---|---|---|---:|
| `extract_newsdata` | success | 09:48:42.170992 | 09:48:43.287932 | 1,117 s |
| `extract_politifact` | success | 09:48:42.170236 | 09:48:43.314063 | 1,144 s |
| `extract_fakeddit` | success | 09:48:42.168116 | 09:48:48.883017 | 6,715 s |
| `extract_theconversation` | success | 09:48:42.168118 | 09:48:53.628208 | 11,460 s |
| `transform` | success | 09:48:54.281085 | 09:48:54.986972 | 0,706 s |
| `load_postgres` | success | 09:48:55.407839 | 09:48:56.054173 | 0,646 s |
| `validate_postgres` | success | 09:48:56.518960 | 09:48:57.129836 | 0,611 s |

Durée globale calculée depuis les logs : **14,962 secondes**.

## Preuve du passage incrémental par XCom

Chaque tâche Extract a retourné le chemin exact du lot créé pendant ce run :

```text
extract_newsdata        -> data/raw/newsdata/extraction_20260907T094843Z.json (10)
extract_politifact      -> data/raw/politifact/extraction_20260907T094843Z.json (20)
extract_fakeddit        -> data/raw/fakeddit/extraction_20260907T094848Z.json (20)
extract_theconversation -> data/raw/theconversation/extraction_20260907T094853Z.json (10)
```

Le manifeste porte le mode `incremental_exact_batches`, le schéma `1.3` et ces
quatre entrées uniquement : **60 occurrences lues, 60 publications acceptées,
aucun doublon dans le run et aucun rejet de validation**. Le collecteur KPI
refuse tout autre mode, une version de schéma différente ou un ensemble de
sources incomplet.

Fakeddit a repris à la ligne 46 du TSV puis a avancé le curseur à la ligne 68,
uniquement après la sauvegarde du lot de 20 couples texte-image.

## Valeurs retournées par les tâches

```text
transform           -> 60
load_postgres       -> 60
validate_postgres   -> {
  'total': 104,
  'invalid_multimodal': 0,
  'duplicate_ids': 0
}
```

## Contrôle de la table métier cumulative

```text
source_name                publications
Fakeddit                             60
NewsData.io                          14
PolitiFact                           20
The Conversation France             10

total                    104
ids_uniques              104
associations_invalides     0
```

Ces 104 lignes proviennent uniquement des trois premières exécutions du pipeline
incrémental courant. L'upsert a mis à jour les publications déjà connues et
Fakeddit a fourni 20 nouvelles publications à chaque run.

## Preuve de la planification

La configuration `CHECKITAI_SCHEDULE=*/15 * * * *` est chargée par le DAG. Le
scheduler a créé automatiquement les runs de 09:30 et 09:45 UTC, tous deux
terminés avec l'état `success`. Le
déclenchement manuel reste disponible pour la démonstration et le diagnostic.

## Contrôle du rôle de chargement

```text
rolname           rolsuper  rolcreatedb  rolcreaterole  rolcanlogin
checkitai_loader  false     false        false          true
```

## Commandes de reproduction

```bash
docker compose --env-file .env.airflow -f docker-compose.airflow.yml \
  exec airflow-apiserver airflow dags list-import-errors

docker compose --env-file .env.airflow -f docker-compose.airflow.yml \
  exec airflow-apiserver airflow dags trigger checkitai_multimodal_etl

docker compose --env-file .env.airflow -f docker-compose.airflow.yml \
  exec airflow-apiserver airflow tasks states-for-dag-run \
  checkitai_multimodal_etl '<run_id>'
```

Le graphe [`checkitai_multimodal_etl.png`](checkitai_multimodal_etl.png) est
exporté directement par la commande `airflow dags show`.

## Test d'idempotence du chargement

Le Parquet courant de 60 lignes a été chargé une seconde fois. PostgreSQL est
resté à **104 lignes et 104 `publication_id` distincts** : l'upsert n'a créé
aucune ligne supplémentaire.
