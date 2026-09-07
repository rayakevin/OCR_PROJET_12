"""Tableau de bord Streamlit des KPI du pipeline multimodal."""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from collect_kpis import collect_kpis

CONFIG_PATH = PROJECT_ROOT / "config" / "monitoring.json"


@st.cache_data(ttl=30)
def load_dashboard_data() -> tuple[dict, dict]:
    """Charge les KPI et les seuils, avec un cache court pour l'interactivité."""
    with CONFIG_PATH.open(encoding="utf-8") as file:
        config = json.load(file)
    return collect_kpis(), config


def format_duration(seconds: float) -> str:
    """Affiche une durée en secondes ou minutes selon sa taille."""
    return f"{seconds:.1f} s" if seconds < 60 else f"{seconds / 60:.1f} min"


def format_bytes(value: int) -> str:
    """Affiche une taille avec une unité lisible."""
    units = ["o", "Ko", "Mo", "Go", "To"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} To"


def metric_level(
    value: float, warning: float, critical: float, lower_is_better: bool = False
) -> str:
    """Classe une valeur selon deux seuils d'alerte."""
    if lower_is_better:
        if value >= critical:
            return "critical"
        if value >= warning:
            return "warning"
        return "healthy"

    if value < critical:
        return "critical"
    if value < warning:
        return "warning"
    return "healthy"


def hours_since(timestamp: str) -> float:
    """Calcule l'ancienneté en heures d'un timestamp ISO avec fuseau."""
    event_time = datetime.fromisoformat(timestamp)
    elapsed = datetime.now(UTC) - event_time
    return max(0.0, elapsed.total_seconds() / 3600)


def overall_health(kpis: dict, config: dict) -> tuple[str, str]:
    """Résume l'état du dernier run avec un libellé compréhensible."""
    quality = kpis["quality"]
    performance = kpis["performance"]
    latest = performance["latest_run"]
    if not latest or latest["status"] != "success":
        return "critical", "Échec ou exécution incomplète"

    freshness_hours = hours_since(latest["ended_at"])

    levels = [
        metric_level(
            quality["validity_percent"],
            config["validity_warning_percent"],
            config["validity_critical_percent"],
        ),
        metric_level(
            quality["multimodal_percent"],
            config["multimodal_warning_percent"],
            config["multimodal_critical_percent"],
        ),
        metric_level(
            performance["success_percent"],
            config["success_warning_percent"],
            config["success_critical_percent"],
        ),
        metric_level(
            latest["duration_seconds"],
            config["duration_warning_seconds"],
            config["duration_critical_seconds"],
            lower_is_better=True,
        ),
        metric_level(
            freshness_hours,
            config["freshness_warning_hours"],
            config["freshness_critical_hours"],
            lower_is_better=True,
        ),
    ]
    if "critical" in levels:
        return "critical", "Action nécessaire"
    if "warning" in levels:
        return "warning", "À surveiller"
    return "healthy", "Pipeline opérationnel"


def render_status(level: str, message: str) -> None:
    """Affiche le bandeau de santé avec la couleur adaptée."""
    if level == "critical":
        st.error(message, icon=":material/error:")
    elif level == "warning":
        st.warning(message, icon=":material/warning:")
    else:
        st.success(message, icon=":material/check_circle:")


st.set_page_config(
    page_title="Suivi ETL CheckIt.AI",
    page_icon=":material/monitoring:",
    layout="wide",
)
st.title("Suivi du pipeline de données multimodales")
st.caption("Qualité du lot courant, rapidité d'exécution et coût opérationnel estimé")

try:
    kpis, config = load_dashboard_data()
except (
    FileNotFoundError,
    TypeError,
    ValueError,
    KeyError,
    json.JSONDecodeError,
) as error:
    st.error(f"Les données de monitoring ne sont pas disponibles : {error}")
    st.stop()

if st.sidebar.button("Actualiser les données", icon=":material/refresh:"):
    load_dashboard_data.clear()
    st.rerun()

st.sidebar.header("Hypothèses de coût")
compute_rate = st.sidebar.number_input(
    "Calcul (€ par heure)",
    min_value=0.0,
    value=float(config["compute_eur_per_hour"]),
    step=0.01,
)
storage_rate = st.sidebar.number_input(
    "Stockage (€ par Go et par mois)",
    min_value=0.0,
    value=float(config["storage_eur_per_gb_month"]),
    step=0.01,
)

quality = kpis["quality"]
performance = kpis["performance"]
latest = performance["latest_run"]
level, health_message = overall_health(kpis, config)
render_status(level, health_message)

if latest:
    compute_cost = latest["duration_seconds"] / 3600 * compute_rate
    freshness_hours = hours_since(latest["ended_at"])
else:
    compute_cost = 0.0
    freshness_hours = None

storage_gb = kpis["storage"]["total_bytes"] / 1024**3
storage_cost = storage_gb * storage_rate

overview_columns = st.columns(4)
overview_columns[0].metric(
    "Données valides",
    f"{quality['validity_percent']:.1f} %",
    help="Part des entrées du lot courant ayant passé les contrôles.",
    border=True,
)
overview_columns[1].metric(
    "Texte + image complets",
    f"{quality['multimodal_percent']:.1f} %",
    help="Part du lot courant possédant texte et preuve d'image.",
    border=True,
)
overview_columns[2].metric(
    "Durée du dernier run",
    format_duration(latest["duration_seconds"]) if latest else "Indisponible",
    help="Temps entre le premier log d'extraction et la dernière validation.",
    border=True,
)
overview_columns[3].metric(
    "Coût estimé du run",
    f"{compute_cost:.4f} €",
    help="Durée du run multipliée par le tarif horaire choisi dans la barre latérale.",
    border=True,
)

quality_tab, performance_tab, cost_tab, details_tab = st.tabs(
    ["Qualité", "Performance", "Coût", "Détails"]
)

with quality_tab:
    left, right = st.columns(2)
    with left:
        st.subheader("Publications du lot courant par source")
        source_frame = pd.DataFrame(kpis["sources"])
        st.bar_chart(
            source_frame,
            x="source_name",
            y="publications",
            x_label="Source",
            y_label="Nombre de publications",
        )
    with right:
        st.subheader("Résultat du lot courant")
        cleaning_frame = pd.DataFrame(
            {
                "Résultat": ["Conservées", "Doublons retirés", "Invalides"],
                "Nombre": [
                    quality["accepted_count"],
                    quality["duplicate_count"],
                    quality["validation_rejected_count"],
                ],
            }
        )
        st.bar_chart(
            cleaning_frame, x="Résultat", y="Nombre", y_label="Nombre d'entrées"
        )
    st.info(
        f"{quality['duplicate_count']} occurrences répétées du run ont été retirées. "
        "Elles ne sont pas comptées comme invalides."
    )

with performance_tab:
    st.subheader("Historique des exécutions incrémentales")
    history = pd.DataFrame(
        [
            {
                "date": run["started_at"],
                "durée (s)": run["duration_seconds"],
                "état": run["status"],
            }
            for run in performance["history"]
        ]
    )
    if not history.empty:
        history["date"] = pd.to_datetime(history["date"])
        st.line_chart(
            history,
            x="date",
            y="durée (s)",
            x_label="Exécution",
            y_label="Durée en secondes",
        )
        st.dataframe(history, hide_index=True, width="stretch")

    if latest:
        st.subheader("Durée par tâche — dernier run")
        task_frame = pd.DataFrame(
            [
                {"Tâche": task_name, "Durée (s)": values["duration_seconds"]}
                for task_name, values in latest["tasks"].items()
            ]
        ).sort_values("Durée (s)", ascending=False)
        st.bar_chart(
            task_frame, x="Tâche", y="Durée (s)", x_label="Tâche", y_label="Secondes"
        )
    st.metric("Taux de succès historique", f"{performance['success_percent']:.1f} %")

with cost_tab:
    cost_columns = st.columns(3)
    cost_columns[0].metric(
        "Volume surveillé", format_bytes(kpis["storage"]["total_bytes"])
    )
    cost_columns[1].metric("Stockage estimé / mois", f"{storage_cost:.4f} €")
    cost_columns[2].metric(
        "Ancienneté du dernier run",
        f"{freshness_hours:.1f} h" if freshness_hours is not None else "Indisponible",
    )

    storage_frame = pd.DataFrame(
        [
            {"Catégorie": name, "Taille (Mo)": round(value / 1024**2, 2)}
            for name, value in kpis["storage"]["breakdown_bytes"].items()
        ]
    )
    st.bar_chart(storage_frame, x="Catégorie", y="Taille (Mo)", y_label="Mégaoctets")
    st.caption(
        "Les coûts sont des estimations locales ajustables, pas une facture fournisseur."
    )

with details_tab:
    st.subheader("Définition des indicateurs")
    st.dataframe(
        pd.DataFrame(
            [
                ["Validité", "Entrées validées / entrées brutes", "≥ 98 %"],
                [
                    "Complétude multimodale",
                    "Publications avec texte et image / publications finales",
                    "≥ 99 %",
                ],
                ["Taux de succès", "Runs réussis / runs détectés", "≥ 95 %"],
                [
                    "Durée",
                    "Fin de validation − début de la première extraction",
                    "< 60 s",
                ],
                [
                    "Coût calcul",
                    "Durée × tarif horaire configurable",
                    "Suivi de tendance",
                ],
                [
                    "Coût stockage",
                    "Volume suivi × tarif mensuel configurable",
                    "Suivi de tendance",
                ],
            ],
            columns=["KPI", "Calcul", "Cible"],
        ),
        hide_index=True,
        width="stretch",
    )
    st.caption(
        f"Indicateurs recalculés à {kpis['generated_at']} — cache maximal : 30 secondes."
    )
