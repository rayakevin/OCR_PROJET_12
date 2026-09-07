# Preuve de fonctionnement — tableau de bord KPI

Test réalisé le 7 septembre 2026 avec le moteur de test Streamlit.

```text
Exceptions : 0
Bandeau : Pipeline opérationnel
Onglets : Qualité, Performance, Coût, Détails

Données valides             100,0 %
Texte + image complets      100,0 %
Durée du dernier run         15,0 s
Coût estimé du run           0,0008 €
Taux de succès historique   100,0 %
Volume surveillé             14,4 Mo
Stockage estimé / mois        0,0004 €
Ancienneté du dernier run     0,0 h
```

Les KPI de qualité ont été calculés sur le lot incrémental de référence : 60
occurrences lues et 60 publications multimodales acceptées. Les indicateurs de
performance couvrent les trois premiers runs du pipeline incrémental, tous
réussis. Le volume disque et l'historique ne contiennent que ce fonctionnement.

Les seuils de fraîcheur sont alignés sur la planification toutes les 15 minutes :
avertissement après 30 minutes sans succès et état critique après 60 minutes.

Commande de reproduction :

```bash
uv run python - <<'PY'
from streamlit.testing.v1 import AppTest

app = AppTest.from_file("dashboard/etl_kpi_dashboard.py").run(timeout=30)
print("exceptions:", len(app.exception))
print("metrics:", [(metric.label, metric.value) for metric in app.metric])
print("tabs:", [tab.label for tab in app.tabs])
PY
```
