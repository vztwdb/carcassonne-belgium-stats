# Carcassonne Belgium Stats

Verzameling en visualisatie van statistieken over Belgische spelers van het bordspel Carcassonne.

## Databronnen

| Bron | Beschrijving | Formaat |
|------|-------------|---------|
| **BK** | Live Belgisch Kampioenschap | Excel / PDF |
| **BCOC** | Belgian Carcassonne Online Cup | TBD |
| **BCL** | Belgian Carcassonne League | TBD |
| **Nations** | Nationale ploeg competities | TBD |

## Tech Stack

- **Python 3.11+** — data verwerking
- **DuckDB** — embedded database
- **pandas / openpyxl / pdfplumber** — data parsing
- **Streamlit + Plotly** — visualisatie dashboard
- **rapidfuzz** — spelernamen normalisatie

## Projectstructuur

```
carcassonne-belgium-stats/
├── data/
│   ├── raw/              # Originele bronbestanden (niet in git)
│   │   ├── bk/           # BK Excel & PDF bestanden
│   │   ├── bcoc/         # BCOC bestanden
│   │   ├── bcl/          # BCL bestanden
│   │   └── nations/      # Nations Cup bestanden
│   ├── processed/        # Tussentijdse CSV/JSON
│   └── players_aliases.json
├── migrations/
│   └── 001_initial_schema.sql
├── src/
│   ├── parsers/          # Excel & PDF parsers per bron
│   ├── importers/        # Database import scripts
│   └── models/           # Data modellen
├── dashboard/
│   ├── app.py            # Streamlit hoofdapp
│   ├── queries.py        # DuckDB query helpers
│   └── pages/            # Pagina's: spelers, rankings, tornooien
├── docs/                 # ER-diagram, inventarissen
├── requirements.txt
└── README.md
```

## Installatie

```bash
python -m venv .venv
.venv/Scripts/activate      # Windows
pip install -r requirements.txt
```

## Database initialiseren

```bash
python -c "import duckdb; conn = duckdb.connect('data/carcassonne.duckdb'); conn.executescript(open('migrations/001_initial_schema.sql').read())"
```

## Dashboard starten

```bash
streamlit run dashboard/app.py
```

## Linear Project

[Carcassonne belgium — Linear](https://linear.app/comulab/project/carcassonne-belgium-762fe89142b3)
