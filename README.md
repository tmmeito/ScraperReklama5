# Reklama5 Scraper

Ein in Python implementierter Scraper für die Automobil-Kategorie von [reklama5.mk](https://www.reklama5.mk/).
Der bestehende Terminal-Workflow kann weiterhin interaktiv bedient werden, zusätzlich steht jetzt ein nicht-interaktiver
CLI-Modus zur Verfügung.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Interaktiver Modus

```bash
python src/scraperReklama5.py
```

Über das Menü lassen sich neue Suchen starten oder bestehende CSV-Dateien analysieren.

## Automatischer CLI-Modus

Wird das Skript mit Argumenten gestartet, greift automatisch der nicht-interaktive Pfad. Die wichtigsten Argumente:

| Argument | Beschreibung |
| --- | --- |
| `--search` | Suchbegriff (Standard: leer = alle Treffer). |
| `--days` | Wie viele Tage zurück berücksichtigt werden sollen (Standard: 1). |
| `--limit` | Maximale Anzahl zu speichernder Einträge. |
| `--details` | Aktiviert die Detail-Erfassung (Einzelaufruf jeder Anzeige). |
| `--details-delay` | Feste Pause zwischen Detail-Aufrufen (Sekunden). |
| `--details-workers` | Anzahl paralleler Detail-Aufrufe (1–5, Standard 3). |
| `--details-rate-limit` | Begrenzt die gleichzeitigen Detail-Aufrufe. |
| `--csv` | Pfad der Zieldatei für die Rohdaten (Standard: `reklama5_autos_raw.csv`). |
| `--base-url` | Alternative Such-URL mit den Platzhaltern `{search_term}` und `{page_num}`. |

### Beispiel

```bash
python src/scraperReklama5.py \
    --search aygo \
    --days 2 \
    --limit 50 \
    --details --details-delay 0.5 \
    --csv data/aygo.csv
```

Der Lauf speichert die Ergebnisse in `data/aygo.csv` und beendet sich anschließend automatisch. Eine Zusammenfassung
wird direkt ausgegeben.

## Tests

```bash
pytest
```
