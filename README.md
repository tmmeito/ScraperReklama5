# Reklama5 Scraper

Ein vielseitiger Python-Scraper für die Automobil-Kategorie von [reklama5.mk](https://www.reklama5.mk/). Das Tool bietet einen geführten Terminal-Workflow, einen vollautomatischen CLI-Modus sowie Auswertungen auf Basis einer SQLite-Datenbank.

## Inhaltsverzeichnis
- [Hauptfunktionen](#hauptfunktionen)
- [Voraussetzungen und Installation](#voraussetzungen-und-installation)
- [Datenablage und Konfigurationsdateien](#datenablage-und-konfigurationsdateien)
- [Interaktiver Modus](#interaktiver-modus)
  - [Hauptmenü](#hauptmenü)
  - [Einstellungen](#einstellungen)
  - [Neue Suche starten](#neue-suche-starten)
  - [Analyse-Center](#analyse-center)
- [Automatischer CLI-Modus](#automatischer-cli-modus)
  - [Parameterreferenz](#parameterreferenz)
  - [Beispiel-Workflows](#beispiel-workflows)
- [Detailerfassung, Deduplizierung und Status-Erkennung](#detailerfassung-deduplizierung-und-status-erkennung)
- [Ausgabeformate](#ausgabeformate)
  - [CSV-Felder](#csv-felder)
  - [JSON-Aggregationen](#json-aggregationen)
  - [SQLite-Datenbank](#sqlite-datenbank)
- [Tests](#tests)

## Hauptfunktionen
- **Geführte Terminal-App** mit drei Hauptbereichen: neue Suche, Analyse vorhandener Daten und Verwaltung der Einstellungen.
- **Automatischer CLI-Modus** für geplante Läufe (z. B. via Cron). Alle Parameter des interaktiven Workflows lassen sich als Argumente setzen.
- **Flexible Speicherung**: Wahlweise CSV-Export oder persistente Speicherung in einer SQLite-Datenbank (`data/reklama5.db`).
- **Detailerfassung**: Optionaler Abruf jeder Einzelanzeige mit konfigurierbarer Worker-Anzahl, Verzögerung und Rate-Limit.
- **Deduplizierung & Änderungsdetektion**: Vor jedem Speichern werden Inserate nach ID bereinigt, bestehende Datensätze werden auf Änderungen geprüft und als *neu*, *geändert* oder *unverändert* markiert.
- **Analyse-Center**: Auswertungen zu häufigsten Marken/Modellen, Durchschnittspreisen pro Modell und Baujahr sowie ein Feed der letzten Preisänderungen.
- **Konfigurierbare Such-URL**: Eigene Basis-URL möglich (z. B. für vordefinierte Filter). Platzhalter `{search_term}` und `{page_num}` werden automatisch ersetzt.

## Voraussetzungen und Installation
1. Python **3.10+** installieren.
2. Repository klonen oder herunterladen.
3. Abhängigkeiten installieren (nur `beautifulsoup4`).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional: ein `data/`-Verzeichnis anlegen (wird bei Bedarf automatisch erstellt), falls Einstellungen oder die SQLite-Datenbank dauerhaft gespeichert werden sollen.

## Datenablage und Konfigurationsdateien
| Pfad | Inhalt |
| --- | --- |
| `data/user_settings.json` | Persistente Standardeinstellungen für den interaktiven Modus. Wird über das Einstellungsmenü gepflegt. |
| `data/reklama5.db` | SQLite-Datenbank mit allen gespeicherten Inseraten inklusive Änderungsverlauf. Aktiviert durch die SQLite-Option im Menü oder via `--use-sqlite`. |
| `reklama5_autos_raw.csv` | Standard-CSV-Datei für Rohdaten, sofern kein eigener Pfad angegeben wird. |
| `reklama5_autos_agg.json` | Aggregierte Statistik (siehe [JSON-Aggregationen](#json-aggregationen)). |

## Interaktiver Modus
Start des geführten Terminal-Workflows:

```bash
python src/scraperReklama5.py
```

### Hauptmenü
| Auswahl | Funktion |
| --- | --- |
| `1` | **Neue Suche durchführen** – startet den Scraper mit den aktuellen Standardeinstellungen (oder temporären Anpassungen). |
| `2` | **Analyse** – öffnet das Analyse-Center auf Basis der SQLite-Datenbank. |
| `3` | **Einstellungen** – verwaltet sämtliche Standardwerte (siehe unten). |
| `q` | Programm beenden.

### Einstellungen
Jede Option aktualisiert `data/user_settings.json`. Übersicht:

| Menüpunkt | Beschreibung |
| --- | --- |
| Basis-URL | Individuelle Such-URL mit `{search_term}` und `{page_num}` setzen oder auf Standard zurücksetzen. |
| Suchbegriff | Standard-Query; „leer“ bedeutet alle Inserate. |
| Tage | Zeitfenster (min. 1) für die Beachtung neuer Inserate. |
| Limit | Maximale Anzahl zu speichernder Inserate (0/leer = alle). |
| Detail-Erfassung | Einzelaufrufe der Anzeigen aktivieren/deaktivieren. |
| Detail-Worker | Anzahl paralleler Threads (1–5) für Detailseiten. |
| Detail-Pause | Feste oder zufällige Wartezeit zwischen Detailaufrufen; `auto` entspricht 1–2 s. |
| Detail-Rate-Limit | Maximal gleichzeitige Detailabrufe; nützlich für vorsichtiges Crawling. |
| CSV-Datei | Standardpfad für Rohdaten, falls kein SQLite genutzt wird. |
| SQLite-Speicherung | Aktiviert `data/reklama5.db` als Ziel. CSV-Export entfällt dann. |
| Umgang mit unveränderten Einträgen | Option „überspringen“ speichert nur neue/geänderte Inserate, „markieren“ schreibt alle Treffer. |

### Neue Suche starten
1. Einstellungen prüfen – optional per `[e]` einzelne Werte temporär überschreiben (z. B. ad-hoc Suchbegriff oder Limit).
2. Scraper ruft nacheinander die Ergebnisseiten auf, filtert nach `days`, dedupliziert IDs und klassifiziert anhand der SQLite-Datenbank.
3. Je nach Detailmodus werden zusätzliche Felder (Treibstoff, Getriebe, Farbe, …) nachgeladen. Verzögerung, Worker und Rate-Limit greifen gemäß Konfiguration.
4. Speicherung:
   - **CSV**: Append-Mode mit Überschrift bei Erstschreibzugriff.
   - **SQLite**: `listings`-Tabelle wird per Upsert aktualisiert, Änderungen landen zusätzlich in `listing_changes`.
5. Nach jedem Lauf wird eine Zusammenfassung angezeigt (z. B. Anzahl neuer/geänderter Inserate, Duplikate, Dauer) und `reklama5_autos_agg.json` aktualisiert.
6. Bei SQLite-Nutzung geht es automatisch ins Analyse-Center; ansonsten zurück ins Hauptmenü.

### Analyse-Center
Nur verfügbar, wenn `data/reklama5.db` existiert. Funktionen:

1. **Häufigste Automarken/-modelle** (`display_make_model_summary`)
   - Zeigt Anzahl, ausgeschlossene Niedrigpreis-Inserate und Durchschnittspreise pro Marke/Modell/Treibstoff.
2. **Durchschnittspreise pro Modell & Baujahr** (`display_avg_price_by_model_year`)
   - Nutzt `year`-Feld, Mindestpreisfilter und zeigt ebenfalls ausgeschlossene günstige Einträge.
3. **Einstellungen**
   - Mindestpreis für Durchschnittsberechnung, Tagesfilter (`last_seen`), Freitextfilter (Marke/Modell/Treibstoff).
4. **Preisänderungs-Feed**
   - Nach jeder Auswertung werden die letzten Änderungen aus `listing_changes` ausgegeben.

## Automatischer CLI-Modus
Startet automatisch, sobald das Skript mit Argumenten aufgerufen wird. Ideal für Skripte/CI.

```bash
python src/scraperReklama5.py --search aygo --days 3 --limit 100 --details --details-workers 5 --details-delay 0.5 --use-sqlite
```

### Parameterreferenz
| Argument | Beschreibung | Standard |
| --- | --- | --- |
| `--search` | Suchbegriff (leer = alle Inserate). | `""` |
| `--days` | Anzahl der zu betrachtenden Tage. Muss > 0 sein. | `1` |
| `--limit` | Obergrenze für zu speichernde Inserate. | unbegrenzt |
| `--details` | Aktiviert Detailerfassung. | deaktiviert |
| `--details-workers` | Parallele Detailabrufe (1–5). | `3` |
| `--details-delay` | Feste Pause (Sek.) zwischen Detailabrufen. `0` deaktiviert Wartezeit, keine Angabe => 1–2 s Zufallsbereich. | `auto` |
| `--details-rate-limit` | Begrenzung gleichzeitiger Detailaufrufe (≤ Worker). | kein Limit |
| `--csv` | Zieldatei für Rohdaten (deaktiv, wenn `--use-sqlite`). | `reklama5_autos_raw.csv` |
| `--use-sqlite` | Speichert in `data/reklama5.db` und aktiviert Änderungsverfolgung. | aus |
| `--base-url` | Eigene Such-URL mit `{search_term}` und `{page_num}`. | Standard-URL |
| `--skip-unchanged` | Unveränderte Inserate nicht erneut speichern. | aus |

### Beispiel-Workflows
1. **CSV-Export ohne Details**
   ```bash
   python src/scraperReklama5.py --search golf --days 2 --limit 50 --csv data/golf.csv
   ```
2. **Langsame Detailerfassung mit Rate-Limit**
   ```bash
   python src/scraperReklama5.py --details --details-workers 4 --details-delay 1.5 --details-rate-limit 2 --skip-unchanged --use-sqlite
   ```
3. **Eigene Basis-URL verwenden**
   ```bash
   python src/scraperReklama5.py --base-url "https://www.reklama5.mk/Search?cat=24&city=1&q={search_term}&page={page_num}" --search skoda
   ```

## Detailerfassung, Deduplizierung und Status-Erkennung
- **Deduplizierung:** Jede Listing-ID wird pro Lauf nur einmal verarbeitet. Doppelte IDs werden gezählt und übersprungen.
- **Status-Spalte (`_status`):** Vergleicht aktuelle Werte mit SQLite-Bestand (Felder `link`, `year`, `price`, `km`, `kw`, `ps`, `date`, `city`). Ergebnis: `neu`, `geändert`, `unverändert`.
- **Detailfelder:** Treibstoff, Getriebe, Karosserie, Farbe, Registrierung, Zulassung bis, Emissionsklasse werden nur durch Detailabrufe befüllt.
- **Verzögerungen & Rate-Limits:** Kombination aus Worker-Anzahl, optionaler globaler Semaphore (`--details-rate-limit`) und zufälligem oder festem Delay reduziert die Last auf reklama5.mk.
- **Skip-Modus:** Mit `skip_unchanged`/`--skip-unchanged` landen nur neue oder geänderte Inserate im Speicher. Ohne Skip werden unveränderte zwar gespeichert, aber markiert.

## Ausgabeformate

### CSV-Felder
| Feld | Beschreibung |
| --- | --- |
| `id` | Anzeigen-ID (aus URL extrahiert). |
| `link` | Absolute URL zur Anzeige. |
| `make`, `model` | Marke und Modell (falls erkennbar). |
| `year`, `price`, `km`, `kw`, `ps` | Basisdaten aus Übersichts- oder Detailseite. |
| `fuel`, `gearbox`, `body`, `color`, `registration`, `reg_until`, `emission_class` | Detailinformationen (nur bei aktivierter Detailerfassung verfügbar). |
| `date` | Veröffentlichungs- bzw. Aktualisierungszeit (lokales Format von reklama5.mk). |
| `city` | Stadt/Region. |
| `promoted` | `1`, wenn Anzeige als „promoted“ markiert ist. |

### JSON-Aggregationen
Nach jedem Lauf wird `reklama5_autos_agg.json` aktualisiert. Struktur:
```json
{
  "Toyota Aygo": {
    "count_total": 42,
    "count_with_price": 40,
    "avg_price": 4880.0
  },
  "VW Golf": { ... }
}
```
Die Datei enthält die Summe und Durchschnittspreise (unter Berücksichtigung des Mindestpreises) für jede Kombination aus Marke und Modell – egal ob CSV oder SQLite als Quelle diente.

### SQLite-Datenbank
- Tabelle `listings`: enthält alle CSV-Felder plus `hash`, `created_at`, `updated_at`, `last_seen`.
- Tabelle `listing_changes`: protokolliert jede Feldänderung (inkl. Preisänderungen für den Analyse-Feed).
- Die Analyse-Menüs greifen ausschließlich auf diese Tabellen zu. Backup oder externe Auswertungen sind jederzeit möglich (z. B. via `sqlite3 data/reklama5.db`).

## Tests
Die Test-Suite deckt Parsing, Deduplizierung, CLI-Einstieg und SQLite-Hilfsfunktionen ab.

```bash
pytest
```

