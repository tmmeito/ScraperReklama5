# scraper_reklama5_with_km_kw_ps.py

import time
import random
import re
import os
import csv
import json
import warnings
from itertools import islice
from datetime import datetime, timedelta
from collections import defaultdict
from urllib import request as urllib_request
from urllib import error as urllib_error

from bs4 import BeautifulSoup

try:
    from urllib3.exceptions import NotOpenSSLWarning
    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
except Exception:
    pass

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_URL_TEMPLATE = "https://www.reklama5.mk/Search?city=&cat=24&q={search_term}&page={page_num}"
OUTPUT_CSV        = "reklama5_autos_raw.csv"
OUTPUT_AGG        = "reklama5_autos_agg.json"

CSV_FIELDNAMES = [
    "id", "link", "make", "model", "year", "price", "km", "kw", "ps",
    "fuel", "gearbox", "body", "color", "registration", "reg_until",
    "emission_class", "date", "city", "promoted"
]

DETAIL_FIELD_MAP = {
    "марка": "make",
    "модел": "model",
    "година": "year",
    "гориво": "fuel",
    "километри": "km",
    "менувач": "gearbox",
    "каросерија": "body",
    "боја": "color",
    "регистрација": "registration",
    "регистрирана до": "reg_until",
    "сила на моторот": "power_text",
    "класа на емисија": "emission_class",
    "kласа на емисија": "emission_class"
}

MK_MONTHS = {
    "јан":1, "фев":2, "мар":3, "апр":4,
    "мај":5, "јун":6, "јул":7, "авг":8,
    "сеп":9, "окт":10, "ное":11, "дек":12
}

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def init_driver():
    options = webdriver.SafariOptions()
    driver = webdriver.Safari(options=options)
    return driver

def fetch_page(driver, search_term, page_num):
    url = BASE_URL_TEMPLATE.format(search_term=search_term, page_num=page_num)
    print(f"INFO: Fetching page {page_num} → {url}")
    driver.get(url)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.row.ad-top-div"))
        )
    except Exception:
        print(f"WARNING: No listings found within wait time on page {page_num}")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)
    return driver.page_source

def parse_listing(html):
    soup     = BeautifulSoup(html, "html.parser")
    results  = []
    listings = soup.select("div.row.ad-top-div")
    for listing in listings:
        link_elem     = listing.select_one("h3 > a.SearchAdTitle")
        if not link_elem:
            continue
        href          = link_elem.get("href", "")
        m_id          = re.search(r"ad=(\d+)", href)
        ad_id         = m_id.group(1) if m_id else None
        if ad_id:
            full_link = f"https://www.reklama5.mk/AdDetails?ad={ad_id}"
        elif href.startswith("http"):
            full_link = href
        else:
            full_link = f"https://www.reklama5.mk{href}" if href else None

        title_elem    = link_elem
        price_elem    = listing.select_one("span.search-ad-price")
        desc_elem     = listing.select_one("div.ad-desc-div p")
        date_elem     = (
            listing.select_one("div.ad-date-div-1 span") or
            listing.select_one("div.ad-date-div-2 span") or
            listing.select_one("div.ad-date-div-3 span")
        )
        city_elem     = listing.select_one("span.city-span")
        promoted_elem = listing.select_one("div.promotedBtn")

        title       = title_elem.get_text(strip=True)
        price_text  = price_elem.get_text(strip=True) if price_elem else None
        desc_text   = desc_elem.get_text(strip=True) if desc_elem else ""
        date_text   = date_elem.get_text(strip=True) if date_elem else None
        parsed_date = parse_mk_date(date_text) if date_text else None
        if parsed_date:
            date_text = parsed_date.strftime("%Y-%m-%d %H:%M")
        city_text   = city_elem.get_text(strip=True) if city_elem else None
        is_promoted = bool(promoted_elem)

        make, model, year = extract_details(title)
        price = clean_price(price_text) if price_text else None

        spec_text = extract_spec_line(listing)
        spec_year, km, kw, ps = parse_spec_line(spec_text)

        if spec_year is not None:
            year = spec_year
        elif year is None:
            m_year = re.search(r"(\b19|20)\d{2}\b", desc_text)
            if m_year:
                year = int(m_year.group(0))

        if km is None:
            m_km = re.search(r"(\d{1,3}(?:[\.,]\d{3})*|\d+)\s*km", desc_text, re.IGNORECASE)
            if m_km:
                km = int(m_km.group(1).replace(".","").replace(",",""))

        if kw is None:
            m_kw = re.search(r"(\d+)\s*kW", desc_text, re.IGNORECASE)
            if m_kw:
                kw = int(m_kw.group(1))

        if ps is None:
            m_ps = re.search(r"\((\d+)\s*Hp\)", desc_text, re.IGNORECASE)
            if not m_ps:
                m_ps = re.search(r"(\d+)\s*HP", desc_text, re.IGNORECASE)
            if m_ps:
                ps = int(m_ps.group(1))

        results.append({
            "id":       ad_id,
            "link":     full_link,
            "make":     make,
            "model":    model,
            "year":     year,
            "price":    price,
            "km":       km,
            "kw":       kw,
            "ps":       ps,
            "fuel":     None,
            "gearbox":  None,
            "body":     None,
            "color":    None,
            "registration": None,
            "reg_until":    None,
            "emission_class": None,
            "date":     date_text,
            "city":     city_text,
            "promoted": is_promoted
        })
    return results

def extract_details(title):
    parts = title.split()
    make  = parts[0] if parts else None
    model = " ".join(parts[1:]) if len(parts) > 1 else None
    year  = None
    m = re.search(r"\b(19|20)\d{2}\b", title)
    if m:
        try:
            year = int(m.group(0))
        except:
            year = None
    return make, model, year

def extract_spec_line(listing):
    candidate_selectors = [
        "div.search-ad-info p",
        "div.searchAdInfo p",
        "div.ad-info p",
        "div.ad-desc-div p",
        "p"
    ]
    for selector in candidate_selectors:
        for elem in listing.select(selector):
            text = elem.get_text(" ", strip=True)
            if looks_like_spec_line(text):
                return text
    return None

def looks_like_spec_line(text):
    if not text:
        return False
    lowered = text.lower()
    has_year = bool(re.search(r"\b(19|20)\d{2}\b", text))
    has_km   = "km" in lowered or "км" in lowered
    has_kw   = "kw" in lowered or "кв" in lowered
    has_ps   = "hp" in lowered or "кс" in lowered
    return has_year and (has_km or has_kw or has_ps)

def parse_spec_line(text):
    if not text:
        return None, None, None, None
    normalized = re.sub(r"\s+", " ", text)
    year = extract_first_int(normalized, r"\b((?:19|20)\d{2})\b")
    km   = extract_first_int(normalized, r"([\d\.\,\s]+)\s*(?:km|км)")
    kw   = extract_first_int(normalized, r"([\d\.\,\s]+)\s*(?:kW|кW|кв)")
    ps   = extract_first_int(normalized, r"\((\d+)\s*(?:Hp|HP|кс)\)")
    return year, km, kw, ps


def enrich_listings_with_details(listings, enabled, delay_range=None, max_items=None):
    if not enabled or not listings:
        return

    if max_items is not None:
        total_to_process = min(len(listings), max_items)
        iterator = islice(listings, total_to_process)
    else:
        total_to_process = len(listings)
        iterator = iter(listings)

    for idx, listing in enumerate(iterator, start=1):
        link = listing.get("link")
        if not link:
            continue
        details = fetch_detail_attributes(link)
        if not details:
            continue
        for key, value in details.items():
            if value in (None, ""):
                continue
            listing[key] = value
        print(
            "INFO: Detaildaten für Anzeige "
            f"{listing.get('id')} geladen ({idx}/{total_to_process})."
        )
        if delay_range:
            wait_time = random.uniform(*delay_range)
            print(f"INFO: Warte {wait_time:.2f} Sekunden vor nächster Detailseite …")
            time.sleep(wait_time)


def fetch_detail_attributes(url):
    if not url:
        return {}
    headers = {"User-Agent": "Mozilla/5.0 (compatible; reklama5-scraper/1.0)"}
    req = urllib_request.Request(url, headers=headers)
    try:
        with urllib_request.urlopen(req, timeout=15) as response:
            html = response.read()
    except urllib_error.URLError as exc:
        print(f"WARNING: Detailseite {url} konnte nicht geladen werden: {exc}")
        return {}
    soup = BeautifulSoup(html, "html.parser")
    raw = {}
    for label_div in soup.select("div.row.mt-3 div.col-5"):
        label_text = label_div.get_text(strip=True)
        if not label_text:
            continue
        label_clean = label_text.strip().rstrip(":").lower()
        key = DETAIL_FIELD_MAP.get(label_clean)
        if not key:
            continue
        value_div = label_div.find_next_sibling("div", class_="col-7")
        if not value_div:
            continue
        value_text = value_div.get_text(strip=True)
        raw[key] = value_text
    return normalize_detail_values(raw)


def normalize_detail_values(raw):
    result = {}
    for text_key in ("make", "model", "fuel", "gearbox", "body", "color",
                     "registration", "reg_until", "emission_class"):
        if text_key in raw:
            result[text_key] = raw[text_key]
    if "year" in raw:
        result["year"] = parse_int_value(raw["year"])
    if "km" in raw:
        result["km"] = parse_int_value(raw["km"])
    power_text = raw.get("power_text")
    if power_text:
        kw_value, ps_value = parse_power_text(power_text)
        if kw_value is not None:
            result["kw"] = kw_value
        if ps_value is not None:
            result["ps"] = ps_value
    return result


def parse_int_value(text):
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_power_text(text):
    lowered = text.lower()
    kw_match = re.search(r"(\d+)\s*(?:kw|кw|кв)", lowered)
    ps_match = re.search(r"(\d+)\s*(?:ks|кс|hp)", lowered)
    kw_value = int(kw_match.group(1)) if kw_match else None
    ps_value = int(ps_match.group(1)) if ps_match else None
    return kw_value, ps_value

def extract_first_int(text, pattern):
    m = re.search(pattern, text, re.IGNORECASE)
    if not m:
        return None
    digits = re.sub(r"[^0-9]", "", m.group(1))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None

def clean_price(price_text):
    price_text = price_text.replace(".", "").replace(" ","").replace(" ","")
    if re.search(r"(ПоДоговор|дог|nachVereinbarung|1€)", price_text, re.IGNORECASE):
        return None
    m = re.search(r"(\d+)", price_text)
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None

def parse_mk_date(date_text):
    if not date_text:
        return None
    raw = date_text.strip()
    txt = raw.lower()

    # Bereits normalisierte Datumsstrings (z. B. "2024-01-05 13:45") unterstützen.
    for candidate in (raw, raw.replace("T", " ")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass

    if txt.startswith("вчера"):
        parts = txt.split()
        hour, minute = 0, 0
        if len(parts) >= 2 and ":" in parts[1]:
            try:
                hour, minute = map(int, parts[1].split(":"))
            except:
                hour, minute = 0, 0
        dt = datetime.now() - timedelta(days=1)
        dt = dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return dt
    if txt.startswith("денес"):
        parts = txt.split()
        hour, minute = 0, 0
        if len(parts) >= 2 and ":" in parts[1]:
            try:
                hour, minute = map(int, parts[1].split(":"))
            except:
                hour, minute = 0, 0
        dt = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        return dt
    parts = date_text.split()
    if len(parts) < 3:
        return None
    try:
        day       = int(parts[0])
        month_txt = parts[1].lower()
        time_txt  = parts[2]
        hour, minute = map(int, time_txt.split(":"))
        month     = MK_MONTHS.get(month_txt)
        if not month:
            return None
        now = datetime.now()
        year = now.year
        dt = datetime(year, month, day, hour, minute)
        # Anzeigen enthalten kein Jahr. Fällt der Monat/Tag in die Zukunft,
        # stammt das Inserat höchstwahrscheinlich aus dem Vorjahr.
        if dt > now + timedelta(days=1):
            dt = datetime(year - 1, month, day, hour, minute)
        return dt
    except Exception:
        return None

def is_within_days(date_text, days, promoted):
    dt = parse_mk_date(date_text)
    if dt is None:
        return False
    if promoted:
        return False
    return dt >= datetime.now() - timedelta(days=days)

def is_older_than_days(date_text, days, promoted):
    dt = parse_mk_date(date_text)
    if dt is None:
        return False
    if promoted:
        return False
    return dt < datetime.now() - timedelta(days=days)

def save_raw_filtered(rows, days, limit=None):
    file_exists = os.path.isfile(OUTPUT_CSV)
    saved = 0
    with open(OUTPUT_CSV, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            if limit is not None and saved >= limit:
                break
            if r["date"] and is_within_days(r["date"], days, r["promoted"]):
                writer.writerow(r)
                saved += 1
    return saved

def aggregate_data():
    if not os.path.isfile(OUTPUT_CSV):
        print(f"WARN: Datei „{OUTPUT_CSV}“ wurde nicht gefunden. Keine Aggregation möglich.")
        return {}
    agg = defaultdict(lambda: {"count_total":0, "count_with_price":0, "sum_price":0})
    with open(OUTPUT_CSV, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            make  = row["make"]
            model = row["model"]
            price = row["price"]
            key   = f"{make} {model}"
            agg[key]["count_total"] += 1
            if price is not None and str(price).strip() != "":
                try:
                    price_int = int(price)
                    agg[key]["count_with_price"] += 1
                    agg[key]["sum_price"] += price_int
                except:
                    pass
    result = {}
    for key, val in agg.items():
        avg = None
        if val["count_with_price"] > 0:
            avg = val["sum_price"] / val["count_with_price"]
        result[key] = {
            "count_total":      val["count_total"],
            "count_with_price": val["count_with_price"],
            "avg_price":        avg
        }
    with open(OUTPUT_AGG, mode="w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def load_rows_from_csv():
    if not os.path.isfile(OUTPUT_CSV):
        print(f"WARN: Datei „{OUTPUT_CSV}“ wurde nicht gefunden.")
        return []
    rows = []
    with open(OUTPUT_CSV, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = dict(row)
            for field in ("price", "year", "km", "kw", "ps"):
                value = parsed.get(field)
                if value in (None, ""):
                    parsed[field] = None
                    continue
                try:
                    parsed[field] = int(value)
                except ValueError:
                    parsed[field] = None
            rows.append(parsed)
    return rows


def display_make_model_summary(rows, min_price_for_avg=500, top_n=15):
    if not rows:
        print("Keine CSV-Daten vorhanden. Bitte zuerst Daten sammeln.")
        return

    grouped = defaultdict(lambda: {"count_total": 0, "count_for_avg": 0, "sum": 0})
    excluded_low_price = 0

    for row in rows:
        make = row.get("make") or "Unbekannt"
        model = row.get("model") or "Unbekannt"
        price = row.get("price")

        key = (make, model)
        grouped[key]["count_total"] += 1

        if price is None:
            continue
        if price < min_price_for_avg:
            excluded_low_price += 1
            continue

        grouped[key]["count_for_avg"] += 1
        grouped[key]["sum"] += price

    if not grouped:
        print("Keine Daten für die Auswertung verfügbar.")
        return

    print("\nTop Automarken/-Modelle nach Anzahl der Inserate:")
    print(f"{'Marke/Modell':40} {'Anzahl':>8} {'Ø-Preis':>12}")
    sorted_items = sorted(
        grouped.items(),
        key=lambda item: item[1]["count_total"],
        reverse=True,
    )
    for (make, model), stats in sorted_items[:top_n]:
        avg = (
            stats["sum"] / stats["count_for_avg"]
            if stats["count_for_avg"]
            else None
        )
        avg_txt = f"{avg:,.0f}".replace(",", " ") if avg is not None else "-"
        combined_label = f"{make} {model}".strip()
        print(f"{combined_label:40} {stats['count_total']:>8} {avg_txt:>12}")

    if excluded_low_price:
        print(
            f"\nHinweis: {excluded_low_price} Anzeigen unter {min_price_for_avg} "
            "wurden für die Durchschnittspreise nicht berücksichtigt."
        )


def display_avg_price_by_model_year(rows, min_listings=1, min_price_for_avg=500):
    if not rows:
        print("Keine CSV-Daten vorhanden. Bitte zuerst Daten sammeln.")
        return
    groups = defaultdict(lambda: {"count": 0, "sum": 0})
    excluded_low_price = 0
    for row in rows:
        price = row.get("price")
        year = row.get("year")
        if price is None or year is None:
            continue
        if price < min_price_for_avg:
            excluded_low_price += 1
            continue
        key = (row.get("make") or "Unbekannt", row.get("model") or "Unbekannt", year)
        groups[key]["count"] += 1
        groups[key]["sum"] += price
    if not groups:
        print("Nicht genügend Daten mit Preis und Baujahr vorhanden.")
        return
    print("\nDurchschnittspreise nach Modell und Baujahr:")
    print(f"{'Marke':15} {'Modell':25} {'Baujahr':>8} {'Anzahl':>8} {'Ø-Preis':>12}")
    sorted_groups = sorted(
        groups.items(),
        key=lambda item: (-item[1]["count"], item[0][2], item[0][0], item[0][1]),
    )
    for (make, model, year), stats in sorted_groups:
        if stats["count"] < min_listings:
            continue
        avg_price = stats["sum"] / stats["count"]
        avg_txt = f"{avg_price:,.0f}".replace(",", " ")
        print(
            f"{make:15} {model[:25]:25} {year:>8} "
            f"{stats['count']:>8} {avg_txt:>12}"
        )
    if excluded_low_price:
        print(
            f"\nHinweis: {excluded_low_price} Anzeigen unter {min_price_for_avg} "
            "wurden für die Durchschnittspreise nicht berücksichtigt."
        )


def analysis_menu(agg_data):
    if not os.path.isfile(OUTPUT_CSV):
        print("\nKeine Daten für Analysen vorhanden.")
        return
    csv_rows = None
    min_price_input = input(
        "Mindestpreis für Durchschnittsberechnung (Enter = 500): "
    ).strip()
    if not min_price_input:
        min_price_for_avg = 500
    else:
        try:
            min_price_for_avg = int(min_price_input)
            if min_price_for_avg < 0:
                raise ValueError
        except ValueError:
            print("Ungültiger Mindestpreis. Verwende Standardwert 500.")
            min_price_for_avg = 500
    while True:
        print("\nAnalysemenü:")
        print("  1) Häufigste Automarken und Modelle (CSV/JSON)")
        print("  2) Durchschnittspreise pro Modell und Baujahr")
        print("  3) Analyse verlassen")
        choice = input("Bitte Auswahl eingeben: ").strip()
        if choice == "1":
            if csv_rows is None:
                csv_rows = load_rows_from_csv()
            display_make_model_summary(csv_rows, min_price_for_avg=min_price_for_avg)
        elif choice == "2":
            if csv_rows is None:
                csv_rows = load_rows_from_csv()
            display_avg_price_by_model_year(
                csv_rows,
                min_listings=1,
                min_price_for_avg=min_price_for_avg,
            )
        elif choice == "3":
            break
        else:
            print("Ungültige Auswahl. Bitte erneut versuchen.")

def main():
    clear_screen()
    print("====================================")
    print("  SCRAPER FÜR reklama5.mk AUTOMOBILE ")
    print("====================================\n")

    print("Wähle die Seite zum Auslesen:")
    print("  1) reklama5")
    choice = input("Deine Wahl (Enter = 1): ").strip() or "1"
    if choice != "1":
        print("Nur ‚reklama5‘ aktuell unterstützt. Programm beendet.")
        return

    search_term = input("Suchbegriff (z. B. „aygo“) eingeben (oder Enter für alle): ").strip()
    search_term = search_term if search_term else ""

    days_input = input(
        "Wie viele Tage zurück sollen berücksichtigt werden? (Enter = 1 Tag): "
    ).strip()
    if not days_input:
        days = 1
    else:
        try:
            days = int(days_input)
            if days <= 0:
                raise ValueError
        except ValueError:
            print("Ungültige Eingabe von Tagen. Programm beendet.")
            return

    limit_input = input("Wieviele Einträge sollen maximal eingelesen werden? (Enter = alle): ").strip()
    if limit_input:
        try:
            limit = int(limit_input)
            if limit <= 0:
                raise ValueError
        except ValueError:
            print("Ungültige Eingabe für Eintrags-Limit. Programm beendet.")
            return
    else:
        limit = None

    detail_input = input("Genaue Erfassung aktivieren? (j/N – Enter = nein): ").strip().lower()
    enable_detail_capture = detail_input in {"j", "ja", "y", "yes"}
    detail_delay_range = None
    if enable_detail_capture:
        print("INFO: Genaue Erfassung aktiv. Jede Anzeige wird einzeln geöffnet, um Detaildaten zu übernehmen.")
        random_delay_input = input(
            "Zufällige Pause (ca. 1–2 Sekunden) zwischen Detailseiten einfügen? (Enter = ja, n = feste Pause): "
        ).strip().lower()
        if random_delay_input in {"", "j", "ja", "y", "yes"}:
            detail_delay_range = (1.0, 2.0)
            print("INFO: Verwende zufällige Pause von ca. 1–2 Sekunden.")
        else:
            fixed_delay_input = input(
                "Feste Pause zwischen Detailseiten in Sekunden (Enter oder 0 = keine): "
            ).strip()
            if not fixed_delay_input or fixed_delay_input == "0":
                detail_delay_range = None
                print("INFO: Keine zusätzliche Pause zwischen den Detailseiten.")
            else:
                try:
                    value = float(fixed_delay_input.replace(",", "."))
                    if value < 0:
                        raise ValueError
                    detail_delay_range = (value, value)
                    print(f"INFO: Verwende feste Pause von {value:.2f} Sekunden.")
                except ValueError:
                    detail_delay_range = (1.0, 2.0)
                    print("WARN: Ungültige Eingabe – verwende zufällige Pause von 1–2 Sekunden.")

    driver = init_driver()
    if os.path.isfile(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)

    total_found   = 0
    total_saved   = 0

    try:
        for page in range(1, 200):
            html     = fetch_page(driver, search_term, page)
            listings = parse_listing(html)
            found_on_page = len(listings)
            total_found  += found_on_page

            if not listings:
                print(f"INFO: Keine Listings auf Seite {page} → Stop.")
                break

            eligible_listings = [
                item for item in listings
                if item["date"] and is_within_days(item["date"], days, item["promoted"])
            ]

            remaining_limit = None
            if limit is not None:
                remaining_limit = max(0, limit - total_saved)
                if remaining_limit == 0:
                    print(f"INFO: Maximalanzahl von {limit} Einträgen bereits erreicht. Stop.")
                    break
                eligible_listings = eligible_listings[:remaining_limit]

            enrich_listings_with_details(
                eligible_listings,
                enable_detail_capture,
                delay_range=detail_delay_range,
                max_items=remaining_limit if limit is not None else None,
            )

            saved_in_page = save_raw_filtered(eligible_listings, days, limit=remaining_limit)
            total_saved   += saved_in_page
            print(
                f"INFO: Seite {page}: {found_on_page} Einträge gefunden, "
                f"{saved_in_page} nach Filter übernommen."
            )

            if limit is not None and total_saved >= limit:
                print(f"INFO: Maximalanzahl von {limit} Einträgen erreicht (aktuell {total_saved}). Stop.")
                break

            stop = False
            for item in listings:
                if item["date"] and is_older_than_days(item["date"], days, item["promoted"]):
                    print(f"INFO: Anzeige älter als {days} Tage gefunden (id={item['id']}) auf Seite {page}. Stop.")
                    stop = True
                    break
            if stop:
                break

            sleep_time = random.uniform(2,4)
            print(f"INFO: Sleeping {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
    finally:
        driver.quit()

    print(
        "\nINFO: Gesamtsumme: "
        f"{total_found} Einträge geprüft, {total_saved} davon gespeichert."
    )

    agg_data = aggregate_data()
    analysis_menu(agg_data)

if __name__ == "__main__":
    main()
