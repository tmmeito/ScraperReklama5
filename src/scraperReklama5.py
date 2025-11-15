# scraper_reklama5_with_km_kw_ps.py

import argparse
import sys
import time
import random
import re
import os
import csv
import json
import warnings
import socket
import threading
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Tuple, Union
from urllib import request as urllib_request
from urllib import error as urllib_error
from urllib.parse import urlsplit, urlunsplit, quote_plus

from storage import sqlite_store

from bs4 import BeautifulSoup

try:
    from urllib3.exceptions import NotOpenSSLWarning
    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
except Exception:
    pass

BASE_URL_TEMPLATE = "https://www.reklama5.mk/Search?city=&cat=24&q={search_term}&page={page_num}"
OUTPUT_CSV        = "reklama5_autos_raw.csv"
OUTPUT_AGG        = "reklama5_autos_agg.json"

CSV_FIELDNAMES = [
    "id", "link", "make", "model", "year", "price", "km", "kw", "ps",
    "fuel", "gearbox", "body", "color", "registration", "reg_until",
    "emission_class", "date", "city", "promoted"
]

INLINE_PROGRESS_SYMBOL = "•"

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


DETAIL_DELAY_UNSET = object()


@dataclass
class ScraperConfig:
    search_term: str = ""
    days: int = 1
    limit: Optional[int] = None
    enable_detail_capture: bool = False
    detail_delay_range: Union[None, Tuple[float, float], object] = DETAIL_DELAY_UNSET
    detail_worker_count: int = 1
    detail_rate_limit_permits: Optional[int] = None
    csv_filename: str = OUTPUT_CSV
    base_url_template: Optional[str] = None
    db_path: Optional[str] = None

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def print_banner(title):
    line = "═" * 70
    print(f"\n{line}\n  {title}\n{line}\n")


def print_section(title):
    line = "─" * 70
    print(f"\n{line}\n{title}\n{line}")


def build_inline_progress_printer(total, symbol=INLINE_PROGRESS_SYMBOL):
    if total <= 0:
        return None, None

    state = {"count": 0, "done": False}

    def callback():
        if state["done"]:
            return
        print(symbol, end="", flush=True)
        state["count"] += 1
        if state["count"] >= total:
            print()
            state["done"] = True

    def finalize():
        if not state["done"]:
            print()
            state["done"] = True

    return callback, finalize


def shorten_url(url, max_length=48):
    if not url:
        return ""
    if len(url) <= max_length:
        return url
    return url[: max_length - 3] + "..."


def prompt_db_path(default_path=sqlite_store.DEFAULT_DB_PATH):
    prompt = (
        "SQLite-Datenbank zum Speichern verwenden "
        f"(Enter = {default_path}, q = Abbruch): "
    )
    user_input = input(prompt).strip()
    if not user_input:
        return default_path
    if user_input.lower() in {"q", "quit"}:
        return None
    return user_input


def prompt_existing_db_path(default_path=sqlite_store.DEFAULT_DB_PATH):
    while True:
        prompt = (
            "SQLite-Datenbank für Analysen wählen "
            f"(Enter = {default_path}, q = Abbruch): "
        )
        user_input = input(prompt).strip()
        if not user_input:
            candidate = default_path
        elif user_input.lower() in {"q", "quit"}:
            return None
        else:
            candidate = user_input
        if candidate and os.path.isfile(candidate):
            return candidate
        print(
            f"⚠️  Datei „{candidate}“ wurde nicht gefunden. Bitte erneut versuchen."
        )


def _split_query_pairs(raw_query):
    pairs = []
    if not raw_query:
        return pairs
    for part in raw_query.split("&"):
        if not part:
            continue
        if "=" in part:
            key, value = part.split("=", 1)
        else:
            key, value = part, ""
        pairs.append([key, value])
    return pairs


def _ensure_placeholder(pairs, target_key, placeholder_value):
    target_lower = target_key.lower()
    for pair in pairs:
        if pair[0].lower() == target_lower:
            pair[1] = placeholder_value
            return
    pairs.append([target_key, placeholder_value])


def _rebuild_query_string(pairs):
    if not pairs:
        return ""
    parts = []
    for key, value in pairs:
        if value == "":
            parts.append(f"{key}=")
        else:
            parts.append(f"{key}={value}")
    return "&".join(parts)


def build_base_url_template(raw_input):
    trimmed = raw_input.strip()
    if not trimmed:
        return None
    if "{search_term}" in trimmed and "{page_num}" in trimmed:
        return trimmed
    parsed = urlsplit(trimmed)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("Ungültige URL – bitte vollständige Adresse angeben.")
    query_pairs = _split_query_pairs(parsed.query)
    _ensure_placeholder(query_pairs, "q", "{search_term}")
    _ensure_placeholder(query_pairs, "page", "{page_num}")
    new_query = _rebuild_query_string(query_pairs)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))

def fetch_listing_page(search_term, page_num, retries=3, backoff_seconds=2):
    encoded_term = quote_plus(search_term or "")
    url = BASE_URL_TEMPLATE.format(search_term=encoded_term, page_num=page_num)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; reklama5-scraper/1.0)"}
    req = urllib_request.Request(url, headers=headers)

    for attempt in range(1, retries + 1):
        try:
            with urllib_request.urlopen(req, timeout=20) as response:
                html_bytes = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
            return html_bytes.decode(charset, errors="replace")
        except (urllib_error.URLError, socket.timeout) as exc:
            print(
                "⚠️  Ergebnisseite konnte nicht geladen werden | "
                f"{shorten_url(url)} (Versuch {attempt}/{retries}: {exc})"
            )
            if attempt >= retries:
                return None
            time.sleep(backoff_seconds * attempt)

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


def _detail_worker(link, delay_range=None, rate_limit_semaphore=None):
    if not link:
        if delay_range:
            time.sleep(random.uniform(*delay_range))
        return {}

    if rate_limit_semaphore is not None:
        rate_limit_semaphore.acquire()
    try:
        details = fetch_detail_attributes(link) or {}
        if delay_range:
            time.sleep(random.uniform(*delay_range))
        return details
    finally:
        if rate_limit_semaphore is not None:
            rate_limit_semaphore.release()


def enrich_listings_with_details(
    listings,
    enabled,
    delay_range=None,
    max_items=None,
    progress_callback=None,
    max_workers=3,
    rate_limit_permits=None,
):
    if not enabled or not listings:
        return

    if not isinstance(listings, list):
        listings = list(listings)

    total_available = len(listings)
    if total_available == 0:
        return

    if max_items is not None:
        total_to_process = min(total_available, max_items)
    else:
        total_to_process = total_available

    if total_to_process <= 0:
        return

    iterator = islice(iter(listings), total_to_process)
    target_listings = list(iterator)

    worker_count = max(1, int(max_workers or 1))
    rate_limit_semaphore = None
    if rate_limit_permits is not None:
        permits = max(1, min(int(rate_limit_permits), worker_count))
        rate_limit_semaphore = threading.Semaphore(permits)

    futures = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for idx, listing in enumerate(target_listings):
            link = listing.get("link")
            future = executor.submit(
                _detail_worker,
                link,
                delay_range=delay_range,
                rate_limit_semaphore=rate_limit_semaphore,
            )
            futures[future] = idx

        results = {}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result() or {}
            except Exception:
                results[idx] = {}
            if progress_callback:
                progress_callback()

    for idx in range(len(target_listings)):
        details = results.get(idx)
        if not details:
            continue
        listing = target_listings[idx]
        for key, value in details.items():
            if value in (None, ""):
                continue
            listing[key] = value


def fetch_detail_attributes(url, retries=3, backoff_seconds=2):
    if not url:
        return {}

    headers = {"User-Agent": "Mozilla/5.0 (compatible; reklama5-scraper/1.0)"}
    req = urllib_request.Request(url, headers=headers)

    for attempt in range(1, retries + 1):
        try:
            with urllib_request.urlopen(req, timeout=15) as response:
                html = response.read()
            break
        except (urllib_error.URLError, socket.timeout) as exc:
            print(
                "⚠️  Detailseite konnte nicht geladen werden | "
                f"{shorten_url(url)} (Versuch {attempt}/{retries}: {exc})"
            )
            if attempt >= retries:
                return {}
            time.sleep(backoff_seconds * attempt)
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
    if not price_text:
        return None

    text = price_text.replace("\xa0", " ").strip()
    if re.search(r"(ПоДоговор|дог|nachVereinbarung|1€)", text, re.IGNORECASE):
        return None

    match = re.search(r"-?[\d\s\.,]+", text)
    if not match:
        return None

    number_text = match.group(0).strip()
    is_negative = number_text.startswith("-")
    number_text = number_text.lstrip("- ")
    number_text = number_text.replace(" ", "")

    # Detect decimal separators (comma or period) at the end with one or two digits.
    last_decimal_sep = max(number_text.rfind(","), number_text.rfind("."))
    if last_decimal_sep != -1:
        fractional = number_text[last_decimal_sep + 1 :]
        if fractional.isdigit() and 1 <= len(fractional) <= 2:
            number_text = number_text[:last_decimal_sep]

    integer_text = number_text.replace(",", "").replace(".", "")
    if not integer_text.isdigit():
        return None

    value = int(integer_text)
    return -value if is_negative else value

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

def save_raw_filtered(
    rows,
    days,
    limit=None,
    csv_filename=OUTPUT_CSV,
    pre_filtered=False,
    *,
    db_connection=None,
):
    saved_rows = []
    for r in rows:
        if limit is not None and len(saved_rows) >= limit:
            break
        if pre_filtered:
            saved_rows.append(r)
            continue
        if r["date"] and is_within_days(r["date"], days, r["promoted"]):
            saved_rows.append(r)

    if not saved_rows:
        return 0

    if db_connection is not None:
        sqlite_store.upsert_many(db_connection, saved_rows, CSV_FIELDNAMES)
        return len(saved_rows)

    target_csv = csv_filename or OUTPUT_CSV
    file_exists = os.path.isfile(target_csv)
    with open(target_csv, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(saved_rows)
    return len(saved_rows)

def aggregate_data(
    csv_filename=None,
    output_json=None,
    *,
    db_path=None,
    db_connection=None,
    search_term=None,
    days=None,
):
    if not output_json:
        output_json = OUTPUT_AGG

    result = {}
    if db_connection is not None or db_path:
        close_after = False
        conn = db_connection
        if conn is None and db_path:
            conn = sqlite_store.open_database(db_path)
            close_after = True
        try:
            stats = sqlite_store.fetch_make_model_stats(
                conn,
                min_price=0,
                days=days,
                search=search_term,
            )
            agg = defaultdict(lambda: {"count_total": 0, "count_with_price": 0, "sum": 0})
            for (make, model, _fuel), values in stats.items():
                key = f"{make} {model}".strip()
                bucket = agg[key]
                bucket["count_total"] += values["count_total"]
                bucket["count_with_price"] += values["count_for_avg"]
                bucket["sum"] += values["sum"]
            for key, val in agg.items():
                avg = None
                if val["count_with_price"] > 0:
                    avg = val["sum"] / val["count_with_price"]
                result[key] = {
                    "count_total": val["count_total"],
                    "count_with_price": val["count_with_price"],
                    "avg_price": avg,
                }
        finally:
            if close_after and conn is not None:
                conn.close()
    else:
        if not csv_filename:
            csv_filename = OUTPUT_CSV
        if not os.path.isfile(csv_filename):
            print(
                f"⚠️  Datei „{csv_filename}“ wurde nicht gefunden. Keine Aggregation möglich."
            )
            return {}
        agg = defaultdict(
            lambda: {"count_total": 0, "count_with_price": 0, "sum_price": 0}
        )
        with open(csv_filename, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                make = row["make"]
                model = row["model"]
                price_txt = row.get("price")
                price = None
                if price_txt not in (None, ""):
                    try:
                        price = int(str(price_txt).strip())
                    except ValueError:
                        price = None
                key = f"{make} {model}"
                agg[key]["count_total"] += 1
                if price is not None:
                    agg[key]["count_with_price"] += 1
                    agg[key]["sum_price"] += price
        for key, val in agg.items():
            avg = None
            if val["count_with_price"] > 0:
                avg = val["sum_price"] / val["count_with_price"]
            result[key] = {
                "count_total": val["count_total"],
                "count_with_price": val["count_with_price"],
                "avg_price": avg,
            }

    with open(output_json, mode="w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def _resolve_grouped_stats(source, accessor_name, min_price):
    if source is None:
        return None
    if isinstance(source, dict):
        return source
    accessor = getattr(source, accessor_name, None)
    if callable(accessor):
        return accessor(min_price)
    return None


def display_make_model_summary(analysis_data, min_price_for_avg=500, top_n=15):
    grouped = _resolve_grouped_stats(analysis_data, "make_model_stats", min_price_for_avg)
    if not grouped:
        print("Keine Daten für die Auswertung verfügbar.")
        return

    print("\nTop Automarken/-Modelle nach Anzahl der Inserate:")
    print(f"{'Marke':15} {'Modell':25} {'Treibstoff':15} {'Anzahl':>12} {'Ø-Preis':>12}")
    sorted_items = sorted(
        grouped.items(),
        key=lambda item: item[1]["count_total"],
        reverse=True,
    )
    top_items = sorted(
        sorted_items[:top_n],
        key=lambda item: (
            item[0][0] or "",
            item[0][1] or "",
            item[0][2] or "",
        ),
    )
    for (make, model, fuel), stats in top_items:
        avg = (
            stats["sum"] / stats["count_for_avg"]
            if stats["count_for_avg"]
            else None
        )
        avg_txt = f"{avg:,.0f}".replace(",", " ") if avg is not None else "-"
        excluded = stats.get("excluded_low_price", 0)
        excluded_txt = "-" if not excluded else str(excluded)
        count_txt = f"{stats['count_total']} ({excluded_txt})"
        print(
            f"{make:15} {model[:25]:25} {fuel[:15]:15} "
            f"{count_txt:>12} {avg_txt:>12}"
        )


def _format_price_value(value):
    if value is None:
        return "-"
    try:
        return f"{int(value):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(value)


def display_recent_price_changes(conn, limit=5):
    if conn is None:
        return
    changes = sqlite_store.fetch_recent_price_changes(conn, limit=limit)
    if not changes:
        return
    print("\nLetzte Preisänderungen:")
    for change in changes:
        old_txt = _format_price_value(change["old_price"])
        new_txt = _format_price_value(change["new_price"])
        print(
            f"  • [{change['changed_at']}] {change['listing_id']}: "
            f"{old_txt} → {new_txt}"
        )


DEFAULT_MIN_PRICE_FOR_AVG = 500


def display_avg_price_by_model_year(
    analysis_data, min_listings=1, min_price_for_avg=DEFAULT_MIN_PRICE_FOR_AVG
):
    groups = _resolve_grouped_stats(analysis_data, "model_year_stats", min_price_for_avg)
    if not groups:
        print("Nicht genügend Daten mit Preis und Baujahr vorhanden.")
        return
    print("\nDurchschnittspreise nach Modell und Baujahr:")
    print(
        f"{'Marke':15} {'Modell':25} {'Baujahr':>8} "
        f"{'Treibstoff':15} {'Anzahl':>12} {'Ø-Preis':>12}"
    )
    sorted_groups = sorted(
        groups.items(),
        key=lambda item: (
            item[0][0] or "",
            item[0][1] or "",
            item[0][3],
            item[0][2] or "",
        ),
    )
    for (make, model, fuel, year), stats in sorted_groups:
        if stats["count_for_avg"] < min_listings:
            continue
        avg_price = stats["sum"] / stats["count_for_avg"]
        avg_txt = f"{avg_price:,.0f}".replace(",", " ")
        excluded = stats.get("excluded_low_price", 0)
        excluded_txt = "-" if not excluded else str(excluded)
        count_total = stats["count_total"]
        count_txt = f"{count_total} ({excluded_txt})"
        print(
            f"{make:15} {model[:25]:25} {year:>8} {fuel[:15]:15} "
            f"{count_txt:>12} {avg_txt:>12}"
        )


def prompt_min_price(current_value=DEFAULT_MIN_PRICE_FOR_AVG):
    if current_value is None or current_value < 0:
        current_value = DEFAULT_MIN_PRICE_FOR_AVG
    while True:
        user_input = input(
            f"Mindestpreis für Durchschnittsberechnung (Enter = {current_value}): "
        ).strip()
        if not user_input:
            return current_value
        try:
            value = int(user_input)
            if value < 0:
                raise ValueError
            return value
        except ValueError:
            print("Ungültiger Mindestpreis. Bitte erneut versuchen.")


def _prompt_days_filter(current_value=None):
    label = current_value if current_value is not None else "alle"
    while True:
        user_input = input(
            f"Filter: Wieviele Tage zurück betrachten? (Enter = {label}, 0 = alle): "
        ).strip()
        if not user_input:
            return current_value
        try:
            value = int(user_input)
            if value <= 0:
                return None
            return value
        except ValueError:
            print("⚠️  Bitte eine ganze Zahl eingeben.")


def _prompt_search_filter(current_value=None):
    label = current_value if current_value else "-"
    user_input = input(
        f"Filter: Freitext-Suche (Enter = {label}, Leer = kein Filter): "
    ).strip()
    return user_input or None


def _analysis_settings_menu(
    *,
    min_price_for_avg,
    db_days_filter,
    db_search_filter,
):
    while True:
        label_days = db_days_filter if db_days_filter is not None else "alle"
        label_search = db_search_filter if db_search_filter else "-"
        print_section("⚙️  Analyse-Einstellungen")
        print(f"   • Mindestpreis..: {min_price_for_avg} €")
        print(f"   • Filter Tage...: {label_days}")
        print(f"   • Filter Suche..: {label_search}")
        print()
        print("  [1] 💶 Mindestpreis setzen")
        print("  [2] ⏱️  Tagesfilter ändern")
        print("  [3] 🔎 Suchfilter ändern")
        print()
        print("  [0] ↩️  Zurück zum Analyse-Menü")
        print()
        choice = input("Deine Auswahl: ").strip()
        if choice == "1":
            min_price_for_avg = prompt_min_price(min_price_for_avg)
        elif choice == "2":
            db_days_filter = _prompt_days_filter(db_days_filter)
        elif choice == "3":
            db_search_filter = _prompt_search_filter(db_search_filter)
        elif choice == "0":
            return min_price_for_avg, db_days_filter, db_search_filter
        else:
            print("⚠️  Ungültige Auswahl. Bitte erneut versuchen.")


def analysis_menu(*, db_path):
    if not db_path:
        print("⚠️  Für Analysen muss eine SQLite-Datenbank angegeben werden.")
        return "exit"
    if not os.path.isfile(db_path):
        print(f"\n⚠️  Datenbank „{db_path}“ nicht gefunden.")
        return "exit"

    try:
        conn = sqlite_store.open_database(db_path)
    except Exception as exc:
        print(f"⚠️  Konnte Datenbank nicht öffnen: {exc}")
        return "exit"

    min_price_for_avg = DEFAULT_MIN_PRICE_FOR_AVG
    db_days_filter = None
    db_search_filter = None
    try:
        while True:
            print_section("📊 Analyse-Center")
            print(f"   • Quelle........: SQLite ({db_path})")
            label_days = db_days_filter if db_days_filter is not None else "alle"
            label_search = db_search_filter if db_search_filter else "-"
            print(f"   • Filter Tage...: {label_days}")
            print(f"   • Filter Suche..: {label_search}")
            print(f"   • Mindestpreis..: {min_price_for_avg} €")
            print("\n  [1] 📈 Häufigste Automarken und Modelle")
            print("  [2] 💶 Durchschnittspreise pro Modell/Baujahr")
            print("  [3] ⚙️  Einstellungen")
            print()
            print("  [0] 🔁 Zurück zum Hauptmenü")
            print()
            choice = input("Deine Auswahl: ").strip()
            if choice == "1":
                clear_screen()
                stats = sqlite_store.fetch_make_model_stats(
                    conn,
                    min_price=min_price_for_avg,
                    days=db_days_filter,
                    search=db_search_filter,
                )
                display_make_model_summary(
                    stats, min_price_for_avg=min_price_for_avg
                )
                display_recent_price_changes(conn)
            elif choice == "2":
                clear_screen()
                stats = sqlite_store.fetch_model_year_stats(
                    conn,
                    min_price=min_price_for_avg,
                    days=db_days_filter,
                    search=db_search_filter,
                )
                display_avg_price_by_model_year(
                    stats,
                    min_listings=1,
                    min_price_for_avg=min_price_for_avg,
                )
                display_recent_price_changes(conn)
            elif choice == "3":
                clear_screen()
                (
                    min_price_for_avg,
                    db_days_filter,
                    db_search_filter,
                ) = _analysis_settings_menu(
                    min_price_for_avg=min_price_for_avg,
                    db_days_filter=db_days_filter,
                    db_search_filter=db_search_filter,
                )
            elif choice == "0":
                return "main"
            else:
                print("⚠️  Ungültige Auswahl. Bitte erneut versuchen.")
    finally:
        conn.close()


def run_scraper_flow_from_config(config, *, interactive=True):
    if not isinstance(config, ScraperConfig):
        config = ScraperConfig(**config)

    global BASE_URL_TEMPLATE
    if config.base_url_template:
        BASE_URL_TEMPLATE = config.base_url_template

    db_path = (config.db_path or "").strip() or None
    db_connection = None
    csv_filename = None
    if db_path:
        try:
            db_connection = sqlite_store.open_database(db_path)
            sqlite_store.init_schema(db_connection, CSV_FIELDNAMES)
        except Exception as exc:
            print("⚠️  SQLite konnte nicht initialisiert werden. Nutze CSV-Datei.")
            print(f"    Grund: {exc}")
            db_connection = None
            db_path = None
    if not db_path:
        csv_filename = config.csv_filename or OUTPUT_CSV
        if os.path.isfile(csv_filename):
            os.remove(csv_filename)
    search_term = config.search_term or ""
    try:
        days_value = int(config.days)
    except (TypeError, ValueError):
        days_value = 1
    days = days_value if days_value > 0 else 1

    limit = None
    if config.limit is not None:
        try:
            limit_candidate = int(config.limit)
        except (TypeError, ValueError):
            limit_candidate = None
        else:
            if limit_candidate > 0:
                limit = limit_candidate
    enable_detail_capture = bool(config.enable_detail_capture)

    detail_worker_count = config.detail_worker_count or 1
    detail_worker_count = max(1, min(5, int(detail_worker_count)))

    detail_rate_limit_permits = config.detail_rate_limit_permits
    if detail_rate_limit_permits is not None:
        detail_rate_limit_permits = int(detail_rate_limit_permits)
        if detail_rate_limit_permits <= 0:
            detail_rate_limit_permits = None
        else:
            detail_rate_limit_permits = min(detail_rate_limit_permits, detail_worker_count)

    detail_delay_range = config.detail_delay_range
    if enable_detail_capture:
        if detail_delay_range is DETAIL_DELAY_UNSET:
            detail_delay_range = (1.0, 2.0)
        elif detail_delay_range is not None and detail_delay_range[0] > detail_delay_range[1]:
            detail_delay_range = (detail_delay_range[1], detail_delay_range[0])
    else:
        detail_delay_range = None
        detail_worker_count = 1
        detail_rate_limit_permits = None

    if csv_filename and os.path.isfile(csv_filename):
        os.remove(csv_filename)

    total_found = 0
    total_saved = 0
    seen_ids = set()
    duplicates_skipped_total = 0

    try:
        for page in range(1, 200):
            html = fetch_listing_page(search_term, page)
            if not html:
                print()
                print(f"⚠️  Seite {page} konnte nicht geladen werden. Stop.")
                break
            listings = parse_listing(html)

            if not listings:
                print()
                print(f"ℹ️  Keine Listings auf Seite {page} → Stop.")
                break

            eligible_listings = [
                item for item in listings
                if item["date"] and is_within_days(item["date"], days, item["promoted"])
            ]

            duplicates_skipped_page = 0
            deduplicated = []
            for item in eligible_listings:
                item_id = item.get("id")
                if item_id and item_id in seen_ids:
                    duplicates_skipped_page += 1
                    continue
                if item_id:
                    seen_ids.add(item_id)
                deduplicated.append(item)
            eligible_listings = deduplicated
            duplicates_skipped_total += duplicates_skipped_page

            remaining_limit = None
            if limit is not None:
                remaining_limit = max(0, limit - total_saved)
                if remaining_limit == 0:
                    print(
                        f"ℹ️  Maximalanzahl von {limit} Einträgen bereits erreicht. Stop."
                    )
                    break
                eligible_listings = eligible_listings[:remaining_limit]

            found_on_page = len(eligible_listings)
            total_found += found_on_page

            print(f"Lade Seite {page:02d} ({found_on_page:02d} Treffer)")
            if duplicates_skipped_page:
                print(f"↺ {duplicates_skipped_page} Duplikate übersprungen")
            print(INLINE_PROGRESS_SYMBOL * found_on_page)

            progress_callback = None
            progress_finalize = None
            if enable_detail_capture and found_on_page:
                (
                    progress_callback,
                    progress_finalize,
                ) = build_inline_progress_printer(found_on_page)

            enrich_listings_with_details(
                eligible_listings,
                enable_detail_capture,
                delay_range=detail_delay_range,
                max_items=remaining_limit if limit is not None else None,
                progress_callback=progress_callback,
                max_workers=detail_worker_count,
                rate_limit_permits=detail_rate_limit_permits,
            )

            if progress_finalize:
                progress_finalize()

            save_kwargs = {
                "limit": remaining_limit,
                "csv_filename": csv_filename,
            }
            save_kwargs["pre_filtered"] = True
            if db_connection is not None:
                save_kwargs["db_connection"] = db_connection
            saved_in_page = save_raw_filtered(
                eligible_listings,
                days,
                **save_kwargs,
            )
            total_saved += saved_in_page
            print(f"{saved_in_page:02d} von {found_on_page:02d} gespeichert")
            print()

            if limit is not None and total_saved >= limit:
                print(
                    "ℹ️  Maximalanzahl von "
                    f"{limit} Einträgen erreicht (aktuell {total_saved}). Stop."
                )
                break

            stop = False
            for item in listings:
                if item["date"] and is_older_than_days(item["date"], days, item["promoted"]):
                    print(
                        f"ℹ️  Anzeige älter als {days} Tage gefunden "
                        f"(ID {item['id']}) auf Seite {page}. Stop."
                    )
                    stop = True
                    break
            if stop:
                break

            sleep_time = random.uniform(2, 4)
            time.sleep(sleep_time)
    finally:
        if db_connection is not None:
            db_connection.close()

    print_section("📦 Zusammenfassung")
    print(
        f"   • Geprüfte Einträge : {total_found}\n"
        f"   • Gespeicherte Einträge: {total_saved}\n"
        f"   • Übersprungene Duplikate: {duplicates_skipped_total}\n"
    )

    if csv_filename:
        aggregate_data(csv_filename=csv_filename)
    elif db_path:
        aggregate_data(
            db_path=db_path,
            search_term=config.search_term,
            days=days,
        )
    if interactive:
        if db_path:
            return analysis_menu(db_path=db_path)
        print("ℹ️  Keine SQLite-Datenbank verfügbar – Analyse übersprungen.")
        return "main"
    return {
        "total_found": total_found,
        "total_saved": total_saved,
        "csv_filename": csv_filename,
        "db_path": db_path,
    }

def run_scraper_flow():
    print_section("🚀 Neue Suche starten")
    print("ℹ️  Aktuell wird nur reklama5.mk unterstützt.")
    print()

    global BASE_URL_TEMPLATE
    print_section("🔗 Basis-URL-Konfiguration")
    new_base_url = input(
        "Eigene Such-URL einfügen (Enter = bestehende URL behalten): "
    ).strip()
    if new_base_url:
        try:
            BASE_URL_TEMPLATE = build_base_url_template(new_base_url)
            print("✨ Verwende neue Basis-URL-Vorlage:")
            print(f"    {shorten_url(BASE_URL_TEMPLATE)}")
        except ValueError as exc:
            print(f"⚠️  {exc} Behalte Standard bei.")

    print_section("🔎 Suchparameter")
    search_term = input("Suchbegriff (z. B. „aygo“) eingeben (oder Enter für alle): ").strip()
    print()
    search_term = search_term if search_term else ""

    days_input = input(
        "Wie viele Tage zurück sollen berücksichtigt werden? (Enter = 1 Tag): "
    ).strip()
    print()
    if not days_input:
        days = 1
    else:
        try:
            days = int(days_input)
            if days <= 0:
                raise ValueError
        except ValueError:
            print("⚠️  Ungültige Eingabe von Tagen. Zurück zum Hauptmenü.")
            return "main"

    limit_input = input("Wieviele Einträge sollen maximal eingelesen werden? (Enter = alle): ").strip()
    print()
    if limit_input:
        try:
            limit = int(limit_input)
            if limit <= 0:
                raise ValueError
        except ValueError:
            print("⚠️  Ungültige Eingabe für Eintrags-Limit. Zurück zum Hauptmenü.")
            return "main"
    else:
        limit = None

    detail_input = input("Genaue Erfassung aktivieren? (j/N – Enter = nein): ").strip().lower()
    print()
    enable_detail_capture = detail_input in {"j", "ja", "y", "yes"}
    detail_delay_range = None
    detail_worker_count = 1
    detail_rate_limit_permits = None
    if enable_detail_capture:
        print("🔍 Genaue Erfassung aktiv. Jede Anzeige wird einzeln geöffnet.")
        print()
        worker_input = input(
            "Wie viele Detailseiten sollen parallel geladen werden? (Enter = 3, min 1, max 5): "
        ).strip()
        print()
        if not worker_input:
            detail_worker_count = 3
        else:
            try:
                detail_worker_count = int(worker_input)
            except ValueError:
                detail_worker_count = 3
        detail_worker_count = max(1, min(5, detail_worker_count))

        random_delay_input = input(
            "Zufällige Pause (ca. 1–2 Sekunden) zwischen Detailseiten einfügen? (Enter = ja, n = feste Pause): "
        ).strip().lower()
        print()
        if random_delay_input in {"", "j", "ja", "y", "yes"}:
            detail_delay_range = (1.0, 2.0)
        else:
            fixed_delay_input = input(
                "Feste Pause zwischen Detailseiten in Sekunden (Enter oder 0 = keine): "
            ).strip()
            print()
            if not fixed_delay_input or fixed_delay_input == "0":
                detail_delay_range = None
                print("⚡ Keine zusätzliche Pause zwischen den Detailseiten.")
            else:
                try:
                    value = float(fixed_delay_input.replace(",", "."))
                    if value < 0:
                        raise ValueError
                    detail_delay_range = (value, value)
                    print(f"⏱️  Verwende feste Pause von {value:.2f} Sekunden.")
                except ValueError:
                    detail_delay_range = (1.0, 2.0)
                    print("⚠️  Ungültige Eingabe – verwende zufällige Pause von 1–2 Sekunden.")

        rate_limit_input = input(
            "Optionale Ratenbegrenzung (max. gleichzeitige Detailabrufe, Enter = aus): "
        ).strip()
        print()
        if rate_limit_input:
            try:
                permits = int(rate_limit_input)
                if permits > 0:
                    detail_rate_limit_permits = min(permits, detail_worker_count)
                else:
                    detail_rate_limit_permits = None
            except ValueError:
                detail_rate_limit_permits = None

    db_path = prompt_db_path()

    config = ScraperConfig(
        search_term=search_term,
        days=days,
        limit=limit,
        enable_detail_capture=enable_detail_capture,
        detail_delay_range=detail_delay_range,
        detail_worker_count=detail_worker_count,
        detail_rate_limit_permits=detail_rate_limit_permits,
        csv_filename=OUTPUT_CSV if not db_path else None,
        base_url_template=BASE_URL_TEMPLATE,
        db_path=db_path,
    )
    return run_scraper_flow_from_config(config)


def _positive_int(value):
    try:
        ivalue = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Bitte eine ganze Zahl angeben.") from exc
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("Wert muss größer als 0 sein.")
    return ivalue


def _non_negative_float(value):
    text = str(value).strip().replace(",", ".")
    try:
        fvalue = float(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Bitte eine Zahl verwenden.") from exc
    if fvalue < 0:
        raise argparse.ArgumentTypeError("Wert darf nicht negativ sein.")
    return fvalue


def build_cli_parser():
    parser = argparse.ArgumentParser(
        description="Nicht-interaktive Ausführung des reklama5-Scrapers",
        add_help=True,
    )
    parser.add_argument("--search", help="Suchbegriff", default="")
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=1,
        help="Anzahl der Tage, die berücksichtigt werden (Standard: 1)",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        help="Maximale Anzahl an Einträgen (optional)",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Genaue Erfassung der einzelnen Inserate aktivieren",
    )
    parser.add_argument(
        "--details-workers",
        type=_positive_int,
        default=3,
        help="Parallele Detailabrufe (Standard: 3, min 1, max 5)",
    )
    parser.add_argument(
        "--details-delay",
        type=_non_negative_float,
        default=None,
        help="Feste Pause zwischen Detailseiten in Sekunden",
    )
    parser.add_argument(
        "--details-rate-limit",
        type=_positive_int,
        default=None,
        help="Maximale gleichzeitige Detailabrufe",
    )
    parser.add_argument(
        "--csv",
        dest="csv_filename",
        help="Zieldatei für die exportierten Rohdaten",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        help="SQLite-Datenbankdatei statt CSV verwenden",
    )
    parser.add_argument(
        "--base-url",
        dest="base_url",
        help="Eigene Such-URL mit Platzhaltern {search_term} und {page_num}",
    )
    return parser


def run_cli_from_args(args):
    db_path = (args.db_path or "").strip() or None
    if db_path and args.csv_filename:
        print("ℹ️  --db angegeben – CSV-Export wird bevorzugt und CSV ignoriert.")
    csv_filename = None if db_path else (args.csv_filename or OUTPUT_CSV)

    detail_delay_range = None
    detail_worker_count = args.details_workers or 3
    detail_rate_limit = args.details_rate_limit

    if args.details:
        if args.details_delay is not None and args.details_delay > 0:
            detail_delay_range = (args.details_delay, args.details_delay)
        elif args.details_delay == 0:
            detail_delay_range = None
        else:
            detail_delay_range = (1.0, 2.0)
        detail_worker_count = max(1, min(5, detail_worker_count))
        if detail_rate_limit is not None:
            detail_rate_limit = min(detail_rate_limit, detail_worker_count)
    else:
        detail_rate_limit = None
        detail_delay_range = None
        detail_worker_count = 1

    base_url_template = BASE_URL_TEMPLATE
    if args.base_url:
        base_url_template = build_base_url_template(args.base_url)

    config = ScraperConfig(
        search_term=args.search or "",
        days=args.days,
        limit=args.limit,
        enable_detail_capture=args.details,
        detail_delay_range=detail_delay_range,
        detail_worker_count=detail_worker_count,
        detail_rate_limit_permits=detail_rate_limit,
        csv_filename=csv_filename,
        base_url_template=base_url_template,
        db_path=db_path,
    )
    result = run_scraper_flow_from_config(config, interactive=False)
    print_section("✅ Automatischer Lauf abgeschlossen")
    storage_info = (
        f"SQLite-Datei: {result['db_path']}"
        if result.get("db_path")
        else f"CSV-Datei: {result['csv_filename']}"
    )
    print(
        f"   • {storage_info}\n"
        f"   • Gespeicherte Einträge: {result['total_saved']}"
    )
    return result


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    if argv:
        parser = build_cli_parser()
        args = parser.parse_args(argv)
        return run_cli_from_args(args)
    while True:
        clear_screen()
        print_banner("SCRAPER FÜR reklama5.mk AUTOMOBILE")
        print("Was möchtest du tun?")
        print("  [1] 🔍 Neue Suche durchführen")
        print("  [2] 📊 Analyse")
        print()
        print("  [q] ❌ Programm beenden")
        print()
        start_choice = (input("Deine Wahl (Enter = 1): ").strip() or "1").lower()

        if start_choice in {"q", "quit", "3"}:
            print("👋 Bis zum nächsten Mal!")
            break
        if start_choice == "2":
            clear_screen()
            db_path = sqlite_store.DEFAULT_DB_PATH
            if not os.path.isfile(db_path):
                print(
                    f"⚠️  SQLite-Datei „{db_path}“ wurde nicht gefunden. Zurück zum Hauptmenü …"
                )
                time.sleep(1.5)
                continue
            outcome = analysis_menu(db_path=db_path)
            if outcome == "main":
                continue
            print("👋 Bis zum nächsten Mal!")
            break
        elif start_choice == "1":
            clear_screen()
            outcome = run_scraper_flow()
            if outcome == "main":
                continue
            print("👋 Bis zum nächsten Mal!")
            break
        else:
            print("⚠️  Ungültige Auswahl. Bitte erneut versuchen.")
            time.sleep(1.5)

if __name__ == "__main__":
    main()
