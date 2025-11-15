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
from dataclasses import dataclass, asdict, replace
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union
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

DEFAULT_BASE_URL_TEMPLATE = "https://www.reklama5.mk/Search?city=&cat=24&q={search_term}&page={page_num}"
BASE_URL_TEMPLATE = DEFAULT_BASE_URL_TEMPLATE
OUTPUT_CSV        = "reklama5_autos_raw.csv"
OUTPUT_AGG        = "reklama5_autos_agg.json"

CSV_FIELDNAMES = [
    "id", "link", "make", "model", "year", "price", "km", "kw", "ps",
    "fuel", "gearbox", "body", "color", "registration", "reg_until",
    "emission_class", "date", "city", "promoted"
]

DB_FIELDNAMES = [name for name in CSV_FIELDNAMES if name != "promoted"]

DETAIL_ONLY_FIELDS = [
    "fuel",
    "gearbox",
    "body",
    "color",
    "registration",
    "reg_until",
    "emission_class",
]

# Only a subset of fields is available from the overview pages without issuing
# a detail request. Change detection must therefore rely exclusively on those
# values to decide whether a listing already exists in the database and whether
# a detail fetch is necessary.
OVERVIEW_COMPARISON_FIELDS = [
    "link",
    "price",
    "km",
    "date",
]

STATUS_COMPARISON_FIELDS = list(OVERVIEW_COMPARISON_FIELDS)

DATE_COMPARISON_TOLERANCE = timedelta(hours=1)

INLINE_PROGRESS_SYMBOL = "•"

STATUS_NEW = "new"
STATUS_CHANGED = "changed"
STATUS_UNCHANGED = "unchanged"
STATUS_LABELS = {
    STATUS_NEW: "neu",
    STATUS_CHANGED: "geändert",
    STATUS_UNCHANGED: "unverändert",
}

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

SETTINGS_DIR = os.path.join("data")
USER_SETTINGS_FILE = os.path.join(SETTINGS_DIR, "user_settings.json")


def format_duration(seconds):
    try:
        total_seconds = int(round(float(seconds)))
    except (TypeError, ValueError):
        total_seconds = 0
    if total_seconds < 0:
        total_seconds = 0
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


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
    skip_unchanged: bool = False
    developer_logging: bool = False


@dataclass
class UserSettings:
    base_url_template: str = DEFAULT_BASE_URL_TEMPLATE
    search_term: str = ""
    days: int = 1
    limit: Optional[int] = None
    enable_detail_capture: bool = False
    detail_delay_range: Union[None, Tuple[float, float]] = (1.0, 2.0)
    detail_worker_count: int = 3
    detail_rate_limit_permits: Optional[int] = None
    csv_filename: str = OUTPUT_CSV
    use_sqlite: bool = False
    skip_unchanged: bool = False
    developer_logging: bool = False


def _serialize_user_settings(settings: UserSettings):
    data = asdict(settings)
    delay_range = data.get("detail_delay_range")
    if delay_range is not None:
        data["detail_delay_range"] = list(delay_range)
    return data


def _deserialize_delay_range(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and len(value) == 2:
        try:
            start = float(value[0])
            end = float(value[1])
            return (start, end)
        except (TypeError, ValueError):
            return (1.0, 2.0)
    try:
        numeric = float(value)
        return (numeric, numeric)
    except (TypeError, ValueError):
        return (1.0, 2.0)


def load_user_settings():
    if not os.path.isfile(USER_SETTINGS_FILE):
        return UserSettings()
    try:
        with open(USER_SETTINGS_FILE, mode="r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return UserSettings()
    base_url = raw.get("base_url_template") or DEFAULT_BASE_URL_TEMPLATE
    days = raw.get("days", 1)
    try:
        days = int(days)
    except (TypeError, ValueError):
        days = 1
    if days <= 0:
        days = 1
    limit = raw.get("limit")
    try:
        limit = int(limit) if limit is not None else None
    except (TypeError, ValueError):
        limit = None
    if limit is not None and limit <= 0:
        limit = None
    worker_count = raw.get("detail_worker_count", 3)
    try:
        worker_count = int(worker_count)
    except (TypeError, ValueError):
        worker_count = 3
    worker_count = max(1, min(5, worker_count))
    rate_limit = raw.get("detail_rate_limit_permits")
    try:
        rate_limit = int(rate_limit) if rate_limit is not None else None
    except (TypeError, ValueError):
        rate_limit = None
    if rate_limit is not None and rate_limit <= 0:
        rate_limit = None
    use_sqlite_raw = raw.get("use_sqlite")
    if isinstance(use_sqlite_raw, str):
        use_sqlite = use_sqlite_raw.strip().lower() in {"1", "true", "t", "yes", "y", "j", "ja"}
    else:
        use_sqlite = bool(use_sqlite_raw)
    if not use_sqlite and raw.get("db_path"):
        use_sqlite = True
    settings = UserSettings(
        base_url_template=base_url,
        search_term=raw.get("search_term", ""),
        days=days,
        limit=limit,
        enable_detail_capture=bool(raw.get("enable_detail_capture", False)),
        detail_delay_range=_deserialize_delay_range(raw.get("detail_delay_range")),
        detail_worker_count=worker_count,
        detail_rate_limit_permits=rate_limit,
        csv_filename=raw.get("csv_filename", OUTPUT_CSV),
        use_sqlite=use_sqlite,
        skip_unchanged=bool(raw.get("skip_unchanged", False)),
        developer_logging=bool(raw.get("developer_logging", False)),
    )
    return settings


def save_user_settings(settings: UserSettings):
    os.makedirs(SETTINGS_DIR, exist_ok=True)
    data = _serialize_user_settings(settings)
    with open(USER_SETTINGS_FILE, mode="w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


current_settings = load_user_settings()


def _apply_settings_to_globals():
    global BASE_URL_TEMPLATE
    BASE_URL_TEMPLATE = current_settings.base_url_template or DEFAULT_BASE_URL_TEMPLATE


def _update_settings(**kwargs):
    global current_settings
    current_settings = replace(current_settings, **kwargs)
    save_user_settings(current_settings)
    _apply_settings_to_globals()


_apply_settings_to_globals()

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def _build_developer_logger(enabled):
    if not enabled:
        return None

    def _logger(message):
        print(f"🛠️  DEV: {message}")

    return _logger


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


def _format_delay_label(value):
    if value is None:
        return "aus"
    try:
        start, end = value
    except (TypeError, ValueError):
        return "aus"
    if abs(start - end) < 0.0001:
        return f"{start:.2f}s fest"
    return f"{start:.2f}–{end:.2f}s zufällig"


def settings_menu():
    while True:
        clear_screen()
        settings = current_settings
        limit_label = settings.limit if settings.limit is not None else "alle"
        delay_label = _format_delay_label(settings.detail_delay_range)
        storage_label = (
            f"SQLite → {sqlite_store.DEFAULT_DB_PATH}"
            if settings.use_sqlite
            else f"CSV → {settings.csv_filename}"
        )
        detail_label = "aktiv" if settings.enable_detail_capture else "aus"
        unchanged_label = "überspringen" if settings.skip_unchanged else "markieren"
        developer_label = "aktiv" if settings.developer_logging else "aus"
        print_section("⚙️  Standard-Einstellungen")
        print(f"   • Basis-URL.........: {shorten_url(settings.base_url_template)}")
        print(f"   • Suchbegriff.......: {settings.search_term or 'alle'}")
        print(f"   • Tage..............: {settings.days}")
        print(f"   • Limit.............: {limit_label}")
        print(f"   • Detail-Erfassung..: {detail_label}")
        print(f"   • Detail-Worker.....: {settings.detail_worker_count}")
        print(f"   • Detail-Pause......: {delay_label}")
        rate_label = settings.detail_rate_limit_permits or "aus"
        print(f"   • Detail-Rate-Limit.: {rate_label}")
        print(f"   • Speicherung.......: {storage_label}")
        print(f"   • Unveränderte......: {unchanged_label}")
        print(f"   • Entwickler-Log....: {developer_label}")
        print()
        print("  [1] 🔗 Basis-URL anpassen")
        print("  [2] 🔎 Suchbegriff festlegen")
        print("  [3] ⏱️  Standard-Tage ändern")
        print("  [4] 📏 Limit setzen")
        print("  [5] 🔍 Detail-Erfassung umschalten")
        print("  [6] 👥 Detail-Worker ändern")
        print("  [7] ⏳ Detail-Pause konfigurieren")
        print("  [8] 🚦 Detail-Rate-Limit setzen")
        print("  [9] 💾 CSV-Datei ändern")
        print("  [10] 🗄️  SQLite-Speicherung an/aus")
        print("  [11] ♻️  Umgang mit unveränderten Einträgen")
        print("  [12] 🧑‍💻 Entwickler-Einstellungen")
        print()
        print("  [0] ↩️  Zurück zum Hauptmenü")
        print()
        choice = input("Deine Auswahl: ").strip()
        if choice == "1":
            new_url = input(
                "Neue Basis-URL mit {search_term} und {page_num} "
                "(Enter = unverändert, 'standard' = Standard wiederherstellen): "
            ).strip()
            if not new_url:
                continue
            if new_url.lower() in {"standard", "default", "reset", "std"}:
                _update_settings(base_url_template=DEFAULT_BASE_URL_TEMPLATE)
                print("✅ Basis-URL auf Standard zurückgesetzt.")
                time.sleep(1.2)
                continue
            try:
                normalized = build_base_url_template(new_url)
                _update_settings(base_url_template=normalized)
                print("✅ Basis-URL gespeichert.")
            except ValueError as exc:
                print(f"⚠️  {exc}")
            time.sleep(1.2)
        elif choice == "2":
            current_label = settings.search_term or "alle"
            new_term = input(
                f"Standard-Suchbegriff (Enter = unverändert, 'leer' = alle, aktuell {current_label}): "
            ).strip()
            if not new_term:
                continue
            if new_term.lower() == "leer":
                _update_settings(search_term="")
            else:
                _update_settings(search_term=new_term)
        elif choice == "3":
            new_days = input(
                f"Standard-Tage (Enter = {settings.days}, min 1): "
            ).strip()
            if new_days:
                try:
                    value = max(1, int(new_days))
                    _update_settings(days=value)
                except ValueError:
                    print("⚠️  Ungültige Zahl.")
                    time.sleep(1.2)
        elif choice == "4":
            new_limit = input(
                f"Max. Einträge (Enter = {limit_label}, 0 = alle): "
            ).strip()
            if new_limit:
                try:
                    limit_value = int(new_limit)
                    if limit_value <= 0:
                        _update_settings(limit=None)
                    else:
                        _update_settings(limit=limit_value)
                except ValueError:
                    print("⚠️  Bitte eine ganze Zahl eingeben.")
                    time.sleep(1.2)
        elif choice == "5":
            toggle = input(
                f"Detail-Erfassung aktivieren? (Enter = {detail_label}, j/n): "
            ).strip().lower()
            if toggle:
                enabled = toggle in {"j", "ja", "y", "yes", "1"}
                if toggle in {"n", "nein", "0"}:
                    enabled = False
                _update_settings(enable_detail_capture=enabled)
        elif choice == "6":
            new_workers = input(
                f"Parallel-Worker (Enter = {settings.detail_worker_count}, 1-5): "
            ).strip()
            if new_workers:
                try:
                    value = max(1, min(5, int(new_workers)))
                    _update_settings(detail_worker_count=value)
                except ValueError:
                    print("⚠️  Ungültige Zahl.")
                    time.sleep(1.2)
        elif choice == "7":
            print("Format: auto = 1-2s, 0 = keine Pause, x = feste Sek., x-y = Zufallsbereich")
            new_delay = input(
                f"Eingabe (Enter = {delay_label}): "
            ).strip().lower()
            if new_delay:
                if new_delay == "auto":
                    _update_settings(detail_delay_range=(1.0, 2.0))
                elif new_delay == "0":
                    _update_settings(detail_delay_range=None)
                elif "-" in new_delay:
                    parts = new_delay.replace(",", ".").split("-", 1)
                    try:
                        start = float(parts[0])
                        end = float(parts[1])
                        if start > end:
                            start, end = end, start
                        _update_settings(detail_delay_range=(start, end))
                    except ValueError:
                        print("⚠️  Ungültiger Bereich.")
                        time.sleep(1.2)
                else:
                    try:
                        value = float(new_delay.replace(",", "."))
                        if value < 0:
                            raise ValueError
                        _update_settings(detail_delay_range=(value, value))
                    except ValueError:
                        print("⚠️  Ungültige Eingabe.")
                        time.sleep(1.2)
        elif choice == "8":
            new_rate = input(
                f"Max. gleichzeitige Detailabrufe (Enter = {rate_label}, 0 = aus): "
            ).strip()
            if new_rate:
                try:
                    permits = int(new_rate)
                    if permits <= 0:
                        _update_settings(detail_rate_limit_permits=None)
                    else:
                        permits = min(permits, current_settings.detail_worker_count)
                        _update_settings(detail_rate_limit_permits=permits)
                except ValueError:
                    print("⚠️  Bitte eine Zahl eingeben.")
                    time.sleep(1.2)
        elif choice == "9":
            new_csv = input(
                f"CSV-Datei (Enter = {settings.csv_filename}): "
            ).strip()
            if new_csv:
                _update_settings(csv_filename=new_csv)
        elif choice == "10":
            current_label = "aktiv" if settings.use_sqlite else "aus"
            toggle = input(
                f"SQLite-Speicherung aktivieren? (Enter = {current_label}, j/n): "
            ).strip().lower()
            if toggle in {"j", "ja", "y", "yes", "1"}:
                _update_settings(use_sqlite=True)
            elif toggle in {"n", "nein", "0"}:
                _update_settings(use_sqlite=False)
        elif choice == "11":
            current_mode = "überspringen" if settings.skip_unchanged else "markieren"
            prompt = (
                "Unveränderte Einträge (Enter = {current}, 'skip' = überspringen, "
                "'mark' = nur markieren): "
            ).format(current=current_mode)
            new_mode = input(prompt).strip().lower()
            if new_mode in {"skip", "überspringen", "u"}:
                _update_settings(skip_unchanged=True)
            elif new_mode in {"mark", "m", "anzeigen"}:
                _update_settings(skip_unchanged=False)
        elif choice == "12":
            developer_settings_menu()
        elif choice == "0":
            return
        else:
            print("⚠️  Ungültige Auswahl. Bitte erneut versuchen.")
            time.sleep(1.2)


def developer_settings_menu():
    while True:
        clear_screen()
        settings = current_settings
        log_label = "aktiv" if settings.developer_logging else "aus"
        print_section("🧑‍💻 Entwickler-Einstellungen")
        print(f"   • Detaillierte Protokollierung: {log_label}")
        print()
        print("  [1] Protokollierung umschalten")
        print()
        print("  [0] ↩️  Zurück")
        print()
        choice = input("Deine Auswahl: ").strip()
        if choice == "1":
            prompt = f"Ausführliche Protokollierung aktivieren? (Enter = {log_label}, j/n): "
            toggle = input(prompt).strip().lower()
            if not toggle:
                continue
            if toggle in {"j", "ja", "y", "yes", "1"}:
                _update_settings(developer_logging=True)
            elif toggle in {"n", "nein", "0"}:
                _update_settings(developer_logging=False)
            else:
                print("⚠️  Ungültige Auswahl.")
                time.sleep(1.2)
        elif choice == "0":
            return
        else:
            print("⚠️  Ungültige Auswahl. Bitte erneut versuchen.")
            time.sleep(1.2)


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


def _normalize_listing_payload_for_hash(listing):
    normalized = {}
    for name in CSV_FIELDNAMES:
        value = listing.get(name)
        if isinstance(value, bool):
            value = int(value)
        if isinstance(value, str):
            stripped = value.strip()
            value = stripped or None
        if name == "id" and value is not None:
            value = str(value)
        normalized[name] = value
    return normalized


def _parse_iso_datetime(value):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _dates_equivalent(old_value, new_value):
    old_dt = _parse_iso_datetime(old_value)
    new_dt = _parse_iso_datetime(new_value)
    if old_dt and new_dt:
        return abs((new_dt - old_dt).total_seconds()) <= DATE_COMPARISON_TOLERANCE.total_seconds()
    return old_value == new_value


def classify_listing_status(listings, db_connection, developer_logger=None):
    """Annotate ``listings`` with a status compared to the SQLite store."""

    status_map: Dict[str, Dict[str, object]] = {}
    if not listings:
        return status_map

    normalized_ids_per_listing: List[Optional[str]] = []
    normalized_ids_to_fetch: List[str] = []
    for listing in listings:
        raw_id = listing.get("id")
        normalized_id = str(raw_id) if raw_id not in (None, "") else None
        normalized_ids_per_listing.append(normalized_id)
        if normalized_id:
            normalized_ids_to_fetch.append(normalized_id)

    existing_rows: Dict[str, Dict[str, object]] = {}
    if db_connection is not None and normalized_ids_to_fetch:
        existing_rows = sqlite_store.fetch_listings_by_ids(
            db_connection, normalized_ids_to_fetch
        )

    fallback_counter = 0
    total_items = len(listings)
    for idx, listing in enumerate(listings):
        fallback_counter += 1
        normalized_id = normalized_ids_per_listing[idx]
        cache_key = normalized_id or f"tmp-{fallback_counter}"
        listing_status = STATUS_NEW
        changes: Dict[str, Dict[str, object]] = {}
        use_database = db_connection is not None and normalized_id
        display_id = normalized_id or f"tmp-{fallback_counter}"

        if developer_logger:
            if not normalized_id:
                developer_logger(
                    f"[DB] ({fallback_counter}/{total_items}) Anzeige ohne ID – Status {STATUS_LABELS[STATUS_NEW]}"
                )
            elif db_connection is None:
                developer_logger(
                    f"[DB] ({fallback_counter}/{total_items}) ID {display_id}: keine Datenbank – Status {STATUS_LABELS[STATUS_NEW]}"
                )
            else:
                developer_logger(
                    f"[DB] ({fallback_counter}/{total_items}) Suche ID {display_id} in der Datenbank"
                )

        existing = None
        if use_database:
            normalized_payload = _normalize_listing_payload_for_hash(listing)
            existing = existing_rows.get(normalized_id)
            if existing is None:
                listing_status = STATUS_NEW
            else:
                fallback_fields = list(DETAIL_ONLY_FIELDS) + [
                    "km",
                    "kw",
                    "ps",
                    "year",
                    "price",
                    "city",
                    "link",
                ]
                for detail_field in fallback_fields:
                    new_value = normalized_payload.get(detail_field)
                    if new_value in (None, ""):
                        existing_value = existing.get(detail_field)
                        if existing_value not in (None, ""):
                            normalized_payload[detail_field] = existing_value

                listing_hash = sqlite_store.calculate_listing_hash(normalized_payload)
                existing_hash = existing.get("hash")

                for field in STATUS_COMPARISON_FIELDS:
                    new_value = normalized_payload.get(field)
                    if new_value in (None, ""):
                        continue
                    old_value = existing.get(field)
                    values_equal = (
                        _dates_equivalent(old_value, new_value)
                        if field == "date"
                        else old_value == new_value
                    )
                    if not values_equal:
                        changes[field] = {"old": old_value, "new": new_value}

                if changes and existing_hash != listing_hash:
                    changes["hash"] = {"old": existing_hash, "new": listing_hash}

                listing_status = STATUS_CHANGED if changes else STATUS_UNCHANGED

            if developer_logger:
                if existing is None:
                    developer_logger(
                        f"[DB] ({fallback_counter}/{total_items}) ID {display_id} nicht gefunden – Status {STATUS_LABELS[listing_status]}"
                    )
                elif listing_status == STATUS_CHANGED:
                    changed_fields = [field for field in changes.keys() if field != "hash"]
                    if "hash" in changes and not changed_fields:
                        changed_fields.append("Hash")
                    field_list = ", ".join(changed_fields) if changed_fields else "-"
                    developer_logger(
                        f"[DB] ({fallback_counter}/{total_items}) ID {display_id} geändert – Felder: {field_list}"
                    )
                else:
                    developer_logger(
                        f"[DB] ({fallback_counter}/{total_items}) ID {display_id} unverändert"
                    )

        listing["_status"] = listing_status
        listing["_status_changes"] = changes
        status_map[cache_key] = {"status": listing_status, "changes": changes}
    return status_map


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

    sanitized_rows = [
        {name: row.get(name) for name in CSV_FIELDNAMES}
        for row in saved_rows
    ]

    if db_connection is not None:
        sqlite_store.upsert_many(db_connection, sanitized_rows, DB_FIELDNAMES)
        return len(saved_rows)

    target_csv = csv_filename or OUTPUT_CSV
    file_exists = os.path.isfile(target_csv)
    with open(target_csv, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(sanitized_rows)
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

    db_enabled = bool(config.db_path)
    db_path = sqlite_store.DEFAULT_DB_PATH if db_enabled else None
    db_connection = None
    csv_filename = None
    if db_path:
        try:
            db_connection = sqlite_store.open_database(db_path)
            sqlite_store.init_schema(db_connection, DB_FIELDNAMES)
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

    skip_unchanged = bool(getattr(config, "skip_unchanged", False))
    developer_logging_enabled = bool(getattr(config, "developer_logging", False))
    developer_logger = _build_developer_logger(developer_logging_enabled)

    if csv_filename and os.path.isfile(csv_filename):
        os.remove(csv_filename)

    if developer_logger:
        if db_connection is not None:
            developer_logger(f"Starte Lauf mit SQLite-Ziel {db_path}")
        else:
            developer_logger(
                f"Starte Lauf mit CSV-Ziel {csv_filename or OUTPUT_CSV}"
            )

    total_found = 0
    total_saved = 0
    seen_ids = set()
    duplicates_skipped_total = 0
    status_totals = {
        STATUS_NEW: 0,
        STATUS_CHANGED: 0,
        STATUS_UNCHANGED: 0,
    }
    skipped_unchanged_total = 0
    start_time = time.time()
    pages_viewed = 0
    detail_requests = 0

    try:
        for page in range(1, 200):
            if developer_logger:
                developer_logger(f"Lade Seite {page:02d} für Suche '{search_term or 'alle'}'")
            html = fetch_listing_page(search_term, page)
            if not html:
                print()
                print(f"⚠️  Seite {page} konnte nicht geladen werden. Stop.")
                if developer_logger:
                    developer_logger(f"Abbruch: Seite {page:02d} lieferte keinen Inhalt")
                break
            listings = parse_listing(html)
            if developer_logger:
                developer_logger(
                    f"Seite {page:02d}: {len(listings)} Inserate in der Übersicht gefunden"
                )

            if not listings:
                print()
                print(f"ℹ️  Keine Listings auf Seite {page} → Stop.")
                if developer_logger:
                    developer_logger(f"Abbruch: Seite {page:02d} enthielt keine Anzeigen")
                break

            eligible_listings = []
            skipped_promoted = 0
            skipped_missing_date = 0
            skipped_old = 0
            for item in listings:
                promoted = bool(item.get("promoted"))
                date_value = item.get("date")
                if promoted:
                    skipped_promoted += 1
                    continue
                if not date_value:
                    skipped_missing_date += 1
                    continue
                if not is_within_days(date_value, days, promoted):
                    skipped_old += 1
                    continue
                eligible_listings.append(item)
            if developer_logger:
                developer_logger(
                    "Filter: {eligible} übrig ({promo} promoviert ignoriert, "
                    "{old} zu alt, {missing} ohne Datum)".format(
                        eligible=len(eligible_listings),
                        promo=skipped_promoted,
                        old=skipped_old,
                        missing=skipped_missing_date,
                    )
                )
            pages_viewed += 1

            duplicates_skipped_page = 0
            deduplicated = []
            if developer_logger and eligible_listings:
                developer_logger(
                    f"Seite {page:02d}: überprüfe {len(eligible_listings)} Anzeigen auf Duplikate"
                )
            for item in eligible_listings:
                item_id = item.get("id")
                if item_id and item_id in seen_ids:
                    duplicates_skipped_page += 1
                    if developer_logger:
                        developer_logger(
                            f"Seite {page:02d}: Anzeige {item_id} bereits verarbeitet – überspringe"
                        )
                    continue
                if item_id:
                    seen_ids.add(item_id)
                deduplicated.append(item)
            eligible_listings = deduplicated
            duplicates_skipped_total += duplicates_skipped_page
            if developer_logger:
                developer_logger(
                    f"Seite {page:02d}: {len(eligible_listings)} eindeutige Anzeigen nach Duplikatprüfung"
                )

            remaining_limit = None
            if limit is not None:
                remaining_limit = max(0, limit - total_saved)
                if remaining_limit == 0:
                    print(
                        f"ℹ️  Maximalanzahl von {limit} Einträgen bereits erreicht. Stop."
                    )
                    if developer_logger:
                        developer_logger(
                            f"Limit {limit} erreicht – beende nach Seite {page:02d}"
                        )
                    break
                eligible_listings = eligible_listings[:remaining_limit]
                if developer_logger:
                    developer_logger(
                        f"Limit aktiv: prüfe nur noch {len(eligible_listings)} Einträge auf Seite {page:02d}"
                    )

            found_on_page = len(eligible_listings)
            total_found += found_on_page

            print(f"Lade Seite {page:02d} ({found_on_page:02d} Treffer)")
            if duplicates_skipped_page:
                print(f"↺ {duplicates_skipped_page} Duplikate übersprungen")
            print(INLINE_PROGRESS_SYMBOL * found_on_page)

            classify_listing_status(
                eligible_listings,
                db_connection,
                developer_logger=developer_logger,
            )
            page_status_counts = {
                STATUS_NEW: 0,
                STATUS_CHANGED: 0,
                STATUS_UNCHANGED: 0,
            }
            for idx, listing in enumerate(eligible_listings, 1):
                status_value = listing.get("_status") or STATUS_NEW
                page_status_counts[status_value] = page_status_counts.get(status_value, 0) + 1
                if developer_logger:
                    listing_id = listing.get("id") or f"ohne-ID#{idx}"
                    if status_value == STATUS_UNCHANGED:
                        decision = "Detailabruf entfällt"
                        if skip_unchanged:
                            decision += ", wird übersprungen"
                    elif not enable_detail_capture:
                        decision = "Detail-Erfassung deaktiviert"
                    else:
                        decision = "Detailabruf geplant"
                    developer_logger(
                        f"[Status] ({idx}/{len(eligible_listings)}) ID {listing_id}: {STATUS_LABELS.get(status_value, status_value)} – {decision}"
                    )
            for key, value in page_status_counts.items():
                status_totals[key] = status_totals.get(key, 0) + value
            if skip_unchanged:
                skipped_unchanged_total += page_status_counts.get(STATUS_UNCHANGED, 0)

            print(
                "   Status: "
                + " | ".join(
                    f"{STATUS_LABELS[key]} {page_status_counts.get(key, 0):02d}"
                    for key in (STATUS_NEW, STATUS_CHANGED, STATUS_UNCHANGED)
                )
            )
            if page_status_counts.get(STATUS_UNCHANGED, 0):
                hint = "übersprungen" if skip_unchanged else "markiert"
                print(
                    f"   ↷ {page_status_counts[STATUS_UNCHANGED]:02d} unveränderte Einträge {hint}"
                )

            detail_candidates = [
                item
                for item in eligible_listings
                if (item.get("_status") or STATUS_NEW) != STATUS_UNCHANGED
            ]
            if developer_logger:
                if not enable_detail_capture:
                    developer_logger("Detail-Erfassung deaktiviert – überspringe Detailaufrufe")
                elif detail_candidates:
                    developer_logger(
                        f"Detailabrufe geplant für {len(detail_candidates)} von {len(eligible_listings)} Anzeigen"
                    )
                else:
                    developer_logger("Keine Detailabrufe nötig – alle Anzeigen unverändert")

            progress_callback = None
            progress_finalize = None
            if enable_detail_capture and detail_candidates:
                detail_requests += len(detail_candidates)
                (
                    progress_callback,
                    progress_finalize,
                ) = build_inline_progress_printer(len(detail_candidates))

            enrich_listings_with_details(
                detail_candidates,
                enable_detail_capture,
                delay_range=detail_delay_range,
                max_items=None,
                progress_callback=progress_callback,
                max_workers=detail_worker_count,
                rate_limit_permits=detail_rate_limit_permits,
            )

            if progress_finalize:
                progress_finalize()
            if developer_logger and enable_detail_capture and detail_candidates:
                developer_logger("Detailabrufe abgeschlossen")

            listings_to_persist = (
                detail_candidates if skip_unchanged else eligible_listings
            )

            save_kwargs = {
                "limit": remaining_limit,
                "csv_filename": csv_filename,
            }
            save_kwargs["pre_filtered"] = True
            if db_connection is not None:
                save_kwargs["db_connection"] = db_connection
            if developer_logger:
                target_label = (
                    f"SQLite ({db_path})" if db_connection is not None else f"CSV ({csv_filename})"
                )
                developer_logger(
                    f"Speichere {len(listings_to_persist)} Anzeigen → {target_label}"
                )
            saved_in_page = save_raw_filtered(
                listings_to_persist,
                days,
                **save_kwargs,
            )
            total_saved += saved_in_page
            print(f"{saved_in_page:02d} von {found_on_page:02d} gespeichert")
            if developer_logger:
                developer_logger(
                    f"Speicherung abgeschlossen – {saved_in_page} Einträge übernommen"
                )
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
                    if developer_logger:
                        developer_logger(
                            f"Abbruchkriterium erreicht: Anzeige {item.get('id')} ist älter als {days} Tage"
                        )
                    stop = True
                    break
            if stop:
                break

            sleep_time = random.uniform(2, 4)
            time.sleep(sleep_time)
            if developer_logger:
                developer_logger(f"Warte {sleep_time:.2f}s vor nächster Seite")
    finally:
        if db_connection is not None:
            db_connection.close()

    total_duration = max(0.0, time.time() - start_time)
    if developer_logger:
        developer_logger(
            f"Zusammenfassung: {total_found} gefunden, {total_saved} gespeichert, Dauer {total_duration:.2f}s"
        )

    print_section("📦 Zusammenfassung")
    print(
        f"   • Geprüfte Einträge : {total_found}\n"
        f"   • Gespeicherte Einträge: {total_saved}\n"
        f"   • Übersprungene Duplikate: {duplicates_skipped_total}\n"
        f"   • Neue Inserate.....: {status_totals.get(STATUS_NEW, 0)}\n"
        f"   • Geänderte Inserate: {status_totals.get(STATUS_CHANGED, 0)}\n"
        f"   • Unveränderte......: {status_totals.get(STATUS_UNCHANGED, 0)}\n"
        f"   • Gesamtdauer.......: {format_duration(total_duration)}\n"
        f"   • Seitenaufrufe.....: {pages_viewed}\n"
        f"   • Detailabrufe......: {detail_requests}\n"
    )
    if skip_unchanged:
        print(f"   • Übersprungene unveränderte Einträge: {skipped_unchanged_total}")

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
        "status_counts": status_totals,
        "skipped_unchanged": skipped_unchanged_total,
        "pages_viewed": pages_viewed,
        "detail_requests": detail_requests,
        "duration_seconds": total_duration,
    }


def _format_settings_summary(settings):
    limit_label = settings.limit if settings.limit is not None else "alle"
    delay_label = _format_delay_label(settings.detail_delay_range)
    storage_label = (
        f"SQLite → {sqlite_store.DEFAULT_DB_PATH}"
        if settings.use_sqlite
        else f"CSV → {settings.csv_filename}"
    )
    detail_label = "aktiv" if settings.enable_detail_capture else "aus"
    rate_label = settings.detail_rate_limit_permits or "aus"
    unchanged_label = "überspringen" if settings.skip_unchanged else "markieren"
    developer_label = "aktiv" if settings.developer_logging else "aus"
    lines = [
        f"   • Basis-URL.........: {shorten_url(settings.base_url_template)}",
        f"   • Suchbegriff.......: {settings.search_term or 'alle'}",
        f"   • Tage..............: {settings.days}",
        f"   • Limit.............: {limit_label}",
        f"   • Detail-Erfassung..: {detail_label}",
        f"   • Detail-Worker.....: {settings.detail_worker_count}",
        f"   • Detail-Pause......: {delay_label}",
        f"   • Detail-Rate-Limit.: {rate_label}",
        f"   • Speicherung.......: {storage_label}",
        f"   • Unveränderte......: {unchanged_label}",
        f"   • Entwickler-Log....: {developer_label}",
    ]
    return "\n".join(lines)


def _build_config_from_settings(settings):
    base_url = settings.base_url_template or BASE_URL_TEMPLATE or DEFAULT_BASE_URL_TEMPLATE
    enable_detail = bool(settings.enable_detail_capture)
    detail_delay_range = settings.detail_delay_range if enable_detail else None
    detail_worker_count = settings.detail_worker_count if enable_detail else 1
    detail_rate_limit_permits = (
        settings.detail_rate_limit_permits if enable_detail else None
    )
    use_sqlite = bool(settings.use_sqlite)
    csv_filename = None if use_sqlite else (settings.csv_filename or OUTPUT_CSV)
    db_path = sqlite_store.DEFAULT_DB_PATH if use_sqlite else None
    return ScraperConfig(
        search_term=settings.search_term or "",
        days=settings.days or 1,
        limit=settings.limit,
        enable_detail_capture=enable_detail,
        detail_delay_range=detail_delay_range,
        detail_worker_count=detail_worker_count,
        detail_rate_limit_permits=detail_rate_limit_permits,
        csv_filename=csv_filename,
        base_url_template=base_url,
        db_path=db_path,
        skip_unchanged=settings.skip_unchanged,
        developer_logging=settings.developer_logging,
    )


def _prompt_detail_delay(default_value):
    default_label = _format_delay_label(default_value)
    random_delay_input = input(
        f"Detail-Pause konfigurieren (Enter = {default_label}, 'auto', '0', oder Sekundenwert): "
    ).strip().lower()
    if not random_delay_input:
        return default_value
    if random_delay_input == "auto":
        return (1.0, 2.0)
    if random_delay_input == "0":
        return None
    try:
        if "-" in random_delay_input:
            start_txt, end_txt = random_delay_input.replace(",", ".").split("-", 1)
            start_val = float(start_txt)
            end_val = float(end_txt)
            if start_val > end_val:
                start_val, end_val = end_val, start_val
            return (start_val, end_val)
        value = float(random_delay_input.replace(",", "."))
        if value < 0:
            raise ValueError
        return (value, value)
    except ValueError:
        print("⚠️  Ungültige Eingabe – verwende vorhandenen Wert.")
        return default_value


def _prompt_temporary_overrides(settings):
    overrides = {}
    working_settings = settings
    while True:
        clear_screen()
        print_section("📝 Aktuelle Einstellungen")
        print(_format_settings_summary(working_settings))
        print()
        print("  [b] 🔗 Basis-URL")
        print("  [s] 🔎 Suchbegriff")
        print("  [t] ⏱️  Tage")
        print("  [l] 📏 Limit")
        print("  [d] 🔍 Detail-Erfassung")
        print("  [w] 👥 Detail-Worker")
        print("  [p] ⏳ Detail-Pause")
        print("  [r] 🚦 Detail-Rate-Limit")
        print("  [c] 💾 CSV-Datei")
        print("  [x] 🗄️  SQLite-Speicherung")
        print("  [u] ♻️  Umgang mit unveränderten Einträgen")
        print("  [v] 🧑‍💻 Entwickler-Log")
        print()
        choice = input("Feld wählen ([Enter] fertig, q = Abbrechen): ").strip().lower()
        if not choice:
            return overrides
        if choice in {"q", "quit"}:
            return None
        if choice == "b":
            new_url = input(
                "Neue Basis-URL mit {search_term} und {page_num} (Enter = unverändert): "
            ).strip()
            if new_url:
                try:
                    normalized = build_base_url_template(new_url)
                    overrides["base_url_template"] = normalized
                except ValueError as exc:
                    print(f"⚠️  {exc}")
                    time.sleep(1)
        elif choice == "s":
            overrides["search_term"] = input("Suchbegriff (leer = alle): ").strip()
        elif choice == "t":
            raw = input("Tage (1..n): ").strip()
            if raw:
                try:
                    value = int(raw)
                    if value <= 0:
                        raise ValueError
                    overrides["days"] = value
                except ValueError:
                    print("⚠️  Ungültige Eingabe – Tage bleiben unverändert.")
                    time.sleep(1)
        elif choice == "l":
            raw = input("Limit (leer = alle): ").strip()
            if not raw:
                overrides["limit"] = None
            else:
                try:
                    value = int(raw)
                    if value <= 0:
                        raise ValueError
                    overrides["limit"] = value
                except ValueError:
                    print("⚠️  Ungültige Eingabe – Limit bleibt unverändert.")
                    time.sleep(1)
        elif choice == "d":
            detail_input = input("Detail-Erfassung aktivieren? (j/N): ").strip().lower()
            overrides["enable_detail_capture"] = detail_input in {"j", "ja", "y", "yes"}
        elif choice == "w":
            raw = input("Detail-Worker (1-5): ").strip()
            if raw:
                try:
                    value = int(raw)
                    overrides["detail_worker_count"] = max(1, min(5, value))
                except ValueError:
                    print("⚠️  Ungültige Eingabe – Worker bleiben unverändert.")
                    time.sleep(1)
        elif choice == "p":
            overrides["detail_delay_range"] = _prompt_detail_delay(
                working_settings.detail_delay_range
            )
        elif choice == "r":
            raw = input("Rate-Limit (leer = aus): ").strip()
            if not raw:
                overrides["detail_rate_limit_permits"] = None
            else:
                try:
                    value = int(raw)
                    overrides["detail_rate_limit_permits"] = value if value > 0 else None
                except ValueError:
                    print("⚠️  Ungültige Eingabe – Rate-Limit bleibt unverändert.")
                    time.sleep(1)
        elif choice == "c":
            filename = input("CSV-Dateiname (Enter = Standard): ").strip()
            if filename:
                overrides["csv_filename"] = filename
        elif choice == "x":
            toggle = input(
                "SQLite-Speicherung aktivieren? (j/n, Enter = unverändert): "
            ).strip().lower()
            if toggle in {"j", "ja", "y", "yes", "1"}:
                overrides["use_sqlite"] = True
            elif toggle in {"n", "nein", "0"}:
                overrides["use_sqlite"] = False
        elif choice == "u":
            current_label = "überspringen" if working_settings.skip_unchanged else "markieren"
            new_mode = input(
                f"Unveränderte Einträge (Enter = {current_label}, 'skip'/'mark'): "
            ).strip().lower()
            if new_mode in {"skip", "überspringen", "u"}:
                overrides["skip_unchanged"] = True
            elif new_mode in {"mark", "m", "anzeigen"}:
                overrides["skip_unchanged"] = False
        elif choice == "v":
            toggle = input(
                "Ausführliche Protokollierung aktivieren? (j/N): "
            ).strip().lower()
            if toggle in {"j", "ja", "y", "yes", "1"}:
                overrides["developer_logging"] = True
            elif toggle in {"n", "nein", "0"}:
                overrides["developer_logging"] = False
        else:
            print("⚠️  Unbekannte Auswahl.")
            time.sleep(1)
        if overrides:
            working_settings = replace(settings, **overrides)


def run_scraper_flow():
    print_section("🚀 Neue Suche starten")
    print(
        "ℹ️  Aktuell wird nur reklama5.mk unterstützt. Die Standardwerte stammen aus dem Menü ⚙️ Einstellungen."
    )
    print()

    settings = current_settings
    print_section("📋 Zusammenfassung")
    print(_format_settings_summary(settings))
    print()
    choice = input(
        "[Enter] mit diesen Einstellungen starten / [e] Einzelwerte kurzfristig anpassen: "
    ).strip().lower()

    if choice == "e":
        overrides = _prompt_temporary_overrides(settings)
        if overrides is None:
            print("↩️  Abbruch – zurück zum Hauptmenü.")
            return "main"
        if overrides:
            settings = replace(settings, **overrides)
            save_choice = input(
                "Als neue Standardeinstellung speichern? (j/N): "
            ).strip().lower()
            if save_choice in {"j", "ja", "y", "yes"}:
                _update_settings(**overrides)
                settings = current_settings
            else:
                print("💡 Temporäre Einstellungen werden nicht gespeichert.")

    config = _build_config_from_settings(settings)
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
        "--use-sqlite",
        action="store_true",
        help="Speichert Ergebnisse in der Standard-SQLite-Datenbank",
    )
    parser.add_argument(
        "--base-url",
        dest="base_url",
        help="Eigene Such-URL mit Platzhaltern {search_term} und {page_num}",
    )
    parser.add_argument(
        "--skip-unchanged",
        action="store_true",
        help="Unveränderte Einträge nicht erneut speichern",
    )
    parser.add_argument(
        "--developer-log",
        action="store_true",
        help="Ausführliche Entwickler-Protokolle für jeden Schritt ausgeben",
    )
    return parser


def run_cli_from_args(args):
    db_path = sqlite_store.DEFAULT_DB_PATH if args.use_sqlite else None
    if db_path and args.csv_filename:
        print("ℹ️  --use-sqlite angegeben – CSV-Export wird deaktiviert.")
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
        skip_unchanged=bool(args.skip_unchanged),
        developer_logging=bool(args.developer_log),
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
    status_counts = result.get("status_counts") or {}
    if status_counts:
        print(
            f"   • Neue Inserate.....: {status_counts.get(STATUS_NEW, 0)}\n"
            f"   • Geänderte Inserate: {status_counts.get(STATUS_CHANGED, 0)}\n"
            f"   • Unveränderte......: {status_counts.get(STATUS_UNCHANGED, 0)}"
        )
        skipped = result.get("skipped_unchanged", 0)
        if skipped:
            print(f"   • Übersprungene unveränderte Einträge: {skipped}")
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
        print("  [1] 🔍 Neue Suche durchführen (nutzt ⚙️ Einstellungen)")
        print("  [2] 📊 Analyse")
        print("  [3] ⚙️ Einstellungen verwalten")
        print()
        print("  [q] ❌ Programm beenden")
        print()
        start_choice = (input("Deine Wahl (Enter = 1): ").strip() or "1").lower()

        if start_choice in {"q", "quit"}:
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
        elif start_choice == "3":
            settings_menu()
            continue
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
