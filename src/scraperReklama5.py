# scraper_reklama5_with_km_kw_ps.py

import time
import random
import re
import os
import csv
import json
import warnings
from datetime import datetime, timedelta
from collections import defaultdict

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
    from bs4 import BeautifulSoup
    soup     = BeautifulSoup(html, "html.parser")
    results  = []
    listings = soup.select("div.row.ad-top-div")
    print(f"INFO: Found {len(listings)} listings on this page")
    for listing in listings:
        link_elem     = listing.select_one("h3 > a.SearchAdTitle")
        if not link_elem:
            continue
        href          = link_elem.get("href", "")
        m_id          = re.search(r"ad=(\d+)", href)
        ad_id         = m_id.group(1) if m_id else None
        full_link     = f"https://m.reklama5.mk/AdDetails?ad={ad_id}" if ad_id else None

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

def save_raw_filtered(rows, days):
    file_exists = os.path.isfile(OUTPUT_CSV)
    with open(OUTPUT_CSV, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id","link","make","model","year","price","km","kw","ps","date","city","promoted"])
        if not file_exists:
            writer.writeheader()
        for r in rows:
            if r["date"] and is_within_days(r["date"], days, r["promoted"]):
                writer.writerow(r)

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

def main():
    clear_screen()
    print("====================================")
    print("  SCRAPER FÜR reklama5.mk AUTOMOBILE ")
    print("====================================\n")

    print("Wähle die Seite zum Auslesen:")
    print("  1) reklama5")
    choice = input("Deine Wahl (Ziffer): ").strip()
    if choice != "1":
        print("Nur ‚reklama5‘ aktuell unterstützt. Programm beendet.")
        return

    search_term = input("Suchbegriff (z. B. „aygo“) eingeben (oder Enter für alle): ").strip()
    search_term = search_term if search_term else ""

    days_input = input("Wie viele Tage zurück sollen berücksichtigt werden? (Ganze Zahl): ").strip()
    try:
        days = int(days_input)
        if days <= 0:
            raise ValueError
    except ValueError:
        print("Ungültige Eingabe von Tagen. Programm beendet.")
        return

    limit_input = input("Wieviele Einträge sollen maximal eingelesen werden? (z. B. 999 = alle): ").strip()
    try:
        limit = int(limit_input)
        if limit <= 0:
            raise ValueError
    except ValueError:
        print("Ungültige Eingabe für Eintrags-Limit. Programm beendet.")
        return

    driver = init_driver()
    if os.path.isfile(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)

    total_saved = 0

    try:
        for page in range(1, 200):
            html     = fetch_page(driver, search_term, page)
            listings = parse_listing(html)

            if not listings:
                print(f"INFO: Keine Listings auf Seite {page} → Stop.")
                break

            save_raw_filtered(listings, days)
            saved_in_page = sum(1 for r in listings if r["date"] and is_within_days(r["date"], days, r["promoted"]))
            total_saved += saved_in_page
            print(f"INFO: Gespeichert {saved_in_page} neue Einträge (gesamt {total_saved}).")

            if limit != 999 and total_saved >= limit:
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

    agg = aggregate_data()
    print("\nINFO: Aggregation result:")
    for k, v in agg.items():
        if v["avg_price"] is not None:
            print(f"{k}: {v['count_total']} listings, {v['count_with_price']} mit Preis, avg price {v['avg_price']:.2f}")
        else:
            print(f"{k}: {v['count_total']} listings, {v['count_with_price']} mit Preis, avg price N/A")

if __name__ == "__main__":
    main()
