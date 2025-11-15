import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import scraperReklama5 as scraper
from storage import sqlite_store


def _make_listing(listing_id, date_text="2024-01-01 12:00"):
    row = {field: None for field in scraper.CSV_FIELDNAMES}
    row.update(
        {
            "id": listing_id,
            "link": f"http://example.com/{listing_id}",
            "make": "Test",
            "model": f"Model {listing_id}",
            "price": 1000,
            "date": date_text,
            "promoted": False,
        }
    )
    return row


def test_run_scraper_filters_duplicate_ids(monkeypatch, tmp_path):
    db_path = tmp_path / "cars.db"
    scraper.sqlite_store.DEFAULT_DB_PATH = str(db_path)
    html_pages = {1: "page-1", 2: "page-2", 3: "page-empty"}

    def fake_fetch(search_term, page_num, retries=3, backoff_seconds=2):
        return html_pages.get(page_num)

    listings_by_html = {
        "page-1": [_make_listing("1"), _make_listing("2")],
        "page-2": [_make_listing("2"), _make_listing("3")],
    }

    def fake_parse(html):
        return listings_by_html.get(html, [])

    monkeypatch.setattr(scraper, "fetch_listing_page", fake_fetch)
    monkeypatch.setattr(scraper, "parse_listing", fake_parse)
    monkeypatch.setattr(scraper, "is_within_days", lambda *_, **__: True)
    monkeypatch.setattr(scraper, "is_older_than_days", lambda *_, **__: False)
    monkeypatch.setattr(scraper, "enrich_listings_with_details", lambda *_, **__: None)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)

    aggregate_calls = []

    def fake_aggregate(*args, **kwargs):
        aggregate_calls.append(kwargs)
        return {}

    monkeypatch.setattr(scraper, "aggregate_data", fake_aggregate)

    config = scraper.ScraperConfig(
        search_term="test",
        days=5,
        enable_detail_capture=False,
        db_path=str(db_path),
    )

    result = scraper.run_scraper_flow_from_config(config, interactive=False)
    assert result["total_saved"] == 3
    assert result["db_path"] == str(db_path)
    assert aggregate_calls and aggregate_calls[0].get("db_path") == str(db_path)

    conn = sqlite_store.open_database(str(db_path))
    rows = conn.execute("SELECT id FROM listings ORDER BY id").fetchall()
    conn.close()
    ids = [row["id"] for row in rows]

    assert ids == ["1", "2", "3"]


def test_run_scraper_pre_filtered_saves_skip_extra_filter(monkeypatch, tmp_path):
    db_path = tmp_path / "cars.db"
    scraper.sqlite_store.DEFAULT_DB_PATH = str(db_path)
    html_pages = {1: "page-1"}

    def fake_fetch(search_term, page_num, retries=3, backoff_seconds=2):
        return html_pages.get(page_num)

    listings_by_html = {
        "page-1": [_make_listing("1"), _make_listing("2")],
    }

    def fake_parse(html):
        return listings_by_html.get(html, [])

    call_counter = {"count": 0}

    def counting_is_within_days(*args, **kwargs):
        call_counter["count"] += 1
        return True

    monkeypatch.setattr(scraper, "fetch_listing_page", fake_fetch)
    monkeypatch.setattr(scraper, "parse_listing", fake_parse)
    monkeypatch.setattr(scraper, "is_within_days", counting_is_within_days)
    monkeypatch.setattr(scraper, "is_older_than_days", lambda *_, **__: False)
    monkeypatch.setattr(scraper, "enrich_listings_with_details", lambda *_, **__: None)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)
    monkeypatch.setattr(scraper, "aggregate_data", lambda **_: {})

    config = scraper.ScraperConfig(
        search_term="test",
        days=5,
        enable_detail_capture=False,
        db_path=str(db_path),
    )

    scraper.run_scraper_flow_from_config(config, interactive=False)

    assert call_counter["count"] == len(listings_by_html["page-1"])
