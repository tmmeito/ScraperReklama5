import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import scraperReklama5 as scraper


def test_cli_entry_triggers_non_interactive_run(monkeypatch, tmp_path):
    html_calls = []

    def fake_fetch(search_term, page_num):
        html_calls.append((search_term, page_num))
        if page_num == 1:
            return "<html></html>"
        return None

    monkeypatch.setattr(scraper, "fetch_listing_page", fake_fetch)

    def fake_parse(_html):
        return [
            {
                "id": "abc",
                "link": "http://example.com/1",
                "date": "dummy",
                "promoted": False,
            }
        ]

    monkeypatch.setattr(scraper, "parse_listing", fake_parse)
    monkeypatch.setattr(scraper, "is_within_days", lambda *_, **__: True)
    monkeypatch.setattr(scraper, "is_older_than_days", lambda *_, **__: False)
    monkeypatch.setattr(scraper, "enrich_listings_with_details", lambda *_, **__: None)

    saved = {}

    def fake_save(
        rows,
        days,
        limit=None,
        csv_filename=None,
        pre_filtered=False,
        *,
        db_connection=None,
    ):
        saved["rows"] = rows
        saved["days"] = days
        saved["limit"] = limit
        saved["csv"] = csv_filename
        saved["pre_filtered"] = pre_filtered
        saved["db_connection"] = db_connection
        return len(rows)

    monkeypatch.setattr(scraper, "save_raw_filtered", fake_save)
    aggregate_calls = []

    def fake_aggregate(*args, **kwargs):
        aggregate_calls.append(kwargs)
        return {}

    monkeypatch.setattr(scraper, "aggregate_data", fake_aggregate)

    analysis_called = False

    def fake_analysis(*_, **__):
        nonlocal analysis_called
        analysis_called = True
        return "exit"

    monkeypatch.setattr(scraper, "analysis_menu", fake_analysis)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)

    db_path = tmp_path / "cars.db"
    scraper.sqlite_store.DEFAULT_DB_PATH = str(db_path)
    result = scraper.main(
        [
            "--search",
            "aygo",
            "--days",
            "2",
            "--limit",
            "1",
            "--details",
            "--details-delay",
            "0.5",
            "--use-sqlite",
        ]
    )

    assert saved["db_connection"] is not None
    assert saved["limit"] == 1
    assert result["total_saved"] == 1
    assert result["db_path"] == str(db_path)
    assert analysis_called is False
    assert html_calls[0][0] == "aygo"
    assert aggregate_calls and aggregate_calls[0].get("db_path") == str(db_path)


def test_cli_details_delay_zero_propagates_none(monkeypatch, tmp_path):
    html_calls = []

    def fake_fetch(search_term, page_num):
        html_calls.append((search_term, page_num))
        if page_num == 1:
            return "<html></html>"
        return None

    monkeypatch.setattr(scraper, "fetch_listing_page", fake_fetch)

    def fake_parse(_html):
        return [
            {
                "id": "abc",
                "link": "http://example.com/1",
                "date": "dummy",
                "promoted": False,
            }
        ]

    monkeypatch.setattr(scraper, "parse_listing", fake_parse)
    monkeypatch.setattr(scraper, "is_within_days", lambda *_, **__: True)
    monkeypatch.setattr(scraper, "is_older_than_days", lambda *_, **__: False)

    captured_delay = {}

    def fake_enrich(listings, enabled, delay_range=None, **kwargs):
        captured_delay["enabled"] = enabled
        captured_delay["delay_range"] = delay_range

    monkeypatch.setattr(scraper, "enrich_listings_with_details", fake_enrich)

    saved = {}

    def fake_save(
        rows,
        days,
        limit=None,
        csv_filename=None,
        pre_filtered=False,
        *,
        db_connection=None,
    ):
        saved["rows"] = rows
        saved["days"] = days
        saved["limit"] = limit
        saved["csv"] = csv_filename
        saved["pre_filtered"] = pre_filtered
        saved["db_connection"] = db_connection
        return len(rows)

    monkeypatch.setattr(scraper, "save_raw_filtered", fake_save)
    monkeypatch.setattr(scraper, "aggregate_data", lambda **_: {})
    monkeypatch.setattr(scraper, "analysis_menu", lambda *_, **__: "exit")
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)

    db_path = tmp_path / "cars.db"
    scraper.sqlite_store.DEFAULT_DB_PATH = str(db_path)
    scraper.main(
        [
            "--search",
            "aygo",
            "--days",
            "2",
            "--limit",
            "1",
            "--details",
            "--details-delay",
            "0",
            "--use-sqlite",
        ]
    )

    assert html_calls[0][0] == "aygo"
    assert captured_delay["enabled"] is True
    assert captured_delay["delay_range"] is None
    assert saved["db_connection"] is not None
