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

    def fake_save(rows, days, limit=None, csv_filename=None):
        saved["rows"] = rows
        saved["days"] = days
        saved["limit"] = limit
        saved["csv"] = csv_filename
        return len(rows)

    monkeypatch.setattr(scraper, "save_raw_filtered", fake_save)
    monkeypatch.setattr(scraper, "aggregate_data", lambda **_: {})

    analysis_called = False

    def fake_analysis(*_, **__):
        nonlocal analysis_called
        analysis_called = True
        return "exit"

    monkeypatch.setattr(scraper, "analysis_menu", fake_analysis)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)

    csv_path = tmp_path / "cars.csv"
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
            "--csv",
            str(csv_path),
        ]
    )

    assert saved["csv"] == str(csv_path)
    assert saved["limit"] == 1
    assert result["total_saved"] == 1
    assert result["csv_filename"] == str(csv_path)
    assert analysis_called is False
    assert html_calls[0][0] == "aygo"
