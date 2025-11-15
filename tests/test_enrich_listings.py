import sys
import time
import threading
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import scraperReklama5 as scraper


def test_enrich_listings_respects_max_items(monkeypatch):
    listings = [
        {"id": str(i), "link": f"http://example.com/{i}"}
        for i in range(1, 6)
    ]

    fetched_links = []

    def fake_fetch(link):
        fetched_links.append(link)
        return {"make": f"make_{link.rsplit('/', 1)[-1]}"}

    monkeypatch.setattr(scraper, "fetch_detail_attributes", fake_fetch)

    scraper.enrich_listings_with_details(listings, True, max_items=2)

    assert fetched_links == ["http://example.com/1", "http://example.com/2"]
    assert listings[0]["make"] == "make_1"
    assert listings[1]["make"] == "make_2"
    assert "make" not in listings[2]


def test_enrich_listings_parallel_preserves_order(monkeypatch):
    listings = [
        {"id": str(i), "link": f"http://example.com/{i}"}
        for i in range(1, 5)
    ]

    def fake_fetch(link):
        # Sleep in reverse order to ensure futures complete out of order
        delay = float(link.rsplit("/", 1)[-1]) * 0.01
        time.sleep(delay)
        return {"make": f"make_{link.rsplit('/', 1)[-1]}"}

    progress_calls = []

    monkeypatch.setattr(scraper, "fetch_detail_attributes", fake_fetch)

    scraper.enrich_listings_with_details(
        listings,
        True,
        max_items=None,
        progress_callback=lambda: progress_calls.append("done"),
        max_workers=3,
    )

    assert [item["make"] for item in listings] == [
        "make_1",
        "make_2",
        "make_3",
        "make_4",
    ]
    assert len(progress_calls) == 4


def test_enrich_listings_rate_limit_restricts_parallel_calls(monkeypatch):
    listings = [
        {"id": str(i), "link": f"http://example.com/{i}"}
        for i in range(1, 4)
    ]

    active_calls = 0
    max_active = 0
    lock = threading.Lock()

    def fake_fetch(link):
        nonlocal active_calls, max_active
        with lock:
            active_calls += 1
            max_active = max(max_active, active_calls)
        time.sleep(0.01)
        with lock:
            active_calls -= 1
        return {"make": f"make_{link.rsplit('/', 1)[-1]}"}

    monkeypatch.setattr(scraper, "fetch_detail_attributes", fake_fetch)

    scraper.enrich_listings_with_details(
        listings,
        True,
        max_workers=3,
        rate_limit_permits=1,
    )

    assert max_active == 1
