import sys
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
