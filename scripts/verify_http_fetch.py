"""Helper script to verify that listings pages can be downloaded without Selenium."""

from __future__ import annotations

import argparse
import sys
from typing import Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request

# Ensure the scraper module can be imported when the script is executed
sys.path.append("src")
import scraperReklama5 as scraper  # type: ignore  # pylint: disable=import-error

USER_AGENT = "Mozilla/5.0 (compatible; reklama5-scraper/1.0)"


def fetch_page_html(search_term: str, page: int) -> Tuple[str, str]:
    """Download a search results page using urllib."""

    url = scraper.BASE_URL_TEMPLATE.format(search_term=search_term, page_num=page)
    headers = {"User-Agent": USER_AGENT}
    req = urllib_request.Request(url, headers=headers)
    with urllib_request.urlopen(req, timeout=20) as response:  # nosec - CLI helper
        encoding = response.headers.get_content_charset() or "utf-8"
        html = response.read().decode(encoding, errors="replace")
    return url, html


def run_verification(search_term: str, page: int) -> None:
    print(f"Requesting listings for '{search_term}' (page {page})...")
    try:
        url, html = fetch_page_html(search_term, page)
    except (urllib_error.URLError, urllib_error.HTTPError) as exc:
        print("❌  Request failed:", exc)
        return

    print(f"✅  Successfully fetched {len(html)} characters from {url}")
    listings = scraper.parse_listing(html)
    print(f"Parsed {len(listings)} listings via BeautifulSoup")
    if listings:
        first = listings[0]
        print("First listing excerpt:")
        for key in ("id", "make", "model", "price", "km", "kw", "ps", "date"):
            print(f"  {key:>5}: {first.get(key)}")
    else:
        print("No listings parsed – check whether the HTML structure has changed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download a reklama5.mk search page via urllib and parse it without Selenium."
        )
    )
    parser.add_argument("search_term", help="Search term to query, e.g. 'golf'")
    parser.add_argument(
        "--page", type=int, default=1, help="Page number to fetch (default: 1)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_verification(args.search_term, args.page)
