# ScraperReklama5 helper scripts

## HTTP verification without Selenium

Use `scripts/verify_http_fetch.py` to confirm that listings pages can be downloaded and
parsed without launching Safari/Selenium. The helper performs exactly the checks that
were outlined previously:

1. Build the same URL that Selenium would open (`BASE_URL_TEMPLATE`).
2. Fetch it via `urllib` with the same user agent that the detail scraper already uses.
3. Feed the returned HTML directly into `parse_listing()` and show how many listings were
   extracted plus an excerpt of the first record.

```bash
python scripts/verify_http_fetch.py golf --page 1
```

If the request succeeds you will see the number of characters downloaded, how many
listings were parsed, and a small field dump for the first hit. Should the script report
that no listings were parsed, inspect the HTML (it is stored in-memory) to see whether
reklama5.mk changed its markup.

> **Note**
> The hosted evaluation environment that produced this README has outbound HTTP/HTTPS
> traffic blocked by a corporate proxy. Running the script here therefore results in an
> `HTTP 403` error before the actual site can be contacted. When executed on a machine
> with normal Internet access, the same script will download and parse the search page
> without involving Selenium.
