"""
Horry County SC — Motivated Seller Lead Scraper v8
Key fix: Acclaim renders results via JavaScript grid (not static HTML tables).
Strategy: Wait for JS grid to load, then extract data from the rendered DOM.
Also tries the CSV export button which bypasses the JS grid entirely.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("horry_scraper")

ACCLAIM_BASE   = "https://acclaimweb.horrycounty.org/AcclaimWeb"
DOCTYPE_URL    = f"{ACCLAIM_BASE}/search/SearchTypeDocType"
LOOK_BACK_DAYS = 7
SCRAPER_TIMEOUT = 18 * 60

DOC_CATEGORIES = {
    "LP":       ("LP",      "Lis Pendens"),
    "NOFC":     ("NOFC",    "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED", "Tax Deed"),
    "JUD":      ("JUD",     "Judgment"),
    "CCJ":      ("JUD",     "Certified Judgment"),
    "DRJUD":    ("JUD",     "Domestic Judgment"),
    "LNCORPTX": ("LN",      "Corp Tax Lien"),
    "LNIRS":    ("LN",      "IRS Lien"),
    "LNFED":    ("LN",      "Federal Lien"),
    "LN":       ("LN",      "Lien"),
    "LNMECH":   ("LN",      "Mechanic Lien"),
    "LNHOA":    ("LN",      "HOA Lien"),
    "MEDLN":    ("LN",      "Medicaid Lien"),
    "PRO":      ("PRO",     "Probate Document"),
    "NOC":      ("NOC",     "Notice of Commencement"),
    "RELLP":    ("RELLP",   "Release Lis Pendens"),
}


def date_range_str():
    end   = datetime.now()
    start = end - timedelta(days=LOOK_BACK_DAYS)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")


def parse_amount(text):
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def normalize_date(raw):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return str(raw).strip()


def compute_flags(record):
    flags    = []
    cat      = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    owner    = record.get("owner", "")
    filed    = record.get("filed", "")
    if doc_type == "LP":     flags.append("Lis pendens")
    if doc_type == "NOFC":   flags.append("Pre-foreclosure")
    if cat == "JUD":         flags.append("Judgment lien")
    if doc_type in ("LNIRS","LNCORPTX","LNFED","TAXDEED"): flags.append("Tax lien")
    if doc_type == "LNMECH": flags.append("Mechanic lien")
    if doc_type == "PRO":    flags.append("Probate / estate")
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|HOLDINGS)\b", owner.upper()):
        flags.append("LLC / corp owner")
    try:
        if (datetime.now() - datetime.strptime(filed, "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:
        pass
    return list(dict.fromkeys(flags))


def compute_score(record, flags):
    score = 30
    score += len(flags) * 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: score += 20
    amount = record.get("amount")
    if amount:
        if amount > 100_000: score += 15
        elif amount > 50_000: score += 10
    if "New this week" in flags: score += 5
    if record.get("prop_address", "").strip(): score += 5
    return min(score, 100)


class AcclaimScraper:

    def __init__(self):
        self.records    = []
        self.start_time = None

    def timed_out(self):
        return (datetime.now().timestamp() - self.start_time) > SCRAPER_TIMEOUT

    async def scrape(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright unavailable")
            return []

        self.start_time = datetime.now().timestamp()

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 1024},
                # Accept downloads for CSV export attempt
                accept_downloads=True,
            )
            page = await context.new_page()
            page.set_default_timeout(20000)

            try:
                await self._run(page)
            except Exception as exc:
                log.error("Scraper error: %s", exc, exc_info=True)
            finally:
                await browser.close()

        log.info("Collected %d raw records", len(self.records))
        return self.records

    async def _run(self, page: Page):
        start_date, end_date = date_range_str()
        log.info("Date range: %s to %s", start_date, end_date)

        # Step 1: Load portal and accept disclaimer
        log.info("Loading portal...")
        await page.goto(ACCLAIM_BASE + "/", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)
        await self._accept_disclaimer(page)
        log.info("URL after disclaimer: %s", page.url)

        # Step 2: Go to Document Type search page
        log.info("Loading search page...")
        await page.goto(DOCTYPE_URL, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        log.info("Search page URL: %s", page.url)

        # Step 3: Fill dates
        await self._fill_field(page, "RecordDateFrom", start_date)
        await self._fill_field(page, "RecordDateTo",   end_date)

        # Step 4: Select all doc types
        await self._select_all(page)

        # Step 5: Submit and wait for JS grid to render
        log.info("Submitting search...")
        await self._submit_and_wait(page)

        # Step 6: Try to get CSV export first (most reliable)
        csv_records = await self._try_csv_export(page)
        if csv_records:
            log.info("Got %d records from CSV export", len(csv_records))
            self.records.extend(csv_records)
            return

        # Step 7: Parse JS-rendered grid
        log.info("Parsing JS grid...")
        await self._parse_js_grid(page)

    async def _accept_disclaimer(self, page: Page):
        content = await page.content()
        if "disclaimer" in content.lower() or "accept" in content.lower():
            for sel in ["input[type='submit']", "input[value*='Accept' i]",
                        "button:has-text('Accept')"]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click()
                        await page.wait_for_load_state("domcontentloaded", timeout=10000)
                        await asyncio.sleep(1)
                        log.info("Disclaimer accepted")
                        return
                except Exception:
                    pass

    async def _fill_field(self, page: Page, field_id: str, value: str):
        for sel in [f"#{field_id}", f"[name='{field_id}']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click(triple_click=True)
                    await el.fill(value)
                    log.info("✓ Filled %s = %s", field_id, value)
                    return
            except Exception:
                pass
        log.warning("✗ Could not fill %s", field_id)

    async def _select_all(self, page: Page):
        for sel in ["#Checkbox1", "[name='SelectAllDocTypesToggle']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.check()
                    await asyncio.sleep(1)
                    log.info("✓ Selected all doc types")
                    return
            except Exception:
                pass
        # Fallback: check individual boxes
        try:
            cbs = await page.query_selector_all("[name='DocTypeInfoCheckBox']")
            log.info("Checking %d checkboxes", len(cbs))
            for cb in cbs[:100]:
                try:
                    if not await cb.is_checked():
                        await cb.check()
                except Exception:
                    pass
        except Exception as e:
            log.warning("Checkbox error: %s", e)

    async def _submit_and_wait(self, page: Page):
        """Submit form and wait for JS grid to fully render."""
        for sel in ["#btnSearch", "input[type='submit']", "button[type='submit']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    # Wait for network to settle after JS loads results
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    await asyncio.sleep(5)  # Extra wait for JS grid rendering
                    log.info("✓ Search submitted. URL: %s", page.url)
                    return
            except Exception:
                pass
        log.error("Could not submit search")

    async def _try_csv_export(self, page: Page) -> list:
        """
        Acclaim has a CSV export button on the results page.
        This is the most reliable way to get all data.
        """
        try:
            # Look for export/CSV button
            for sel in [
                "button:has-text('Export')",
                "button:has-text('CSV')",
                "a:has-text('Export')",
                "a:has-text('CSV')",
                "input[value*='Export' i]",
                "input[value*='CSV' i]",
                ".export-btn",
                "#btnExport",
                "#btnCSV",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        log.info("Found export button: %s", sel)
                        async with page.expect_download(timeout=15000) as dl_info:
                            await el.click()
                        download = await dl_info.value
                        content  = await download.path()
                        if content:
                            with open(content, "r", encoding="utf-8", errors="ignore") as f:
                                raw = f.read()
                            records = self._parse_csv_content(raw)
                            log.info("CSV export: %d records", len(records))
                            return records
                except Exception as e:
                    log.debug("Export attempt %s failed: %s", sel, e)
        except Exception as e:
            log.debug("CSV export overall failed: %s", e)
        return []

    def _parse_csv_content(self, raw: str) -> list:
        """Parse CSV content from Acclaim export."""
        records = []
        try:
            reader = csv.DictReader(io.StringIO(raw))
            for row in reader:
                # Map CSV columns to our format
                doc_type = (row.get("Doc Type","") or row.get("Document Type","") or "").strip().upper()
                if doc_type not in DOC_CATEGORIES:
                    continue
                cat, cat_label = DOC_CATEGORIES[doc_type]
                records.append({
                    "doc_num":     (row.get("Instrument","") or row.get("Doc Number","") or "").strip(),
                    "doc_type":    doc_type,
                    "filed":       normalize_date(row.get("Record Date","") or row.get("Date","")),
                    "cat":         cat,
                    "cat_label":   cat_label,
                    "owner":       (row.get("Grantor","") or row.get("Owner","")).strip(),
                    "grantee":     (row.get("Grantee","") or "").strip(),
                    "amount":      parse_amount(row.get("Consideration","") or row.get("Amount","")),
                    "legal":       (row.get("Legal","") or row.get("Description","")).strip(),
                    "clerk_url":   DOCTYPE_URL,
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
        except Exception as e:
            log.error("CSV parse error: %s", e)
        return records

    async def _parse_js_grid(self, page: Page):
        """
        Acclaim uses a JavaScript grid. Extract data directly from the DOM
        using JavaScript evaluation — more reliable than BeautifulSoup for JS-rendered content.
        """
        # Log the full page structure for debugging
        page_text = await page.evaluate("() => document.body.innerText")
        log.info("Page text preview (first 500 chars): %s", page_text[:500])

        # Try to extract grid rows via JavaScript
        rows = await page.evaluate("""
            () => {
                const results = [];

                // Try standard table rows
                const tableRows = document.querySelectorAll('table tbody tr, table tr');
                tableRows.forEach(row => {
                    const cells = Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim());
                    if (cells.length >= 3) results.push({type: 'table', cells: cells});
                });

                // Try grid rows (common in Acclaim)
                const gridRows = document.querySelectorAll(
                    '.k-grid-content tr, .grid-row, [role="row"], .search-result-row, .result-row'
                );
                gridRows.forEach(row => {
                    const cells = Array.from(row.querySelectorAll('td, [role="gridcell"], .cell'))
                        .map(td => td.innerText.trim());
                    if (cells.length >= 3) results.push({type: 'grid', cells: cells});
                });

                // Try list items
                const listItems = document.querySelectorAll('.search-results li, .results-list li');
                listItems.forEach(item => {
                    results.push({type: 'list', cells: [item.innerText.trim()]});
                });

                return results;
            }
        """)

        log.info("JS extraction found %d row candidates", len(rows))

        # Also get all links on the page (each result usually has a link)
        links = await page.evaluate("""
            () => Array.from(document.querySelectorAll('a[href]'))
                .map(a => ({text: a.innerText.trim(), href: a.href}))
                .filter(a => a.text.length > 0)
        """)
        log.info("Found %d links on results page", len(links))
        for link in links[:20]:
            log.info("  Link: %s → %s", link['text'][:50], link['href'][:80])

        # Parse whatever rows we found
        for row_data in rows:
            cells = row_data.get("cells", [])
            if len(cells) < 2:
                continue
            try:
                rec = self._parse_row_cells(cells)
                if rec:
                    self.records.append(rec)
            except Exception as e:
                log.debug("Row parse error: %s", e)

        log.info("Parsed %d records from JS grid", len(self.records))

        # If still 0 — try paginating through results using URL patterns
        if len(self.records) == 0:
            await self._try_url_pagination(page)

    async def _try_url_pagination(self, page: Page):
        """
        Try to access results via direct URL patterns that Acclaim uses.
        Some Acclaim installs use /search/SearchResults with query params.
        """
        start_date, end_date = date_range_str()

        # Try common Acclaim result URL patterns
        result_urls = [
            f"{ACCLAIM_BASE}/search/SearchResults",
            f"{ACCLAIM_BASE}/Search/GetSearchResults",
            f"{ACCLAIM_BASE}/api/search/results",
        ]

        for url in result_urls:
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                if resp and resp.status == 200:
                    content = await page.content()
                    log.info("Result URL %s returned %d chars", url, len(content))
                    soup = BeautifulSoup(content, "lxml")
                    # Try parsing as JSON
                    try:
                        data = json.loads(content)
                        log.info("Got JSON response with keys: %s", list(data.keys())[:5])
                    except Exception:
                        pass
            except Exception:
                pass

    def _parse_row_cells(self, cells: list) -> Optional[dict]:
        """Parse a row of cells into a record."""
        if len(cells) < 3:
            return None

        # Try to identify which cell is which based on content patterns
        doc_num  = cells[0] if cells else ""
        doc_type = cells[1].strip().upper() if len(cells) > 1 else ""
        filed    = ""
        grantor  = ""
        grantee  = ""
        amount   = ""
        legal    = ""

        # Find date cell
        for i, c in enumerate(cells):
            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', c):
                filed = c
                grantor = cells[i+1] if i+1 < len(cells) else ""
                grantee = cells[i+2] if i+2 < len(cells) else ""
                legal   = cells[i+3] if i+3 < len(cells) else ""
                amount  = cells[i+4] if i+4 < len(cells) else ""
                break

        if doc_type not in DOC_CATEGORIES:
            return None

        cat, cat_label = DOC_CATEGORIES[doc_type]

        return {
            "doc_num":     doc_num.strip(),
            "doc_type":    doc_type,
            "filed":       normalize_date(filed),
            "cat":         cat,
            "cat_label":   cat_label,
            "owner":       grantor.strip(),
            "grantee":     grantee.strip(),
            "amount":      parse_amount(amount),
            "legal":       legal.strip(),
            "clerk_url":   DOCTYPE_URL,
            "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
            "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
        }


def save_records_json(records, *paths):
    start_date, end_date = date_range_str()
    payload = {
        "fetched_at":   datetime.now().isoformat(),
        "source":       "Horry County Register of Deeds",
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(records),
        "with_address": 0,
        "records":      records,
    }
    for path in paths:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Saved → %s (%d records)", path, len(records))


def export_ghl_csv(records, path):
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City",
        "Mailing State","Mailing Zip","Property Address","Property City",
        "Property State","Property Zip","Lead Type","Document Type",
        "Date Filed","Document Number","Amount/Debt Owed","Seller Score",
        "Motivated Seller Flags","Source","Public Records URL",
    ]
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            owner = r.get("owner","")
            parts = owner.split(" ",1) if owner else ["",""]
            writer.writerow({
                "First Name":            parts[0],
                "Last Name":             parts[1] if len(parts)>1 else "",
                "Mailing Address":       r.get("mail_address",""),
                "Mailing City":          r.get("mail_city",""),
                "Mailing State":         r.get("mail_state","SC"),
                "Mailing Zip":           r.get("mail_zip",""),
                "Property Address":      r.get("prop_address",""),
                "Property City":         r.get("prop_city",""),
                "Property State":        r.get("prop_state","SC"),
                "Property Zip":          r.get("prop_zip",""),
                "Lead Type":             r.get("cat_label",""),
                "Document Type":         r.get("doc_type",""),
                "Date Filed":            r.get("filed",""),
                "Document Number":       r.get("doc_num",""),
                "Amount/Debt Owed":      r.get("amount",""),
                "Seller Score":          r.get("score",""),
                "Motivated Seller Flags":"; ".join(r.get("flags",[])),
                "Source":                "Horry County Register of Deeds",
                "Public Records URL":    r.get("clerk_url",""),
            })
    log.info("GHL CSV → %s (%d rows)", path, len(records))


async def main():
    log.info("="*60)
    log.info("Horry County Scraper v8 — JS Grid + CSV Export")
    log.info("="*60)

    scraper = AcclaimScraper()
    raw     = await scraper.scrape()

    seen, unique = set(), []
    for r in raw:
        key = (r.get("doc_num",""), r.get("doc_type",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique after dedup: %d", len(unique))

    for r in unique:
        flags     = compute_flags(r)
        r["flags"]= flags
        r["score"]= compute_score(r, flags)
    unique.sort(key=lambda r: r.get("score",0), reverse=True)

    repo = Path(__file__).parent.parent
    save_records_json(unique,
        str(repo/"dashboard"/"records.json"),
        str(repo/"data"/"records.json"),
    )
    export_ghl_csv(unique, str(repo/"data"/"leads_export.csv"))
    log.info("Done. Total leads: %d", len(unique))


if __name__ == "__main__":
    asyncio.run(main())
