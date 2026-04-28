"""
Horry County SC — Motivated Seller Lead Scraper FINAL
CONFIRMED WORKING APPROACH:
- Navigate to Acclaim DocType search page
- Set date range + Select All doc types
- Click "Export to CSV" button
- Parse the downloaded CSV with known columns:
  Consideration, DirectName, IndirectName, BookPage, 
  RecordDate, DocTypeDescription, BookType, Comments, DeletedAfterVerify
"""

import asyncio
import csv
import io
import json
import logging
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

# Map DocTypeDescription values from CSV to our categories
# These are the EXACT strings from the CSV DocTypeDescription column
DOC_TYPE_MAP = {
    # Lis Pendens
    "LIS PENDENS":                    ("LP",      "Lis Pendens"),
    "LIS PENDENS RELEASE":            ("RELLP",   "Release Lis Pendens"),
    "RELEASE OF LIS PENDENS":         ("RELLP",   "Release Lis Pendens"),
    # Foreclosure
    "NOTICE OF FORECLOSURE":          ("NOFC",    "Notice of Foreclosure"),
    "FORECLOSURE":                    ("NOFC",    "Notice of Foreclosure"),
    # Tax Deed
    "TAX DEED":                       ("TAXDEED", "Tax Deed"),
    # Judgments
    "JUDGMENT":                       ("JUD",     "Judgment"),
    "CERTIFIED JUDGMENT":             ("JUD",     "Certified Judgment"),
    "DOMESTIC JUDGMENT":              ("JUD",     "Domestic Judgment"),
    "JUDGMENT LIEN":                  ("JUD",     "Judgment"),
    "FOREIGN JUDGMENT":               ("JUD",     "Judgment"),
    "TRANSCRIPT OF JUDGMENT":         ("JUD",     "Judgment"),
    # Tax Liens
    "STATE TAX LIEN":                 ("LN",      "State Tax Lien"),
    "FEDERAL TAX LIEN":               ("LN",      "Federal Tax Lien"),
    "IRS TAX LIEN":                   ("LN",      "IRS Lien"),
    "TAX LIEN":                       ("LN",      "Tax Lien"),
    "CORP TAX LIEN":                  ("LN",      "Corp Tax Lien"),
    "RELEASE OF STATE TAX LIEN":      ("RELLP",   "Release State Tax Lien"),
    "RELEASE OF FEDERAL TAX LIEN":    ("RELLP",   "Release Federal Tax Lien"),
    # Mechanic / HOA / Other Liens
    "MECHANIC LIEN":                  ("LN",      "Mechanic Lien"),
    "MECHANICS LIEN":                 ("LN",      "Mechanic Lien"),
    "MECHANIC'S LIEN":                ("LN",      "Mechanic Lien"),
    "HOA LIEN":                       ("LN",      "HOA Lien"),
    "HOMEOWNERS ASSOCIATION LIEN":    ("LN",      "HOA Lien"),
    "CONDOMINIUM LIEN":               ("LN",      "HOA Lien"),
    "CONDO LIEN":                     ("LN",      "HOA Lien"),
    "LIEN":                           ("LN",      "Lien"),
    "MEDICAID LIEN":                  ("LN",      "Medicaid Lien"),
    "HOSPITAL LIEN":                  ("LN",      "Medical Lien"),
    # Probate
    "PROBATE":                        ("PRO",     "Probate Document"),
    "LETTERS TESTAMENTARY":           ("PRO",     "Probate Document"),
    "LETTERS OF ADMINISTRATION":      ("PRO",     "Probate Document"),
    "WILL":                           ("PRO",     "Probate Document"),
    # Notice of Commencement
    "NOTICE OF COMMENCEMENT":         ("NOC",     "Notice of Commencement"),
}

# Also check if these keywords appear anywhere in the description
DOC_TYPE_KEYWORDS = [
    ("LIS PENDENS",         "LP",      "Lis Pendens"),
    ("FORECLOSURE",         "NOFC",    "Notice of Foreclosure"),
    ("TAX DEED",            "TAXDEED", "Tax Deed"),
    ("JUDGMENT",            "JUD",     "Judgment"),
    ("MECHANIC",            "LN",      "Mechanic Lien"),
    ("MECHANICS",           "LN",      "Mechanic Lien"),
    ("HOA LIEN",            "LN",      "HOA Lien"),
    ("CONDO LIEN",          "LN",      "HOA Lien"),
    ("TAX LIEN",            "LN",      "Tax Lien"),
    ("IRS LIEN",            "LN",      "IRS Lien"),
    ("FEDERAL LIEN",        "LN",      "Federal Lien"),
    ("MEDICAID LIEN",       "LN",      "Medicaid Lien"),
    ("HOSPITAL LIEN",       "LN",      "Medical Lien"),
    ("PROBATE",             "PRO",     "Probate Document"),
    ("NOTICE OF COMMENCEMENT","NOC",   "Notice of Commencement"),
]


def date_range_str():
    end   = datetime.now()
    start = end - timedelta(days=LOOK_BACK_DAYS)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")


def parse_amount(text):
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def normalize_date(raw):
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return str(raw).strip()[:10]


def classify_doc_type(description: str) -> Optional[tuple]:
    """Return (cat, cat_label) or None if not a target doc type."""
    desc = description.strip().upper()

    # Exact match first
    if desc in DOC_TYPE_MAP:
        return DOC_TYPE_MAP[desc]

    # Keyword match
    for keyword, cat, label in DOC_TYPE_KEYWORDS:
        if keyword in desc:
            return (cat, label)

    return None


def compute_flags(record):
    flags    = []
    cat      = record.get("cat", "")
    cat_label= record.get("cat_label", "")
    owner    = record.get("owner", "")
    filed    = record.get("filed", "")
    amount   = record.get("amount")

    if cat == "LP":     flags.append("Lis pendens")
    if cat == "NOFC":   flags.append("Pre-foreclosure")
    if cat == "JUD":    flags.append("Judgment lien")
    if "TAX" in cat_label.upper() or cat == "TAXDEED":
        flags.append("Tax lien")
    if "MECHANIC" in cat_label.upper():
        flags.append("Mechanic lien")
    if cat == "PRO":    flags.append("Probate / estate")
    if "HOA" in cat_label.upper() or "CONDO" in cat_label.upper():
        flags.append("HOA lien")
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
    if record.get("prop_address","").strip(): score += 5
    return min(score, 100)


def parse_csv(raw: str) -> list:
    """Parse the Acclaim CSV export with known column structure."""
    records = []
    try:
        # Remove BOM if present
        raw = raw.lstrip('\ufeff')
        reader = csv.DictReader(io.StringIO(raw))
        log.info("CSV columns: %s", reader.fieldnames)

        for row in reader:
            try:
                description = (row.get("DocTypeDescription","") or
                               row.get("Comments","") or "").strip()

                classified = classify_doc_type(description)
                if not classified:
                    continue

                cat, cat_label = classified

                book_page = row.get("BookPage","").strip()
                filed_raw = row.get("RecordDate","").strip()
                owner     = row.get("DirectName","").strip()
                grantee   = row.get("IndirectName","").strip()
                amount    = parse_amount(row.get("Consideration",""))
                comments  = row.get("Comments","").strip()

                # Build clerk URL from book/page
                if book_page:
                    parts = book_page.split("/")
                    if len(parts) == 2:
                        clerk_url = (f"{ACCLAIM_BASE}/search/BookPageSearchResult"
                                     f"?bookNumber={parts[0]}&pageNumber={parts[1]}")
                    else:
                        clerk_url = DOCTYPE_URL
                else:
                    clerk_url = DOCTYPE_URL

                records.append({
                    "doc_num":     book_page,
                    "doc_type":    cat,
                    "filed":       normalize_date(filed_raw),
                    "cat":         cat,
                    "cat_label":   cat_label,
                    "owner":       owner,
                    "grantee":     grantee,
                    "amount":      amount,
                    "legal":       comments,
                    "clerk_url":   clerk_url,
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
            except Exception as e:
                log.debug("Row error: %s", e)

    except Exception as e:
        log.error("CSV parse error: %s", e)

    log.info("Parsed %d target records from CSV", len(records))
    return records


class AcclaimScraper:

    def __init__(self):
        self.records = []

    async def scrape(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright unavailable")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-dev-shm-usage"]
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 1024},
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

        log.info("Total records collected: %d", len(self.records))
        return self.records

    async def _run(self, page: Page):
        start_date, end_date = date_range_str()
        log.info("Date range: %s to %s", start_date, end_date)

        # Load portal and accept disclaimer
        log.info("Loading portal...")
        await page.goto(ACCLAIM_BASE + "/", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)
        await self._accept_disclaimer(page)

        # Go to DocType search page
        log.info("Loading search page...")
        await page.goto(DOCTYPE_URL, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        log.info("URL: %s | Title: %s", page.url, await page.title())

        # Set date range dropdown to "Specify Date Range"
        await self._set_date_range_dropdown(page)

        # Fill From and To dates
        await self._fill_date(page, start_date, "from")
        await self._fill_date(page, end_date,   "to")

        # Select All doc types
        await self._select_all(page)

        # Click Search
        log.info("Clicking Search...")
        await self._click_search(page)
        await asyncio.sleep(5)
        log.info("Results page URL: %s", page.url)

        # Click Export to CSV
        log.info("Clicking Export to CSV...")
        csv_content = await self._export_csv(page)

        if csv_content:
            records = parse_csv(csv_content)
            self.records.extend(records)
            log.info("Got %d leads from CSV export", len(records))
        else:
            log.warning("CSV export failed — no data collected")

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

    async def _set_date_range_dropdown(self, page: Page):
        """Set the Date Range dropdown to 'Specify Date Range'."""
        for sel in [
            "select[name='DateRangeList']",
            "#DateRangeDropDown",
            "select",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    # Try selecting by visible text
                    for opt in ["Specify Date Range", "Specify Date Range...",
                                "Custom", "Date Range"]:
                        try:
                            await el.select_option(label=opt)
                            log.info("✓ Set date range dropdown to: %s", opt)
                            await asyncio.sleep(1)
                            return
                        except Exception:
                            pass
                    # Try selecting last option (usually "Specify Date Range")
                    options = await el.evaluate(
                        "el => Array.from(el.options).map(o => ({value:o.value,text:o.text}))"
                    )
                    log.info("Dropdown options: %s", options)
                    if options:
                        last = options[-1]
                        await el.select_option(value=last["value"])
                        log.info("✓ Selected last dropdown option: %s", last["text"])
                        await asyncio.sleep(1)
                    return
            except Exception as e:
                log.debug("Dropdown error: %s", e)

    async def _fill_date(self, page: Page, value: str, which: str):
        """Fill From or To date field."""
        if which == "from":
            selectors = ["#RecordDateFrom", "[name='RecordDateFrom']",
                         "input[id*='From']", "input[name*='From']"]
        else:
            selectors = ["#RecordDateTo", "[name='RecordDateTo']",
                         "input[id*='To']", "input[name*='To']"]

        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click(triple_click=True)
                    await el.fill(value)
                    await el.press("Tab")
                    log.info("✓ Filled %s date = %s via %s", which, value, sel)
                    return
            except Exception:
                pass
        log.warning("✗ Could not fill %s date", which)

    async def _select_all(self, page: Page):
        """Check SelectAll checkbox."""
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
        log.warning("Could not find SelectAll checkbox")

    async def _click_search(self, page: Page):
        """Click the Search button."""
        for sel in ["#btnSearch", "input[value='Search']",
                    "input[type='submit']", "button[type='submit']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    log.info("✓ Search clicked via %s", sel)
                    return
            except Exception:
                pass
        log.warning("Could not click Search")

    async def _export_csv(self, page: Page) -> Optional[str]:
        """Click Export to CSV and return file contents."""
        # From the screenshot we saw "Export to CSV" button
        for sel in [
            "input[value='Export to CSV']",
            "button:has-text('Export to CSV')",
            "a:has-text('Export to CSV')",
            "input[value*='Export' i]",
            "button:has-text('Export')",
            "a:has-text('Export')",
            "input[value*='CSV' i]",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    log.info("Found export button: %s", sel)
                    async with page.expect_download(timeout=30000) as dl_info:
                        await el.click()
                    download = await dl_info.value
                    path = await download.path()
                    if path:
                        with open(path, "r", encoding="utf-8-sig",
                                  errors="ignore") as f:
                            content = f.read()
                        log.info("✓ CSV downloaded: %d chars, %d lines",
                                 len(content), content.count("\n"))
                        return content
            except Exception as e:
                log.debug("Export %s failed: %s", sel, e)

        # Log all buttons on page for debugging
        buttons = await page.evaluate("""
            () => Array.from(document.querySelectorAll(
                'input[type=button],input[type=submit],button,a.btn'
            )).map(el => ({
                tag: el.tagName,
                type: el.type || '',
                value: el.value || el.innerText || '',
                id: el.id || ''
            }))
        """)
        log.warning("Could not find Export button. Buttons on page:")
        for b in buttons:
            log.warning("  %s[%s] id=%s value='%s'",
                        b['tag'], b['type'], b['id'], b['value'][:50])
        return None


def save_records_json(records, *paths):
    start_date, end_date = date_range_str()
    payload = {
        "fetched_at":   datetime.now().isoformat(),
        "source":       "Horry County Register of Deeds",
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address","")),
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
    log.info("Horry County Scraper — FINAL VERSION")
    log.info("Strategy: Export to CSV button")
    log.info("="*60)

    scraper = AcclaimScraper()
    raw     = await scraper.scrape()

    # Deduplicate
    seen, unique = set(), []
    for r in raw:
        key = (r.get("doc_num",""), r.get("cat",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique records: %d", len(unique))

    # Score
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
    log.info("✓ Done. Total leads: %d", len(unique))


if __name__ == "__main__":
    asyncio.run(main())
