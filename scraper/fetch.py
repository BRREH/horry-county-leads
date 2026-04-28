"""
Horry County SC — Motivated Seller Lead Scraper v11
CONFIRMED from logs:
- Dropdown field name: DocTypesDisplay / DocTypeGroupDropDown
- Date fields need to be found differently (dropdown takes 2+ min to load)
- Export to CSV button only appears AFTER search results load
- Doc type codes confirmed from dropdown options in log

KEY INSIGHT: Select specific doc type GROUP values directly,
fill dates via JavaScript injection (bypasses DOM timing issues),
then search and export CSV.
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

# Doc type codes from the dropdown log — LIENS group contains our targets
# Value string contains all codes for that group
LIENS_VALUE = (
    "AFFIDAVIT - LIEN BOOK (109),TAX LIENS - FEDERAL CHARGE (112),"
    "FEDERAL TAX SATISFACTION (117),STATE TAX PARTIAL RELEASE (119),"
    "FEDERAL TAX WITHDRAWAL (120),FEDERAL TAX PARTIAL RELEASE (122),"
    "STATE TAX WITHDRAWAL (123),LIEN SATISFACTION RESCISSION (124),"
    "REVOCATION OF RELEASE (126),CHILD SUPPORT LIEN (031),"
    "MECHANICS & CONDO LIEN SATISFACTION (035),MECHANICS LIEN SATISFACTION (042),"
    "TAX LIENS - STATE (084),TAX SATISFACTION (092),TAX LIENS - FEDERAL (093),"
    "TAX LIEN AMENDMENT (106),UCC3: 1-2 PAGES, 1-2 DEBTORS (203),"
    "UCC3: 2+ PAGES, 1-2 DEBTORS (204),CHILD SUPPORT LIEN SATISFACTION (097),"
    "UCC3: TERMINATION LIEN BK (210),MISC - LIEN (115),"
    "MISC - LIEN SATISFACTION (116),MECHANICS LIEN SURETY BOND (094),"
    "MECHANICS LIEN AMENDMENT (098),MENTAL HEALTH AMENDMENT (099),"
    "CONDO LIEN - PARTIAL RELEASE (110),MECHANICS LIEN (015),"
    "MENTAL HEALTH LIEN (029),CONTRACTORS NOTICE OF PROJECT (034),"
    "CONDO LIEN SATISFACTION (036),CONDO LIEN (064),"
    "MECHANICS LIEN - PARTIAL RELEASE (103),"
    "MECHANICS LIEN - SEP. AFF. OF SERVICE (104),"
    "CONDO LIEN - AMENDMENT (105),MENTAL HEALTH LIEN SATISFACTION (095)"
    "|85,86,87,88,89,90,91,92,93,94,95,96,99,100,101,102,103,104,105,106,"
    "107,108,181,182,183,184,185,186,187,188,189,190,191,192,193"
)

# Map DocTypeDescription to our categories
DOC_TYPE_KEYWORDS = [
    ("LIS PENDENS",           "LP",      "Lis Pendens"),
    ("FORECLOSURE",           "NOFC",    "Notice of Foreclosure"),
    ("TAX DEED",              "TAXDEED", "Tax Deed"),
    ("JUDGMENT",              "JUD",     "Judgment"),
    ("MECHANIC",              "LN",      "Mechanic Lien"),
    ("CONDO LIEN",            "LN",      "HOA/Condo Lien"),
    ("HOA LIEN",              "LN",      "HOA Lien"),
    ("TAX LIEN",              "LN",      "Tax Lien"),
    ("TAX LIENS",             "LN",      "Tax Lien"),
    ("IRS LIEN",              "LN",      "IRS Lien"),
    ("FEDERAL TAX",           "LN",      "Federal Tax Lien"),
    ("STATE TAX",             "LN",      "State Tax Lien"),
    ("CHILD SUPPORT LIEN",    "LN",      "Child Support Lien"),
    ("MENTAL HEALTH LIEN",    "LN",      "Mental Health Lien"),
    ("MEDICAID LIEN",         "LN",      "Medicaid Lien"),
    ("HOSPITAL LIEN",         "LN",      "Medical Lien"),
    ("PROBATE",               "PRO",     "Probate Document"),
    ("NOTICE OF COMMENCEMENT","NOC",     "Notice of Commencement"),
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


def classify_doc(description: str) -> Optional[tuple]:
    desc = description.strip().upper()
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

    if cat == "LP":                          flags.append("Lis pendens")
    if cat == "NOFC":                        flags.append("Pre-foreclosure")
    if cat == "JUD":                         flags.append("Judgment lien")
    if "TAX" in cat_label.upper():           flags.append("Tax lien")
    if "MECHANIC" in cat_label.upper():      flags.append("Mechanic lien")
    if cat == "PRO":                         flags.append("Probate / estate")
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
    records = []
    try:
        raw = raw.lstrip('\ufeff')
        reader = csv.DictReader(io.StringIO(raw))
        log.info("CSV columns: %s", reader.fieldnames)
        rows = list(reader)
        log.info("CSV total rows: %d", len(rows))

        for row in rows:
            try:
                description = (row.get("DocTypeDescription","") or "").strip()
                comments    = (row.get("Comments","") or "").strip()

                classified = classify_doc(description)
                if not classified:
                    # Also check Comments field
                    classified = classify_doc(comments)
                if not classified:
                    continue

                cat, cat_label = classified
                book_page = (row.get("BookPage","") or "").strip()
                filed_raw = (row.get("RecordDate","") or "").strip()
                owner     = (row.get("DirectName","") or "").strip()
                grantee   = (row.get("IndirectName","") or "").strip()
                amount    = parse_amount(row.get("Consideration",""))

                if book_page:
                    parts = book_page.split("/")
                    if len(parts) == 2:
                        clerk_url = (
                            f"{ACCLAIM_BASE}/search/BookPageSearchResult"
                            f"?bookNumber={parts[0]}&pageNumber={parts[1]}"
                        )
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

    log.info("Classified %d target records", len(records))
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

        log.info("Total records: %d", len(self.records))
        return self.records

    async def _run(self, page: Page):
        start_date, end_date = date_range_str()
        log.info("Date range: %s to %s", start_date, end_date)

        # Step 1: Load portal + accept disclaimer
        log.info("Loading portal...")
        await page.goto(ACCLAIM_BASE + "/", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)
        await self._accept_disclaimer(page)

        # Step 2: Load search page and wait for it to fully initialize
        log.info("Loading search page...")
        await page.goto(DOCTYPE_URL, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        log.info("Page loaded: %s", page.url)

        # Step 3: Select "All" from the doc type group dropdown
        # From logs we know the dropdown has value 'All|all'
        await self._select_all_doc_types(page)

        # Step 4: Set date range using JavaScript injection
        # This bypasses timing issues with DOM-based filling
        await self._set_dates_via_js(page, start_date, end_date)

        # Step 5: Click Search
        log.info("Clicking Search...")
        await self._click_search(page)
        await asyncio.sleep(5)

        # Step 6: Wait for Export to CSV button to appear and click it
        log.info("Waiting for Export to CSV button...")
        csv_content = await self._wait_and_export(page)

        if csv_content:
            records = parse_csv(csv_content)
            self.records.extend(records)
        else:
            log.warning("No CSV content obtained")

    async def _accept_disclaimer(self, page: Page):
        content = await page.content()
        if "disclaimer" in content.lower() or "accept" in content.lower():
            for sel in ["input[type='submit']", "input[value*='Accept' i]"]:
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

    async def _select_all_doc_types(self, page: Page):
        """Select 'All' from the DocTypeGroupDropDown."""
        # From logs: dropdown has option {'value': 'All|all', 'text': 'All'}
        try:
            sel = page.locator("#DocTypeGroupDropDown, [name='DocTypeGroupDropDown'], select").first
            if await sel.count() > 0:
                await sel.select_option(value="All|all")
                await asyncio.sleep(2)
                log.info("✓ Selected All doc types from group dropdown")
                return
        except Exception as e:
            log.debug("Group dropdown select failed: %s", e)

        # Fallback: use JavaScript
        try:
            await page.evaluate("""
                () => {
                    const sel = document.querySelector(
                        '#DocTypeGroupDropDown, [name="DocTypeGroupDropDown"], select'
                    );
                    if (sel) {
                        // Find the 'All' option
                        for (let opt of sel.options) {
                            if (opt.text.trim() === 'All') {
                                sel.value = opt.value;
                                sel.dispatchEvent(new Event('change', {bubbles: true}));
                                return 'selected All';
                            }
                        }
                    }
                    return 'not found';
                }
            """)
            await asyncio.sleep(2)
            log.info("✓ Selected All via JavaScript")
        except Exception as e:
            log.warning("Could not select All doc types: %s", e)

        # Also check the SelectAll checkbox if it exists
        for sel in ["#Checkbox1", "[name='SelectAllDocTypesToggle']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.check()
                    await asyncio.sleep(1)
                    log.info("✓ Checked SelectAll checkbox")
            except Exception:
                pass

    async def _set_dates_via_js(self, page: Page, start_date: str, end_date: str):
        """Set date fields using JavaScript to bypass timing issues."""
        result = await page.evaluate(f"""
            () => {{
                const results = {{}};

                // First set the date range dropdown to "Specify Date Range"
                const dropdowns = document.querySelectorAll('select');
                for (let dd of dropdowns) {{
                    for (let opt of dd.options) {{
                        if (opt.text.includes('Specify') || opt.text.includes('Range')) {{
                            dd.value = opt.value;
                            dd.dispatchEvent(new Event('change', {{bubbles: true}}));
                            results.dropdown = 'set to: ' + opt.text;
                            break;
                        }}
                    }}
                }}

                // Set RecordDateFrom
                const fromField = document.querySelector(
                    '#RecordDateFrom, [name="RecordDateFrom"], [id*="From"], [name*="From"]'
                );
                if (fromField) {{
                    fromField.value = '{start_date}';
                    fromField.dispatchEvent(new Event('change', {{bubbles: true}}));
                    fromField.dispatchEvent(new Event('input', {{bubbles: true}}));
                    results.from = 'set to {start_date}';
                }}

                // Set RecordDateTo
                const toField = document.querySelector(
                    '#RecordDateTo, [name="RecordDateTo"], [id*="To"], [name*="To"]'
                );
                if (toField) {{
                    toField.value = '{end_date}';
                    toField.dispatchEvent(new Event('change', {{bubbles: true}}));
                    toField.dispatchEvent(new Event('input', {{bubbles: true}}));
                    results.to = 'set to {end_date}';
                }}

                // Log all input fields for debugging
                const inputs = Array.from(document.querySelectorAll('input[type=text], input[type=date]'))
                    .map(el => ({{id: el.id, name: el.name, value: el.value}}));
                results.inputs = inputs;

                return results;
            }}
        """)
        log.info("JS date setting result: %s", result)
        await asyncio.sleep(1)

    async def _click_search(self, page: Page):
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

    async def _wait_and_export(self, page: Page, max_wait: int = 30) -> Optional[str]:
        """Wait for results to load then click Export to CSV."""
        # Wait for results to appear
        log.info("Waiting for results grid to load...")
        for i in range(max_wait):
            await asyncio.sleep(1)
            # Check if any result count text appears
            result_text = await page.evaluate("""
                () => {
                    // Look for result count indicators
                    const indicators = [
                        '.k-pager-info', '.pager-info', 'span[class*="page"]',
                        'span[class*="count"]', 'span[class*="total"]',
                        '.t-status-text', '.t-pager'
                    ];
                    for (let sel of indicators) {
                        const el = document.querySelector(sel);
                        if (el && el.innerText.trim()) return el.innerText.trim();
                    }
                    // Also check for export button
                    const exportBtn = document.querySelector(
                        'input[value*="Export"], button[*="Export"], a[*="Export"]'
                    );
                    if (exportBtn) return 'export_ready';
                    return '';
                }
            """)
            if result_text:
                log.info("Results indicator found: %s", result_text)
                break

        await asyncio.sleep(2)

        # Log all buttons now visible
        buttons = await page.evaluate("""
            () => Array.from(document.querySelectorAll(
                'input[type=button],input[type=submit],button,a'
            )).map(el => ({
                tag: el.tagName,
                type: el.type || '',
                value: el.value || el.innerText.trim() || '',
                id: el.id || '',
                cls: el.className || ''
            })).filter(b => b.value.length > 0 && b.value.length < 60)
        """)
        log.info("Buttons after search:")
        for b in buttons:
            log.info("  %s id='%s' cls='%s' value='%s'",
                     b['tag'], b['id'], b['cls'][:30], b['value'][:40])

        # Try every possible export button
        for sel in [
            "input[value='Export to CSV']",
            "input[value*='Export']",
            "button:has-text('Export to CSV')",
            "button:has-text('Export')",
            "a:has-text('Export to CSV')",
            "a:has-text('Export')",
            ".t-toolbar input[value*='Export']",
            ".k-toolbar input[value*='Export']",
            "input[value*='CSV']",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    log.info("✓ Found export button: %s", sel)
                    async with page.expect_download(timeout=30000) as dl_info:
                        await el.click()
                    download = await dl_info.value
                    path = await download.path()
                    if path:
                        with open(path,"r",encoding="utf-8-sig",errors="ignore") as f:
                            content = f.read()
                        log.info("✓ CSV downloaded: %d chars, %d lines",
                                 len(content), content.count("\n"))
                        log.info("CSV preview: %s", content[:300])
                        return content
            except Exception as e:
                log.debug("Export %s: %s", sel, e)

        log.warning("Export button not found after search")
        return None


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
    log.info("Horry County Scraper v11 — JS Date Injection + CSV Export")
    log.info("="*60)

    scraper = AcclaimScraper()
    raw     = await scraper.scrape()

    seen, unique = set(), []
    for r in raw:
        key = (r.get("doc_num",""), r.get("cat",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique: %d", len(unique))

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
