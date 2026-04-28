"""
Horry County SC — Motivated Seller Lead Scraper v5
Strategy: FAST - Use Acclaim's Record Date search with exact field IDs
- Navigate directly to DocType search page
- Fill RecordDateFrom + RecordDateTo with exact IDs
- Click Select All checkbox
- Submit and parse results
- Hard 20 minute total timeout
"""

import asyncio
import csv
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

ACCLAIM_BASE  = "https://acclaimweb.horrycounty.org/AcclaimWeb"
DOCTYPE_URL   = f"{ACCLAIM_BASE}/search/SearchTypeDocType"
LOOK_BACK_DAYS = 7

# Hard timeout — scraper stops collecting after this many seconds
SCRAPER_TIMEOUT = 18 * 60  # 18 minutes

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
    if doc_type in ("LNIRS","LNCORPTX","LNFED","TAXDEED"):
        flags.append("Tax lien")
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
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    amount = record.get("amount")
    if amount:
        if amount > 100_000: score += 15
        elif amount > 50_000: score += 10
    if "New this week" in flags: score += 5
    if record.get("prop_address", "").strip(): score += 5
    return min(score, 100)


# ===========================================================================
# Fast Acclaim Scraper
# ===========================================================================

class AcclaimScraper:

    def __init__(self):
        self.records   = []
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
                viewport={"width": 1280, "height": 900},
            )
            page = await context.new_page()

            # Set default timeout for all operations
            page.set_default_timeout(15000)

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
        log.info("Date range: %s → %s", start_date, end_date)

        # ── Step 1: Accept disclaimer ──────────────────────────────
        log.info("Loading portal...")
        await page.goto(ACCLAIM_BASE + "/", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(1)
        await self._accept_disclaimer(page)

        # ── Step 2: Go directly to DocType search ─────────────────
        log.info("Loading DocType search page...")
        await page.goto(DOCTYPE_URL, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)

        # ── Step 3: Fill dates with EXACT field IDs ───────────────
        log.info("Filling RecordDateFrom = %s", start_date)
        await self._fill_field(page, "RecordDateFrom", start_date)

        log.info("Filling RecordDateTo = %s", end_date)
        await self._fill_field(page, "RecordDateTo", end_date)

        # ── Step 4: Select All doc types ──────────────────────────
        log.info("Selecting all doc types...")
        await self._select_all(page)

        # ── Step 5: Submit ────────────────────────────────────────
        log.info("Clicking search...")
        await self._submit(page)
        await asyncio.sleep(3)
        log.info("Results URL: %s", page.url)

        # ── Step 6: Parse results ─────────────────────────────────
        await self._parse_all_pages(page)

    async def _accept_disclaimer(self, page: Page):
        try:
            # Check if disclaimer button exists
            btn = page.locator("input[type='submit']").first
            if await btn.count() > 0:
                await btn.click()
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                await asyncio.sleep(1)
                log.info("Disclaimer accepted. URL: %s", page.url)
        except Exception as e:
            log.debug("Disclaimer step: %s", e)

    async def _fill_field(self, page: Page, field_id: str, value: str):
        """Fill field by exact ID — the IDs we confirmed from the log."""
        try:
            el = page.locator(f"#{field_id}")
            await el.click(triple_click=True)
            await el.fill(value)
            log.info("✓ Filled #%s = %s", field_id, value)
        except Exception as e:
            log.warning("✗ Could not fill #%s: %s", field_id, e)
            # Try by name as fallback
            try:
                el = page.locator(f"[name='{field_id}']")
                await el.click(triple_click=True)
                await el.fill(value)
                log.info("✓ Filled [name=%s] = %s (fallback)", field_id, value)
            except Exception as e2:
                log.warning("✗ Fallback also failed for %s: %s", field_id, e2)

    async def _select_all(self, page: Page):
        """Check the Select All checkbox to get all document types."""
        # From the log: id='Checkbox1' name='SelectAllDocTypesToggle'
        selectors = [
            "#Checkbox1",
            "[name='SelectAllDocTypesToggle']",
        ]
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.check()
                    await asyncio.sleep(1)
                    log.info("✓ Selected all doc types via %s", sel)
                    return
            except Exception as e:
                log.debug("Select all %s failed: %s", sel, e)

        # Fallback: check every DocTypeInfoCheckBox
        try:
            cbs = await page.query_selector_all("[name='DocTypeInfoCheckBox']")
            log.info("Checking %d individual doc type boxes", len(cbs))
            for cb in cbs[:50]:  # limit to first 50 to avoid timeout
                try:
                    if not await cb.is_checked():
                        await cb.check()
                except Exception:
                    pass
        except Exception as e:
            log.warning("Could not check doc type boxes: %s", e)

    async def _submit(self, page: Page):
        """Click the search button."""
        for sel in ["#btnSearch", "input[type='submit']", "button[type='submit']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    await page.wait_for_load_state("domcontentloaded", timeout=20000)
                    log.info("✓ Submitted via %s", sel)
                    return
            except Exception:
                pass
        log.error("Could not find submit button!")

    async def _parse_all_pages(self, page: Page):
        """Parse result pages — stops if timeout reached."""
        page_num = 1
        consecutive_empty = 0

        while True:
            if self.timed_out():
                log.warning("Approaching time limit — stopping at page %d with %d records",
                            page_num, len(self.records))
                break

            await asyncio.sleep(1)
            html  = await page.content()
            soup  = BeautifulSoup(html, "lxml")
            found = self._parse_table(soup)
            log.info("Page %d: %d rows found (total so far: %d)",
                     page_num, found, len(self.records))

            if found == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    log.info("Two empty pages — done")
                    break
            else:
                consecutive_empty = 0

            # Find next page button
            next_el = await self._find_next(page)
            if not next_el:
                log.info("No next page — done at page %d", page_num)
                break

            try:
                await next_el.click()
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                page_num += 1
            except Exception as e:
                log.warning("Pagination failed: %s", e)
                break

    async def _find_next(self, page: Page):
        for sel in [
            "a:has-text('Next')",
            "a:has-text('>')",
            "li.next a",
            ".pagination a:last-child",
            "a[title='Next Page']",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    cls      = await el.get_attribute("class") or ""
                    disabled = await el.get_attribute("disabled")
                    if "disabled" not in cls.lower() and not disabled:
                        return el
            except Exception:
                pass
        return None

    def _parse_table(self, soup: BeautifulSoup) -> int:
        tables = soup.find_all("table")
        if not tables:
            return 0

        # Pick table with most rows
        best = max(tables, key=lambda t: len(t.find_all("tr")))
        rows = best.find_all("tr")
        if len(rows) < 2:
            return 0

        headers = [th.get_text(strip=True).lower()
                   for th in rows[0].find_all(["th", "td"])]

        count = 0
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            try:
                rec = self._parse_row(cells, headers, row)
                if rec:
                    self.records.append(rec)
                    count += 1
            except Exception as e:
                log.debug("Row error: %s", e)
        return count

    def _parse_row(self, cells, headers, row) -> Optional[dict]:
        def cell(i):
            return cells[i].get_text(strip=True) if i < len(cells) else ""

        def find(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return cell(i)
            return ""

        doc_num  = find("instrument","instr","doc","number","book") or cell(0)
        doc_type = (find("type","doctype") or cell(1)).strip().upper()
        filed    = find("record date","date","filed","recorded") or cell(2)
        grantor  = find("grantor","owner","seller") or cell(3)
        grantee  = find("grantee","buyer") or (cell(4) if len(cells)>4 else "")
        legal    = find("legal","description") or (cell(5) if len(cells)>5 else "")
        amount   = find("consideration","amount") or (cell(6) if len(cells)>6 else "")

        # Only keep target doc types
        if doc_type not in DOC_CATEGORIES:
            return None

        cat, cat_label = DOC_CATEGORIES[doc_type]

        link = row.find("a", href=True)
        if link:
            href = link["href"]
            clerk_url = (href if href.startswith("http")
                         else ACCLAIM_BASE + href if href.startswith("/")
                         else ACCLAIM_BASE + "/" + href)
        else:
            clerk_url = DOCTYPE_URL

        if not doc_num and not grantor:
            return None

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
            "clerk_url":   clerk_url,
            "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
            "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
        }


# ===========================================================================
# Save + Export
# ===========================================================================

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
    log.info("Horry County Scraper v5 — Fast Mode")
    log.info("Hard timeout: %d minutes", SCRAPER_TIMEOUT//60)
    log.info("="*60)

    scraper = AcclaimScraper()
    raw     = await scraper.scrape()

    # Deduplicate
    seen, unique = set(), []
    for r in raw:
        key = (r.get("doc_num",""), r.get("doc_type",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique after dedup: %d", len(unique))

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
