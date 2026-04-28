"""
Horry County SC — Motivated Seller Lead Scraper
Portal: Horry County Register of Deeds (Acclaim)
Strategy: Use Document Type search page which has:
  - DocTypesDisplay / DocTypeInfoCheckBox fields
  - RecordDateFrom / RecordDateTo fields  
  - btnSearch submit button
"""

import asyncio
import csv
import json
import logging
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
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
LOOK_BACK_DAYS = 7

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

TARGET_DOC_TYPES = list(DOC_CATEGORIES.keys())


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
# Acclaim Scraper — using exact field IDs discovered from log
# ===========================================================================

class AcclaimScraper:

    BASE      = "https://acclaimweb.horrycounty.org/AcclaimWeb"
    DOCTYPE_URL = "https://acclaimweb.horrycounty.org/AcclaimWeb/search/SearchTypeDocType"

    def __init__(self):
        self.records = []

    async def scrape(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright unavailable")
            return []

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

        # Step 1: Load portal and accept disclaimer
        log.info("Loading portal...")
        await page.goto(self.BASE + "/", wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)
        await self._accept_disclaimer(page)
        log.info("Post-disclaimer URL: %s", page.url)

        # Step 2: Go to Document Type search page
        # This page has RecordDateFrom, RecordDateTo, DocTypeInfoCheckBox, btnSearch
        log.info("Loading Document Type search page...")
        await page.goto(self.DOCTYPE_URL, wait_until="networkidle", timeout=20000)
        await asyncio.sleep(3)

        # Step 3: Fill date range using EXACT field IDs from log
        log.info("Filling date fields...")
        date_start_filled = await self._fill_exact(page, "RecordDateFrom", start_date)
        date_end_filled   = await self._fill_exact(page, "RecordDateTo",   end_date)
        log.info("Date fields filled: start=%s end=%s", date_start_filled, date_end_filled)

        # Step 4: Select all doc type checkboxes
        # The page has DocTypeInfoCheckBox checkboxes — check all that match our targets
        await self._select_doc_types(page)

        # Step 5: Submit search
        log.info("Submitting search...")
        submitted = await self._click_search(page)
        if not submitted:
            log.error("Could not submit search form")
            return

        await asyncio.sleep(4)
        log.info("Results URL: %s", page.url)

        # Step 6: Parse all result pages
        total = await self._parse_all_pages(page)
        log.info("Total records collected: %d", total)

    async def _accept_disclaimer(self, page: Page):
        """Accept Acclaim disclaimer — clicks the submit button."""
        content = await page.content()
        if "Disclaimer" not in content and "disclaimer" not in page.url.lower():
            log.info("No disclaimer needed")
            return

        for selector in [
            "input[type='submit']",
            "input[value*='Accept' i]",
            "input[value*='Continue' i]",
            "input[value*='Agree' i]",
            "button:has-text('Accept')",
            "a:has-text('Accept')",
        ]:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click()
                    await page.wait_for_load_state("networkidle", timeout=10000)
                    await asyncio.sleep(2)
                    log.info("Disclaimer accepted via: %s", selector)
                    return
            except Exception:
                pass

        log.warning("Could not find disclaimer button — proceeding anyway")

    async def _fill_exact(self, page: Page, field_id: str, value: str) -> bool:
        """Fill a field by its exact ID."""
        try:
            el = page.locator(f"#{field_id}").first
            if await el.count() > 0:
                await el.click(triple_click=True)
                await el.fill(value)
                log.info("Filled #%s = %s", field_id, value)
                return True
        except Exception as e:
            log.warning("Could not fill #%s: %s", field_id, e)

        # Also try by name attribute
        try:
            el = page.locator(f"[name='{field_id}']").first
            if await el.count() > 0:
                await el.click(triple_click=True)
                await el.fill(value)
                log.info("Filled [name=%s] = %s", field_id, value)
                return True
        except Exception as e:
            log.warning("Could not fill [name=%s]: %s", field_id, e)

        return False

    async def _select_doc_types(self, page: Page):
        """
        The Acclaim DocType page shows a list of doc types with checkboxes
        named DocTypeInfoCheckBox. We need to either:
        1. Check all boxes (select all) then search — gets everything, filter in parsing
        2. Or type in DocTypesDisplay field to filter
        Strategy: Use the SelectAllDocTypesToggle checkbox to select all, 
        then we filter results by our target doc types during parsing.
        """
        # Try "Select All" checkbox first
        try:
            select_all = page.locator("#Checkbox1, [name='SelectAllDocTypesToggle']").first
            if await select_all.count() > 0:
                checked = await select_all.is_checked()
                if not checked:
                    await select_all.check()
                    await asyncio.sleep(1)
                log.info("Selected all doc types via SelectAllDocTypesToggle")
                return
        except Exception as e:
            log.debug("SelectAll checkbox failed: %s", e)

        # Fallback: check all DocTypeInfoCheckBox checkboxes
        try:
            checkboxes = await page.query_selector_all("[name='DocTypeInfoCheckBox']")
            log.info("Found %d doc type checkboxes", len(checkboxes))
            for cb in checkboxes:
                try:
                    is_checked = await cb.is_checked()
                    if not is_checked:
                        await cb.check()
                except Exception:
                    pass
            log.info("Checked all doc type checkboxes")
        except Exception as e:
            log.warning("Could not check doc type boxes: %s", e)

    async def _click_search(self, page: Page) -> bool:
        """Click the btnSearch submit button."""
        for selector in [
            "#btnSearch",
            "input[type='submit']",
            "button[type='submit']",
            "input[value*='Search' i]",
        ]:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click()
                    await page.wait_for_load_state("networkidle", timeout=20000)
                    log.info("Clicked search via: %s", selector)
                    return True
            except Exception:
                pass
        return False

    async def _parse_all_pages(self, page: Page) -> int:
        total    = 0
        page_num = 1

        while True:
            await asyncio.sleep(2)
            html  = await page.content()
            soup  = BeautifulSoup(html, "lxml")
            found = self._parse_table(soup)
            total += found
            log.info("Page %d: %d rows", page_num, found)

            # Check for next page
            next_el = None
            for sel in [
                "a:has-text('Next')",
                "a:has-text('>')",
                "li.next a",
                ".pagination a:last-child",
                "a[title='Next']",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        cls      = await el.get_attribute("class") or ""
                        disabled = await el.get_attribute("disabled")
                        if "disabled" not in cls and not disabled:
                            next_el = el
                            break
                except Exception:
                    pass

            if not next_el:
                log.info("No more pages after page %d", page_num)
                break

            try:
                await next_el.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                page_num += 1
            except Exception as e:
                log.warning("Pagination error: %s", e)
                break

        return total

    def _parse_table(self, soup: BeautifulSoup) -> int:
        """Parse Acclaim results table."""
        # Find largest table on page
        tables = soup.find_all("table")
        if not tables:
            log.debug("No tables found on page")
            return 0

        best = max(tables, key=lambda t: len(t.find_all("tr")), default=None)
        if not best or len(best.find_all("tr")) < 2:
            log.debug("No data rows in table")
            return 0

        header_row = best.find("tr")
        headers = [th.get_text(strip=True).lower()
                   for th in header_row.find_all(["th", "td"])]
        log.info("Table headers: %s", headers)

        count = 0
        for row in best.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            try:
                rec = self._parse_row(cells, headers, row)
                if rec:
                    self.records.append(rec)
                    count += 1
            except Exception as exc:
                log.debug("Row error: %s", exc)
        return count

    def _parse_row(self, cells, headers, row) -> Optional[dict]:
        def cell(idx):
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        def find(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return cell(i)
            return ""

        # Acclaim standard columns
        doc_num  = find("instrument","instr","doc num","number","book") or cell(0)
        doc_type = find("type","doctype","doc type") or cell(1)
        filed    = find("record date","date","filed","recorded") or cell(2)
        grantor  = find("grantor","owner","seller","party 1") or cell(3)
        grantee  = find("grantee","buyer","party 2") or (cell(4) if len(cells) > 4 else "")
        legal    = find("legal","description","memo") or (cell(5) if len(cells) > 5 else "")
        amount   = find("consideration","amount","price") or (cell(6) if len(cells) > 6 else "")

        doc_type = doc_type.strip().upper()

        # Only keep our target doc types
        if doc_type not in DOC_CATEGORIES:
            return None

        cat, cat_label = DOC_CATEGORIES.get(doc_type, ("MISC", doc_type))

        # Build document URL from any link in the row
        link = row.find("a", href=True)
        if link:
            href = link["href"]
            clerk_url = (href if href.startswith("http")
                         else self.BASE + href if href.startswith("/")
                         else self.BASE + "/" + href)
        else:
            clerk_url = self.DOCTYPE_URL

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
            "prop_address":"", "prop_city":"", "prop_state":"SC", "prop_zip":"",
            "mail_address":"", "mail_city":"", "mail_state":"SC", "mail_zip":"",
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
        "with_address": sum(1 for r in records if r.get("prop_address","").strip()),
        "records":      records,
    }
    for path in paths:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Saved → %s", path)


def export_ghl_csv(records, path):
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            owner = r.get("owner","")
            parts = owner.split(" ", 1) if owner else ["",""]
            writer.writerow({
                "First Name":            parts[0] if parts else "",
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
    log.info("Horry County Motivated Seller Scraper v4")
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
    log.info("Unique records after dedup: %d", len(unique))

    # Score all records
    for r in unique:
        flags     = compute_flags(r)
        r["flags"]= flags
        r["score"]= compute_score(r, flags)
    unique.sort(key=lambda r: r.get("score",0), reverse=True)

    repo = Path(__file__).parent.parent
    save_records_json(
        unique,
        str(repo/"dashboard"/"records.json"),
        str(repo/"data"/"records.json"),
    )
    export_ghl_csv(unique, str(repo/"data"/"leads_export.csv"))
    log.info("Done. Total=%d | Avg score=%.0f",
             len(unique),
             sum(r.get("score",0) for r in unique)/len(unique) if unique else 0)


if __name__ == "__main__":
    asyncio.run(main())
