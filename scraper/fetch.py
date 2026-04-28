"""
Horry County SC — Motivated Seller Lead Scraper
Portal: Horry County Register of Deeds (Acclaim system)
URL:    https://acclaimweb.horrycounty.org/AcclaimWeb/
"""

import asyncio
import csv
import json
import logging
import os
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

ACCLAIM_BASE = "https://acclaimweb.horrycounty.org/AcclaimWeb"
LOOK_BACK_DAYS = 7
MAX_RETRIES = 3
RETRY_DELAY = 3

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
    flags = []
    cat      = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    owner    = record.get("owner", "")
    filed    = record.get("filed", "")

    if doc_type == "LP":    flags.append("Lis pendens")
    if doc_type == "NOFC":  flags.append("Pre-foreclosure")
    if cat == "JUD":        flags.append("Judgment lien")
    if doc_type in ("LNIRS","LNCORPTX","LNFED","TAXDEED"): flags.append("Tax lien")
    if doc_type == "LNMECH": flags.append("Mechanic lien")
    if doc_type == "PRO":   flags.append("Probate / estate")
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
    if record.get("prop_address","").strip(): score += 5
    return min(score, 100)


# ===========================================================================
# Acclaim Scraper — with full page inspection to find correct field IDs
# ===========================================================================

class AcclaimScraper:

    BASE = "https://acclaimweb.horrycounty.org/AcclaimWeb"

    def __init__(self):
        self.records = []

    async def scrape(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright unavailable")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
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

    async def _run(self, page):
        # Step 1: Accept disclaimer
        log.info("Loading Acclaim portal...")
        await page.goto(self.BASE + "/", wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)
        await self._accept_disclaimer(page)

        # Step 2: Navigate to Record Date search and inspect the page
        log.info("Navigating to Record Date search...")
        await page.goto(
            self.BASE + "/search/SearchTypeRecordDate",
            wait_until="networkidle", timeout=20000
        )
        await asyncio.sleep(3)

        # Step 3: Dump ALL input fields so we can see exact IDs
        inputs = await page.evaluate("""
            () => {
                const inputs = document.querySelectorAll('input, select');
                return Array.from(inputs).map(el => ({
                    tag: el.tagName,
                    id: el.id,
                    name: el.name,
                    type: el.type,
                    placeholder: el.placeholder,
                    value: el.value,
                    className: el.className
                }));
            }
        """)
        log.info("=== PAGE INPUTS FOUND ===")
        for inp in inputs:
            log.info("  TAG=%s ID=%s NAME=%s TYPE=%s PH=%s",
                     inp.get('tag'), inp.get('id'), inp.get('name'),
                     inp.get('type'), inp.get('placeholder'))
        log.info("=== END INPUTS ===")

        # Step 4: Try to fill date fields using every possible selector
        start_date, end_date = date_range_str()
        log.info("Date range: %s to %s", start_date, end_date)

        start_filled = await self._try_fill_date(page, start_date, "start")
        end_filled   = await self._try_fill_date(page, end_date, "end")

        if start_filled and end_filled:
            log.info("Date fields filled — submitting search...")
            await self._submit_and_parse(page, filter_types=True)
        else:
            log.warning("Date fill failed (start=%s end=%s) — trying doc type search", start_filled, end_filled)
            await self._doctype_search(page, start_date, end_date)

    async def _accept_disclaimer(self, page):
        """Accept the Acclaim disclaimer page."""
        current_url = page.url
        log.info("Current URL: %s", current_url)

        # Check if we're on disclaimer page
        if "Disclaimer" in current_url or "disclaimer" in await page.content():
            for selector in [
                "input[type='submit']",
                "input[type='button']",
                "button",
                "a.btn",
                "#btnAccept",
                "input[value*='Accept' i]",
                "input[value*='Continue' i]",
                "input[value*='Agree' i]",
            ]:
                try:
                    btn = page.locator(selector).first
                    if await btn.count() > 0:
                        await btn.click()
                        await page.wait_for_load_state("networkidle", timeout=10000)
                        await asyncio.sleep(2)
                        log.info("Disclaimer accepted via: %s | New URL: %s", selector, page.url)
                        return
                except Exception as e:
                    log.debug("Selector %s failed: %s", selector, e)

        log.info("No disclaimer needed or already accepted. URL: %s", page.url)

    async def _try_fill_date(self, page, date_value, which):
        """Try every possible way to fill a date field."""
        # Try by specific known Acclaim field IDs first
        acclaim_ids = [
            # Record date search fields
            "RecordDateFrom" if which == "start" else "RecordDateTo",
            "txtRecordDateFrom" if which == "start" else "txtRecordDateTo",
            "StartDate" if which == "start" else "EndDate",
            "FromDate" if which == "start" else "ToDate",
            "dateFrom" if which == "start" else "dateTo",
        ]

        for field_id in acclaim_ids:
            for prefix in ["", "#", "[id='", "[name='"]:
                if prefix == "#":
                    selector = f"#{field_id}"
                elif prefix == "[id='":
                    selector = f"[id='{field_id}']"
                elif prefix == "[name='":
                    selector = f"[name='{field_id}']"
                else:
                    continue

                try:
                    el = page.locator(selector).first
                    if await el.count() > 0:
                        await el.triple_click()
                        await el.fill(date_value)
                        log.info("Filled %s date (%s) via selector: %s", which, date_value, selector)
                        return True
                except Exception:
                    pass

        # Try by position — find all date-type or text inputs and use index
        try:
            all_inputs = await page.query_selector_all("input[type='text'], input[type='date'], input:not([type])")
            text_inputs = [el for el in all_inputs]
            log.info("Found %d text inputs total", len(text_inputs))

            if which == "start" and len(text_inputs) >= 1:
                await text_inputs[0].triple_click()
                await text_inputs[0].fill(date_value)
                log.info("Filled START date by position [0]")
                return True
            elif which == "end" and len(text_inputs) >= 2:
                await text_inputs[1].triple_click()
                await text_inputs[1].fill(date_value)
                log.info("Filled END date by position [1]")
                return True
        except Exception as e:
            log.warning("Position-based fill failed: %s", e)

        return False

    async def _submit_and_parse(self, page, filter_types=True):
        """Click search and parse all result pages."""
        # Try every possible submit button
        for sel in [
            "input[type='submit']",
            "button[type='submit']",
            "button:has-text('Search')",
            "input[value*='Search' i]",
            "input[value*='Go' i]",
            ".btn-search",
            "#btnSearch",
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=20000)
                    await asyncio.sleep(3)
                    log.info("Submitted via: %s", sel)
                    break
            except Exception:
                pass

        # Parse pages
        total = await self._parse_pages(page, None, filter_types)
        log.info("Total records from date search: %d", total)

    async def _doctype_search(self, page, start_date, end_date):
        """Search doc type by doc type as fallback."""
        log.info("Running doc-type-by-doc-type fallback search...")
        url = self.BASE + "/search/SearchTypeDocType"

        for doc_type in TARGET_DOC_TYPES:
            try:
                await page.goto(url, wait_until="networkidle", timeout=20000)
                await asyncio.sleep(2)

                # Inspect inputs on this page too
                inputs = await page.evaluate("""
                    () => Array.from(document.querySelectorAll('input,select')).map(el => ({
                        id: el.id, name: el.name, type: el.type, placeholder: el.placeholder
                    }))
                """)
                log.info("DocType page inputs: %s", inputs)

                # Try to fill doc type field
                filled = False
                for sel in ["#DocType","[name='DocType']","input[placeholder*='type' i]",
                            "input[placeholder*='document' i]","input[type='text']"]:
                    try:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.triple_click()
                            await el.fill(doc_type)
                            filled = True
                            log.info("Filled doc type %s via %s", doc_type, sel)
                            break
                    except Exception:
                        pass

                # Try dropdown
                if not filled:
                    for sel in ["select#DocType","select[name='DocType']","select"]:
                        try:
                            el = page.locator(sel).first
                            if await el.count() > 0:
                                await el.select_option(value=doc_type)
                                filled = True
                                log.info("Selected doc type %s via dropdown %s", doc_type, sel)
                                break
                        except Exception:
                            pass

                if filled:
                    # Fill dates if possible
                    await self._try_fill_date(page, start_date, "start")
                    await self._try_fill_date(page, end_date, "end")
                    await self._submit_and_parse(page, filter_types=False)

            except Exception as exc:
                log.warning("Doc type %s failed: %s", doc_type, exc)

    async def _parse_pages(self, page, override_doc_type, filter_types):
        total = 0
        page_num = 1
        while True:
            await asyncio.sleep(2)
            html  = await page.content()
            soup  = BeautifulSoup(html, "lxml")
            found = self._parse_table(soup, override_doc_type, filter_types)
            total += found
            log.info("Page %d: %d rows found", page_num, found)

            # Look for next page link
            next_btn = None
            for sel in ["a:has-text('Next')","a:has-text('>')","li.next a",".pagination a:last-child"]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        cls = await el.get_attribute("class") or ""
                        disabled = await el.get_attribute("disabled")
                        if "disabled" not in cls and not disabled:
                            next_btn = el
                            break
                except Exception:
                    pass

            if not next_btn:
                break

            try:
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                page_num += 1
            except Exception:
                break

        return total

    def _parse_table(self, soup, override_doc_type, filter_types):
        tables = soup.find_all("table")
        if not tables:
            return 0

        best = max(tables, key=lambda t: len(t.find_all("tr")), default=None)
        if not best or len(best.find_all("tr")) < 2:
            return 0

        header_row = best.find("tr")
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th","td"])]
        log.info("Table headers: %s", headers)

        count = 0
        for row in best.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            try:
                rec = self._parse_row(cells, headers, override_doc_type, filter_types, row)
                if rec:
                    self.records.append(rec)
                    count += 1
            except Exception as exc:
                log.debug("Row error: %s", exc)
        return count

    def _parse_row(self, cells, headers, override_doc_type, filter_types, row):
        def cell(idx):
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        def find(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return cell(i)
            return ""

        doc_num  = find("instrument","instr","doc num","book","number") or cell(0)
        doc_type = (find("type","doctype","doc type") or override_doc_type or cell(1)).strip().upper()
        filed    = find("record date","date","filed","recorded") or cell(2)
        grantor  = find("grantor","owner","seller","party 1") or cell(3)
        grantee  = find("grantee","buyer","party 2") or (cell(4) if len(cells) > 4 else "")
        legal    = find("legal","description","memo") or (cell(5) if len(cells) > 5 else "")
        amount   = find("consideration","amount","price") or (cell(6) if len(cells) > 6 else "")

        if filter_types and doc_type not in DOC_CATEGORIES:
            return None

        cat, cat_label = DOC_CATEGORIES.get(doc_type, ("MISC", doc_type or "Unknown"))

        link = row.find("a", href=True)
        if link:
            href = link["href"]
            clerk_url = href if href.startswith("http") else self.BASE + (href if href.startswith("/") else "/" + href)
        else:
            clerk_url = self.BASE + "/"

        if not doc_num and not grantor:
            return None

        return {
            "doc_num":   doc_num.strip(),
            "doc_type":  doc_type,
            "filed":     normalize_date(filed),
            "cat":       cat,
            "cat_label": cat_label,
            "owner":     grantor.strip(),
            "grantee":   grantee.strip(),
            "amount":    parse_amount(amount),
            "legal":     legal.strip(),
            "clerk_url": clerk_url,
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
    log.info("Horry County Motivated Seller Scraper")
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
    log.info("Done. Total=%d", len(unique))


if __name__ == "__main__":
    asyncio.run(main())
