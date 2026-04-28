"""
Horry County SC — Motivated Seller Lead Scraper v9
DEFINITIVE APPROACH: Intercept network requests made by Acclaim's Kendo JS grid.
When the search form submits, the Kendo grid makes a background POST/GET request
to load results as JSON. We capture that response directly.
Also attempts CSV download as fallback.
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
    from playwright.async_api import async_playwright, Page, Route, Request
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
SCRAPER_TIMEOUT = 20 * 60

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
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(str(raw).strip()[:19], fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return str(raw).strip()[:10]


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
        self.records        = []
        self.api_responses  = []  # captured JSON from network
        self.start_time     = None

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
                accept_downloads=True,
            )
            page = await context.new_page()
            page.set_default_timeout(20000)

            # ── INTERCEPT ALL NETWORK RESPONSES ──────────────────────────
            # This captures the JSON data the Kendo grid loads from the server
            async def handle_response(response):
                try:
                    url = response.url
                    ct  = response.headers.get("content-type","")
                    # Capture JSON responses from the Acclaim domain
                    if "horrycounty.org" in url and response.status == 200:
                        if "json" in ct or "text" in ct:
                            try:
                                body = await response.text()
                                if body and len(body) > 10:
                                    log.info("Captured response: %s (%d chars) CT=%s",
                                             url, len(body), ct)
                                    self.api_responses.append({
                                        "url": url, "body": body, "ct": ct
                                    })
                            except Exception as e:
                                log.debug("Response read error: %s", e)
                except Exception as e:
                    log.debug("Response handler error: %s", e)

            page.on("response", handle_response)

            try:
                await self._run(page)
            except Exception as exc:
                log.error("Scraper error: %s", exc, exc_info=True)
            finally:
                await browser.close()

        # Process captured API responses
        log.info("Captured %d network responses", len(self.api_responses))
        for resp in self.api_responses:
            self._process_api_response(resp)

        log.info("Total records: %d", len(self.records))
        return self.records

    def _process_api_response(self, resp: dict):
        """Try to parse captured network response as JSON data."""
        url  = resp["url"]
        body = resp["body"]
        log.info("Processing response from: %s", url)
        log.info("Body preview: %s", body[:300])

        # Try JSON parsing
        try:
            data = json.loads(body)
            records = self._extract_from_json(data)
            if records:
                log.info("Extracted %d records from JSON at %s", len(records), url)
                self.records.extend(records)
                return
        except Exception:
            pass

        # Try CSV parsing
        try:
            records = self._parse_csv_content(body)
            if records:
                log.info("Extracted %d records from CSV at %s", len(records), url)
                self.records.extend(records)
                return
        except Exception:
            pass

        # Try HTML table parsing
        try:
            soup    = BeautifulSoup(body, "lxml")
            records = self._parse_html_table(soup)
            if records:
                log.info("Extracted %d records from HTML at %s", len(records), url)
                self.records.extend(records)
        except Exception:
            pass

    def _extract_from_json(self, data) -> list:
        """Extract records from various JSON structures."""
        records = []

        # Kendo grid typically returns {"Data": [...], "Total": N}
        # or just a plain array
        rows = []
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            for key in ["Data", "data", "Results", "results",
                        "Items", "items", "Records", "records", "rows"]:
                if key in data and isinstance(data[key], list):
                    rows = data[key]
                    break

        log.info("JSON rows found: %d", len(rows))

        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                # Map common Acclaim JSON field names
                def get(*keys):
                    for k in keys:
                        v = row.get(k) or row.get(k.lower()) or row.get(k.upper())
                        if v:
                            return str(v).strip()
                    return ""

                doc_type = get("DocType","DocumentType","Doc_Type","docType").upper()
                if not doc_type:
                    continue

                if doc_type not in DOC_CATEGORIES:
                    continue

                cat, cat_label = DOC_CATEGORIES[doc_type]

                records.append({
                    "doc_num":     get("InstrumentNumber","DocNumber","Instrument","InstrNum","docNum"),
                    "doc_type":    doc_type,
                    "filed":       normalize_date(get("RecordDate","RecDate","DateRecorded","Filed","recordDate")),
                    "cat":         cat,
                    "cat_label":   cat_label,
                    "owner":       get("Grantor","GrantorName","Owner","grantor"),
                    "grantee":     get("Grantee","GranteeName","grantee"),
                    "amount":      parse_amount(get("Consideration","Amount","consideration")),
                    "legal":       get("LegalDescription","Legal","Memo","legal"),
                    "clerk_url":   get("DocumentUrl","Url","url") or DOCTYPE_URL,
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
            except Exception as e:
                log.debug("JSON row error: %s", e)

        return records

    def _parse_csv_content(self, raw: str) -> list:
        """Parse CSV export from Acclaim."""
        if not raw or "\n" not in raw:
            return []
        records = []
        try:
            reader = csv.DictReader(io.StringIO(raw))
            for row in reader:
                doc_type = (row.get("Doc Type","") or row.get("Document Type","") or
                            row.get("DocType","") or "").strip().upper()
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
            log.debug("CSV parse error: %s", e)
        return records

    def _parse_html_table(self, soup: BeautifulSoup) -> list:
        """Parse standard HTML table."""
        tables = soup.find_all("table")
        if not tables:
            return []
        best = max(tables, key=lambda t: len(t.find_all("tr")))
        rows = best.find_all("tr")
        if len(rows) < 2:
            return []

        headers = [th.get_text(strip=True).lower()
                   for th in rows[0].find_all(["th","td"])]

        records = []
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            try:
                def cell(i): return cells[i].get_text(strip=True) if i < len(cells) else ""
                def find(*names):
                    for name in names:
                        for i, h in enumerate(headers):
                            if name in h: return cell(i)
                    return ""

                doc_type = (find("type","doctype") or cell(1)).strip().upper()
                if doc_type not in DOC_CATEGORIES:
                    continue
                cat, cat_label = DOC_CATEGORIES[doc_type]
                link = row.find("a", href=True)
                clerk_url = link["href"] if link else DOCTYPE_URL
                if clerk_url.startswith("/"):
                    clerk_url = ACCLAIM_BASE + clerk_url

                records.append({
                    "doc_num":     (find("instrument","instr","doc","number") or cell(0)).strip(),
                    "doc_type":    doc_type,
                    "filed":       normalize_date(find("record date","date","filed") or cell(2)),
                    "cat":         cat,
                    "cat_label":   cat_label,
                    "owner":       (find("grantor","owner") or cell(3)).strip(),
                    "grantee":     (find("grantee","buyer") or (cell(4) if len(cells)>4 else "")).strip(),
                    "amount":      parse_amount(find("consideration","amount") or (cell(6) if len(cells)>6 else "")),
                    "legal":       (find("legal","description") or (cell(5) if len(cells)>5 else "")).strip(),
                    "clerk_url":   clerk_url,
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
            except Exception as e:
                log.debug("HTML row error: %s", e)
        return records

    async def _run(self, page: Page):
        start_date, end_date = date_range_str()
        log.info("Date range: %s to %s", start_date, end_date)

        # Load portal
        log.info("Loading portal...")
        await page.goto(ACCLAIM_BASE + "/", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)
        await self._accept_disclaimer(page)

        # Load search page — wait for full network idle so JS initializes
        log.info("Loading search page...")
        await page.goto(DOCTYPE_URL, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        log.info("Search page loaded: %s", page.url)

        # Fill dates
        await self._fill_field(page, "RecordDateFrom", start_date)
        await self._fill_field(page, "RecordDateTo",   end_date)

        # Select all
        await self._select_all(page)

        # Submit — wait for networkidle so all AJAX calls complete
        log.info("Submitting search and waiting for results...")
        for sel in ["#btnSearch", "input[type='submit']", "button[type='submit']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    # Wait for network to be fully idle after results load
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    await asyncio.sleep(5)
                    log.info("Search complete. URL: %s", page.url)
                    break
            except Exception:
                pass

        # Log full page text so we can see results
        page_text = await page.evaluate("() => document.body.innerText")
        log.info("=== PAGE TEXT (first 1000 chars) ===")
        log.info(page_text[:1000])
        log.info("=== END PAGE TEXT ===")

        # Try clicking Export/CSV button
        await self._try_export(page)

        # Also try scrolling through paginated results
        await self._try_pagination(page)

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

    async def _try_export(self, page: Page):
        """Click the Acclaim CSV export button."""
        for sel in [
            "a:has-text('Export')", "button:has-text('Export')",
            "a:has-text('CSV')",    "button:has-text('CSV')",
            "input[value*='Export' i]", "input[value*='CSV' i]",
            ".k-grid-toolbar a", "a.k-button",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    log.info("Found export button: %s", sel)
                    async with page.expect_download(timeout=10000) as dl:
                        await el.click()
                    download = await dl.value
                    path     = await download.path()
                    if path:
                        with open(path, "r", encoding="utf-8", errors="ignore") as f:
                            content = f.read()
                        log.info("Downloaded file: %d chars", len(content))
                        records = self._parse_csv_content(content)
                        if records:
                            self.records.extend(records)
                            log.info("Got %d records from export", len(records))
                            return
            except Exception as e:
                log.debug("Export %s failed: %s", sel, e)

    async def _try_pagination(self, page: Page):
        """Try to page through results if they exist."""
        # Check for any result count text
        count_text = await page.evaluate("""
            () => {
                const els = document.querySelectorAll(
                    '.k-pager-info, .pager-info, [class*="count"], [class*="total"], [class*="results"]'
                );
                return Array.from(els).map(e => e.innerText).join(' | ');
            }
        """)
        log.info("Result count elements: %s", count_text)


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
    log.info("Horry County Scraper v9 — Network Intercept")
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
