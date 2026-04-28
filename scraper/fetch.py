"""
Horry County SC — Motivated Seller Lead Scraper v10
CONFIRMED: Portal uses Telerik/Kendo UI grid (telerik.common.css visible in logs)
Strategy: 
1. Intercept ONLY non-CSS/JS responses to find the data endpoint
2. After form submit, wait for and capture the Telerik grid data call
3. The Telerik grid typically POSTs to a /Read or /GetData endpoint
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

# File extensions to skip when intercepting
SKIP_EXTENSIONS = (
    ".css", ".js", ".png", ".jpg", ".gif", ".ico",
    ".woff", ".woff2", ".ttf", ".svg", ".map"
)


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
        self.records       = []
        self.data_responses = []  # non-CSS/JS responses only
        self.all_requests  = []  # all requests made after form submit

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
                viewport={"width": 1280, "height": 1024},
                accept_downloads=True,
            )
            page = await context.new_page()
            page.set_default_timeout(20000)

            # Intercept responses — SKIP CSS/JS/images, only capture data
            async def handle_response(response):
                try:
                    url = response.url
                    # Skip static assets
                    if any(url.lower().endswith(ext) for ext in SKIP_EXTENSIONS):
                        return
                    # Skip CSS and JS content types
                    ct = response.headers.get("content-type", "")
                    if "css" in ct or "javascript" in ct:
                        return
                    # Only capture from Acclaim domain
                    if "horrycounty.org" not in url:
                        return
                    if response.status == 200:
                        try:
                            body = await response.text()
                            if body and len(body) > 50:
                                log.info("DATA RESPONSE: %s", url)
                                log.info("  Status: %d | CT: %s | Size: %d",
                                         response.status, ct, len(body))
                                log.info("  Preview: %s", body[:500])
                                self.data_responses.append({
                                    "url": url, "body": body, "ct": ct
                                })
                        except Exception as e:
                            log.debug("Body read error: %s", e)
                except Exception as e:
                    log.debug("Response handler error: %s", e)

            # Also capture ALL requests to see what URLs are called
            async def handle_request(request):
                try:
                    url = request.url
                    if "horrycounty.org" in url:
                        if not any(url.lower().endswith(ext) for ext in SKIP_EXTENSIONS):
                            log.info("REQUEST: %s %s", request.method, url)
                            self.all_requests.append({
                                "method": request.method,
                                "url": url,
                                "post_data": request.post_data or ""
                            })
                except Exception:
                    pass

            page.on("response", handle_response)
            page.on("request",  handle_request)

            try:
                await self._run(page)
            except Exception as exc:
                log.error("Scraper error: %s", exc, exc_info=True)
            finally:
                await browser.close()

        # Log all requests made
        log.info("=== ALL NON-ASSET REQUESTS MADE ===")
        for req in self.all_requests:
            log.info("  %s %s", req["method"], req["url"])
            if req["post_data"]:
                log.info("    POST data: %s", req["post_data"][:200])
        log.info("=== END REQUESTS ===")

        # Process data responses
        log.info("Processing %d data responses...", len(self.data_responses))
        for resp in self.data_responses:
            self._process_response(resp)

        log.info("Total records: %d", len(self.records))
        return self.records

    def _process_response(self, resp: dict):
        url  = resp["url"]
        body = resp["body"]
        ct   = resp["ct"]

        # Try JSON
        try:
            data    = json.loads(body)
            records = self._from_json(data)
            if records:
                log.info("✓ Got %d records from JSON: %s", len(records), url)
                self.records.extend(records)
                return
        except Exception:
            pass

        # Try CSV
        if "\n" in body and "," in body:
            try:
                records = self._from_csv(body)
                if records:
                    log.info("✓ Got %d records from CSV: %s", len(records), url)
                    self.records.extend(records)
                    return
            except Exception:
                pass

        # Try HTML table
        try:
            soup    = BeautifulSoup(body, "lxml")
            records = self._from_html(soup)
            if records:
                log.info("✓ Got %d records from HTML: %s", len(records), url)
                self.records.extend(records)
        except Exception:
            pass

    def _from_json(self, data) -> list:
        rows = []
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            for key in ["Data","data","Results","results","Items","items",
                        "Records","records","rows","Rows","d"]:
                if key in data and isinstance(data[key], list):
                    rows = data[key]
                    log.info("JSON key '%s' has %d rows", key, len(rows))
                    break

        records = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                def get(*keys):
                    for k in keys:
                        for variant in [k, k.lower(), k.upper(),
                                        k[0].lower()+k[1:]]:
                            v = row.get(variant)
                            if v is not None and str(v).strip():
                                return str(v).strip()
                    return ""

                doc_type = get("DocType","DocumentType","Doc_Type",
                               "docType","DOCTYPE").upper()
                if not doc_type or doc_type not in DOC_CATEGORIES:
                    continue

                cat, cat_label = DOC_CATEGORIES[doc_type]
                records.append({
                    "doc_num":   get("InstrumentNumber","Instrument","DocNumber",
                                     "InstrNum","docNum","INSTRUMENT"),
                    "doc_type":  doc_type,
                    "filed":     normalize_date(get("RecordDate","RecDate",
                                                    "DateRecorded","Filed","recordDate")),
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     get("Grantor","GrantorName","Owner","grantor","GRANTOR"),
                    "grantee":   get("Grantee","GranteeName","grantee","GRANTEE"),
                    "amount":    parse_amount(get("Consideration","Amount",
                                                  "consideration","CONSIDERATION")),
                    "legal":     get("LegalDescription","Legal","Memo","legal","LEGAL"),
                    "clerk_url": get("DocumentUrl","Url","url","Link") or DOCTYPE_URL,
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
            except Exception as e:
                log.debug("JSON row error: %s", e)
        return records

    def _from_csv(self, raw: str) -> list:
        records = []
        try:
            reader = csv.DictReader(io.StringIO(raw))
            for row in reader:
                doc_type = ""
                for k in row:
                    if "type" in k.lower() or "doctype" in k.lower():
                        doc_type = row[k].strip().upper()
                        break
                if doc_type not in DOC_CATEGORIES:
                    continue
                cat, cat_label = DOC_CATEGORIES[doc_type]
                def get_col(*names):
                    for n in names:
                        for k in row:
                            if n.lower() in k.lower():
                                return row[k].strip()
                    return ""
                records.append({
                    "doc_num":   get_col("instrument","doc number","instr"),
                    "doc_type":  doc_type,
                    "filed":     normalize_date(get_col("record date","date","filed")),
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     get_col("grantor","owner"),
                    "grantee":   get_col("grantee","buyer"),
                    "amount":    parse_amount(get_col("consideration","amount")),
                    "legal":     get_col("legal","description","memo"),
                    "clerk_url": DOCTYPE_URL,
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
        except Exception as e:
            log.debug("CSV error: %s", e)
        return records

    def _from_html(self, soup: BeautifulSoup) -> list:
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
                def cell(i): return cells[i].get_text(strip=True) if i<len(cells) else ""
                def find(*names):
                    for name in names:
                        for i,h in enumerate(headers):
                            if name in h: return cell(i)
                    return ""
                doc_type = (find("type","doctype") or cell(1)).strip().upper()
                if doc_type not in DOC_CATEGORIES:
                    continue
                cat, cat_label = DOC_CATEGORIES[doc_type]
                link = row.find("a", href=True)
                href = link["href"] if link else ""
                clerk_url = (ACCLAIM_BASE + href if href.startswith("/")
                             else href if href.startswith("http")
                             else DOCTYPE_URL)
                records.append({
                    "doc_num":   (find("instrument","instr","doc","number") or cell(0)).strip(),
                    "doc_type":  doc_type,
                    "filed":     normalize_date(find("record date","date","filed") or cell(2)),
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     (find("grantor","owner") or cell(3)).strip(),
                    "grantee":   (find("grantee","buyer") or (cell(4) if len(cells)>4 else "")).strip(),
                    "amount":    parse_amount(find("consideration","amount") or (cell(6) if len(cells)>6 else "")),
                    "legal":     (find("legal","description") or (cell(5) if len(cells)>5 else "")).strip(),
                    "clerk_url": clerk_url,
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
            except Exception as e:
                log.debug("HTML row error: %s", e)
        return records

    async def _run(self, page: Page):
        start_date, end_date = date_range_str()
        log.info("Date range: %s to %s", start_date, end_date)

        # Load portal home
        log.info("Loading portal home...")
        await page.goto(ACCLAIM_BASE + "/", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)

        # Accept disclaimer
        content = await page.content()
        if "disclaimer" in content.lower() or "accept" in content.lower():
            for sel in ["input[type='submit']","input[value*='Accept' i]"]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click()
                        await page.wait_for_load_state("domcontentloaded", timeout=10000)
                        await asyncio.sleep(1)
                        log.info("Disclaimer accepted")
                        break
                except Exception:
                    pass

        # Navigate to doc type search
        log.info("Loading search page...")
        await page.goto(DOCTYPE_URL, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        log.info("Search page URL: %s", page.url)

        # Log the page title and visible text
        title = await page.title()
        log.info("Page title: %s", title)

        # Fill dates
        for field_id, value in [("RecordDateFrom", start_date), ("RecordDateTo", end_date)]:
            for sel in [f"#{field_id}", f"[name='{field_id}']"]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(triple_click=True)
                        await el.fill(value)
                        log.info("✓ Filled %s = %s", field_id, value)
                        break
                except Exception:
                    pass

        # Select all doc types
        for sel in ["#Checkbox1", "[name='SelectAllDocTypesToggle']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.check()
                    await asyncio.sleep(1)
                    log.info("✓ Checked SelectAll")
                    break
            except Exception:
                pass

        # Submit and wait for ALL network calls to complete
        log.info("Submitting search...")
        for sel in ["#btnSearch", "input[type='submit']", "button[type='submit']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    await asyncio.sleep(5)
                    log.info("Search submitted. URL: %s", page.url)
                    break
            except Exception:
                pass

        # Log full page text
        page_text = await page.evaluate("() => document.body.innerText")
        log.info("=== PAGE TEXT AFTER SEARCH ===")
        log.info(page_text[:2000])
        log.info("=== END PAGE TEXT ===")

        # Try CSV export
        await self._try_csv_export(page)

    async def _try_csv_export(self, page: Page):
        """Try every possible export button."""
        log.info("Looking for export buttons...")
        # Log all buttons and links on page
        buttons = await page.evaluate("""
            () => Array.from(document.querySelectorAll('a,button,input[type=button],input[type=submit]'))
                .map(el => ({tag: el.tagName, text: el.innerText || el.value || '', href: el.href || ''}))
                .filter(el => el.text.length > 0)
        """)
        log.info("Buttons/links found:")
        for b in buttons:
            log.info("  %s: '%s' -> %s", b['tag'], b['text'][:50], b['href'][:80])

        for sel in [
            "a:has-text('Export')", "button:has-text('Export')",
            "a:has-text('CSV')",    "button:has-text('CSV')",
            "a:has-text('Print')",  ".k-grid-toolbar a",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    log.info("Trying export: %s", sel)
                    async with page.expect_download(timeout=10000) as dl:
                        await el.click()
                    download = await dl.value
                    path     = await download.path()
                    if path:
                        with open(path,"r",encoding="utf-8",errors="ignore") as f:
                            content = f.read()
                        log.info("Downloaded %d chars", len(content))
                        log.info("Download preview: %s", content[:300])
            except Exception as e:
                log.debug("Export %s: %s", sel, e)


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
    log.info("Horry County Scraper v10 — Request/Response Logging")
    log.info("="*60)

    scraper = AcclaimScraper()
    raw     = await scraper.scrape()

    seen, unique = set(), []
    for r in raw:
        key = (r.get("doc_num",""), r.get("doc_type",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)

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
