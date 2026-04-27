"""
Horry County SC — Motivated Seller Lead Scraper
Portal: Horry County Register of Deeds (Acclaim system)
URL:    https://acclaimweb.horrycounty.org/AcclaimWeb/
Outputs: dashboard/records.json, data/records.json, data/leads_export.csv
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, Page, BrowserContext
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not installed — scraping disabled")

try:
    from dbfread import DBF
    DBFREAD_AVAILABLE = True
except ImportError:
    DBFREAD_AVAILABLE = False
    logging.warning("dbfread not installed — parcel lookup disabled")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("horry_scraper")

# ---------------------------------------------------------------------------
# Constants — Acclaim / Register of Deeds portal
# ---------------------------------------------------------------------------
ACCLAIM_BASE        = "https://acclaimweb.horrycounty.org/AcclaimWeb"
ACCLAIM_DISCLAIMER  = f"{ACCLAIM_BASE}/Search/Disclaimer"
ACCLAIM_DOCTYPE     = f"{ACCLAIM_BASE}/search/SearchTypeDocType"
ACCLAIM_RECORDDATE  = f"{ACCLAIM_BASE}/search/SearchTypeRecordDate"
ACCLAIM_RESULTS_API = f"{ACCLAIM_BASE}/search/SearchResults"

# Document type codes used by Horry County Acclaim system
# These are the exact abbreviations used in the Register of Deeds
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
LOOK_BACK_DAYS   = 7
MAX_RETRIES      = 3
RETRY_DELAY      = 3


# ===========================================================================
# Utilities
# ===========================================================================

def retry(fn, *args, retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(delay)
    return None


async def async_retry(coro_fn, *args, retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as exc:
            log.warning("Async attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                await asyncio.sleep(delay)
    return None


def parse_amount(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().upper())


def name_variants(full_name: str) -> list:
    parts = normalize_name(full_name).split()
    if len(parts) < 2:
        return [normalize_name(full_name)]
    first = parts[0]
    last  = " ".join(parts[1:])
    return [
        normalize_name(full_name),
        f"{last} {first}",
        f"{last}, {first}",
    ]


def date_range_str() -> tuple:
    end   = datetime.now()
    start = end - timedelta(days=LOOK_BACK_DAYS)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")


def normalize_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return str(raw).strip()


# ===========================================================================
# Scoring
# ===========================================================================

def compute_flags(record: dict) -> list:
    flags = []
    cat      = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    owner    = record.get("owner", "")
    filed    = record.get("filed", "")
    amount   = record.get("amount")

    if doc_type == "LP":
        flags.append("Lis pendens")
    if doc_type == "NOFC":
        flags.append("Pre-foreclosure")
    if cat == "JUD":
        flags.append("Judgment lien")
    if doc_type in ("LNIRS", "LNCORPTX", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if doc_type == "PRO":
        flags.append("Probate / estate")
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|HOLDINGS)\b", owner.upper()):
        flags.append("LLC / corp owner")
    try:
        if (datetime.now() - datetime.strptime(filed, "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    return list(dict.fromkeys(flags))


def compute_score(record: dict, flags: list) -> int:
    score = 30
    for flag in flags:
        score += 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    amount = record.get("amount")
    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10
    if "New this week" in flags:
        score += 5
    if record.get("prop_address", "").strip():
        score += 5
    return min(score, 100)


# ===========================================================================
# Parcel Lookup
# ===========================================================================

class ParcelLookup:
    def __init__(self):
        self.index  = {}
        self.loaded = False

    def load(self):
        if self._load_from_arcgis():
            return
        if self._load_from_zip():
            return
        log.warning("Parcel data unavailable — address enrichment skipped")

    def lookup(self, owner_name: str) -> Optional[dict]:
        if not self.loaded:
            return None
        for variant in name_variants(owner_name):
            result = self.index.get(variant)
            if result:
                return result
        return None

    def _load_from_arcgis(self) -> bool:
        # Try multiple known Horry County ArcGIS endpoints
        endpoints = [
            "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Horry_County_Parcel/FeatureServer/0/query",
            "https://gis.horrycountysc.gov/arcgis/rest/services/Parcels/MapServer/0/query",
        ]
        params = {
            "where": "1=1",
            "outFields": "OWNER,OWN1,SITE_ADDR,SITEADDR,SITE_CITY,SITE_ZIP,ADDR_1,MAILADR1,CITY,MAILCITY,STATE,ZIP,MAILZIP",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": 0,
            "resultRecordCount": 2000,
        }
        for url in endpoints:
            records = []
            try:
                p = dict(params)
                while True:
                    resp = retry(requests.get, url, params=p, timeout=30)
                    if not resp or resp.status_code != 200:
                        break
                    data = resp.json()
                    if "error" in data:
                        break
                    features = data.get("features", [])
                    if not features:
                        break
                    for feat in features:
                        records.append(self._normalize(feat.get("attributes", {})))
                    p["resultOffset"] += len(features)
                    if len(features) < 2000:
                        break
                if records:
                    self._build_index(records)
                    log.info("Parcel index: %d records from ArcGIS", len(records))
                    return True
            except Exception as exc:
                log.debug("ArcGIS endpoint %s failed: %s", url, exc)
        return False

    def _load_from_zip(self) -> bool:
        if not DBFREAD_AVAILABLE:
            return False
        urls = [
            "https://gis.horrycountysc.gov/data/parcels.zip",
            "https://opendata.horrycountysc.gov/datasets/parcels.zip",
        ]
        for url in urls:
            try:
                resp = retry(requests.get, url, timeout=60, stream=True)
                if not resp or resp.status_code != 200:
                    continue
                records = self._from_zip_bytes(resp.content)
                if records:
                    self._build_index(records)
                    log.info("Parcel index: %d records from ZIP/DBF", len(records))
                    return True
            except Exception as exc:
                log.debug("ZIP load %s failed: %s", url, exc)
        return False

    def _from_zip_bytes(self, content: bytes) -> list:
        records = []
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            dbf_files = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
            if not dbf_files:
                return []
            tmp = "/tmp/parcels.dbf"
            with open(tmp, "wb") as f:
                f.write(zf.read(dbf_files[0]))
            for row in DBF(tmp, encoding="latin-1", ignore_missing_memofile=True):
                records.append(self._normalize(dict(row)))
        return records

    def _normalize(self, attrs: dict) -> dict:
        def pick(*keys):
            for k in keys:
                for variant in [k, k.upper(), k.lower()]:
                    v = attrs.get(variant)
                    if v and str(v).strip() and str(v).strip() != "None":
                        return str(v).strip()
            return ""
        return {
            "owner":      pick("OWNER", "OWN1"),
            "site_addr":  pick("SITE_ADDR", "SITEADDR"),
            "site_city":  pick("SITE_CITY"),
            "site_zip":   pick("SITE_ZIP"),
            "mail_addr":  pick("ADDR_1", "MAILADR1"),
            "mail_city":  pick("CITY", "MAILCITY"),
            "mail_state": pick("STATE"),
            "mail_zip":   pick("ZIP", "MAILZIP"),
        }

    def _build_index(self, records: list):
        for rec in records:
            owner = rec.get("owner", "")
            if not owner:
                continue
            for variant in name_variants(owner):
                self.index[variant] = rec
        self.loaded = bool(self.index)


# ===========================================================================
# Acclaim Scraper — Horry County Register of Deeds
# ===========================================================================

class AcclaimScraper:
    """
    Scrapes https://acclaimweb.horrycounty.org/AcclaimWeb/
    Uses the Document Type search + Record Date filter.
    Steps:
      1. Accept disclaimer
      2. Navigate to Document Type search
      3. Select each doc type, set date range, submit
      4. Parse paginated results table
    """

    BASE = "https://acclaimweb.horrycounty.org/AcclaimWeb"

    def __init__(self):
        self.records: list = []

    async def scrape(self) -> list:
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright unavailable — skipping scrape")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )
            page = await context.new_page()
            try:
                accepted = await self._accept_disclaimer(page)
                if not accepted:
                    log.error("Could not accept Acclaim disclaimer")
                    return []
                await self._scrape_by_date_range(page)
            except Exception as exc:
                log.error("Acclaim scraper error: %s", exc, exc_info=True)
            finally:
                await browser.close()

        log.info("Acclaim scraper collected %d raw records", len(self.records))
        return self.records

    # ------------------------------------------------------------------
    # Step 1: Accept disclaimer
    # ------------------------------------------------------------------
    async def _accept_disclaimer(self, page: Page) -> bool:
        try:
            await page.goto(self.BASE + "/", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # Look for disclaimer accept button / checkbox
            for selector in [
                "input[value*='Accept']",
                "button:has-text('Accept')",
                "a:has-text('Accept')",
                "#btnDisclaimerAccept",
                "input[type='submit']",
                "input[type='button'][value*='accept' i]",
            ]:
                try:
                    btn = await page.wait_for_selector(selector, timeout=3000)
                    if btn:
                        await btn.click()
                        await page.wait_for_load_state("networkidle", timeout=10000)
                        await asyncio.sleep(1)
                        log.info("Disclaimer accepted via: %s", selector)
                        return True
                except Exception:
                    pass

            # If no button found, we may already be past disclaimer
            current = page.url
            if "Disclaimer" not in current:
                log.info("No disclaimer found, proceeding (URL: %s)", current)
                return True

            log.warning("Could not find disclaimer accept button")
            return False
        except Exception as exc:
            log.error("Disclaimer step failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Step 2: Scrape by date range (most reliable approach)
    # Uses the Record Date search to get ALL documents in date range,
    # then filters by doc type client-side
    # ------------------------------------------------------------------
    async def _scrape_by_date_range(self, page: Page):
        start_date, end_date = date_range_str()
        log.info("Searching date range: %s → %s", start_date, end_date)

        url = self.BASE + "/search/SearchTypeRecordDate"
        try:
            await page.goto(url, wait_until="networkidle", timeout=20000)
            await asyncio.sleep(2)
        except Exception as exc:
            log.error("Could not navigate to Record Date search: %s", exc)
            await self._scrape_by_doctype(page, start_date, end_date)
            return

        # Fill start date
        start_filled = await self._fill_field(page, [
            "#RecordDateFrom", "input[name='RecordDateFrom']",
            "input[placeholder*='Start']", "input[placeholder*='From']",
            "input[id*='From']", "input[id*='Start']",
        ], start_date)

        # Fill end date
        end_filled = await self._fill_field(page, [
            "#RecordDateTo", "input[name='RecordDateTo']",
            "input[placeholder*='End']", "input[placeholder*='To']",
            "input[id*='To']", "input[id*='End']",
        ], end_date)

        if not start_filled:
            log.warning("Could not fill date fields — trying doc type search instead")
            await self._scrape_by_doctype(page, start_date, end_date)
            return

        # Submit
        await self._click_search(page)
        await asyncio.sleep(2)

        # Parse all results (filter by doc type while parsing)
        await self._parse_all_pages(page, filter_doc_types=True)

    # ------------------------------------------------------------------
    # Fallback: search by each doc type individually
    # ------------------------------------------------------------------
    async def _scrape_by_doctype(self, page: Page, start_date: str, end_date: str):
        log.info("Falling back to doc-type-by-doc-type search")
        url = self.BASE + "/search/SearchTypeDocType"

        for doc_type in TARGET_DOC_TYPES:
            try:
                await page.goto(url, wait_until="networkidle", timeout=20000)
                await asyncio.sleep(1)

                # Type doc type into search field
                filled = await self._fill_field(page, [
                    "#DocType", "input[name='DocType']",
                    "input[placeholder*='Document Type']",
                    "input[placeholder*='Type']",
                ], doc_type)

                if filled:
                    # Also fill date range if fields exist
                    await self._fill_field(page, [
                        "#RecordDateFrom", "input[name*='From']",
                        "input[id*='From']",
                    ], start_date)
                    await self._fill_field(page, [
                        "#RecordDateTo", "input[name*='To']",
                        "input[id*='To']",
                    ], end_date)

                    await self._click_search(page)
                    await asyncio.sleep(2)
                    count = await self._parse_all_pages(page,
                                                        override_doc_type=doc_type,
                                                        filter_doc_types=False)
                    log.info("  %s → %d records", doc_type, count)
            except Exception as exc:
                log.warning("Doc type %s search failed: %s", doc_type, exc)

    # ------------------------------------------------------------------
    # Parse all result pages
    # ------------------------------------------------------------------
    async def _parse_all_pages(self, page: Page,
                                override_doc_type: str = None,
                                filter_doc_types: bool = True) -> int:
        total = 0
        page_num = 1

        while True:
            await asyncio.sleep(1)
            html    = await page.content()
            soup    = BeautifulSoup(html, "lxml")
            found   = self._parse_results(soup, override_doc_type, filter_doc_types)
            total  += found
            log.debug("  Page %d: %d rows", page_num, found)

            # Next page
            next_btn = await self._find_next(page)
            if not next_btn:
                break
            try:
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                page_num += 1
            except Exception:
                break

        return total

    def _parse_results(self, soup: BeautifulSoup,
                        override_doc_type: str,
                        filter_doc_types: bool) -> int:
        """Parse Acclaim results table. Returns row count added."""
        # Acclaim renders results in a <table> with class containing 'results' or similar
        tables = soup.find_all("table")
        if not tables:
            return 0

        # Pick the table with the most data rows
        best = max(tables, key=lambda t: len(t.find_all("tr")), default=None)
        if not best or len(best.find_all("tr")) < 2:
            return 0

        rows = best.find_all("tr")[1:]  # skip header
        headers = [th.get_text(strip=True).lower()
                   for th in best.find("tr").find_all(["th", "td"])]

        count = 0
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            try:
                rec = self._parse_row(cells, headers, override_doc_type,
                                      filter_doc_types, row)
                if rec:
                    self.records.append(rec)
                    count += 1
            except Exception as exc:
                log.debug("Row parse error: %s", exc)
        return count

    def _parse_row(self, cells, headers, override_doc_type,
                   filter_doc_types, row) -> Optional[dict]:

        def cell(idx):
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        def find(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return cell(i)
            return ""

        # Acclaim typical columns: Instrument#, DocType, RecordDate, Grantor, Grantee, Legal, Consideration
        doc_num  = find("instrument", "doc", "book", "number", "instr")
        doc_type = find("type", "doctype", "document type") or override_doc_type or ""
        filed    = find("date", "recorded", "record date", "filed")
        grantor  = find("grantor", "owner", "seller", "party 1")
        grantee  = find("grantee", "buyer", "party 2")
        legal    = find("legal", "description", "memo")
        amount   = find("consideration", "amount", "price")

        # Positional fallback for standard Acclaim 7-column layout
        if not doc_num and len(cells) >= 4:
            doc_num  = cell(0)
            doc_type = cell(1) if not override_doc_type else override_doc_type
            filed    = cell(2)
            grantor  = cell(3)
            grantee  = cell(4) if len(cells) > 4 else ""
            legal    = cell(5) if len(cells) > 5 else ""
            amount   = cell(6) if len(cells) > 6 else ""

        doc_type = (doc_type or "").strip().upper()

        # Filter to only our target doc types
        if filter_doc_types and doc_type not in DOC_CATEGORIES:
            return None

        if not doc_type and override_doc_type:
            doc_type = override_doc_type

        cat, cat_label = DOC_CATEGORIES.get(doc_type, ("MISC", doc_type or "Unknown"))

        # Build document URL
        link = row.find("a", href=True)
        if link:
            href = link["href"]
            clerk_url = (href if href.startswith("http")
                         else self.BASE + href if href.startswith("/")
                         else self.BASE + "/" + href)
        else:
            clerk_url = self.BASE + "/search/SearchTypeRecordDate"

        filed_norm = normalize_date(filed)

        if not doc_num and not grantor:
            return None

        return {
            "doc_num":   doc_num.strip(),
            "doc_type":  doc_type,
            "filed":     filed_norm,
            "cat":       cat,
            "cat_label": cat_label,
            "owner":     grantor.strip(),
            "grantee":   grantee.strip(),
            "amount":    parse_amount(amount),
            "legal":     legal.strip(),
            "clerk_url": clerk_url,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    async def _fill_field(self, page: Page, selectors: list, value: str) -> bool:
        for sel in selectors:
            try:
                field = await page.wait_for_selector(sel, timeout=2000)
                if field:
                    await field.triple_click()
                    await field.fill(value)
                    return True
            except Exception:
                pass
        return False

    async def _click_search(self, page: Page):
        for sel in [
            "input[type='submit']",
            "button[type='submit']",
            "button:has-text('Search')",
            "input[value*='Search' i]",
            "#btnSearch",
            ".search-btn",
        ]:
            try:
                btn = await page.wait_for_selector(sel, timeout=3000)
                if btn:
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    return
            except Exception:
                pass

    async def _find_next(self, page: Page):
        for sel in [
            "a:has-text('Next')",
            "a:has-text('>')",
            "li.next a",
            ".pagination a:last-child",
            "input[value='Next']",
            "a[rel='next']",
        ]:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    disabled = await btn.get_attribute("disabled")
                    cls      = await btn.get_attribute("class") or ""
                    if not disabled and "disabled" not in cls:
                        return btn
            except Exception:
                pass
        return None


# ===========================================================================
# Enrichment
# ===========================================================================

def enrich_records(records: list, parcel: ParcelLookup) -> list:
    enriched = []
    for rec in records:
        r = dict(rec)
        parcel_data = parcel.lookup(r.get("owner", "")) if r.get("owner") else None

        if parcel_data:
            r["prop_address"] = parcel_data.get("site_addr", "")
            r["prop_city"]    = parcel_data.get("site_city", "")
            r["prop_state"]   = "SC"
            r["prop_zip"]     = parcel_data.get("site_zip", "")
            r["mail_address"] = parcel_data.get("mail_addr", "")
            r["mail_city"]    = parcel_data.get("mail_city", "")
            r["mail_state"]   = parcel_data.get("mail_state", "SC")
            r["mail_zip"]     = parcel_data.get("mail_zip", "")
        else:
            for k in ["prop_address","prop_city","prop_state","prop_zip",
                      "mail_address","mail_city","mail_state","mail_zip"]:
                r.setdefault(k, "")
            r.setdefault("prop_state", "SC")
            r.setdefault("mail_state", "SC")

        flags     = compute_flags(r)
        r["flags"]= flags
        r["score"]= compute_score(r, flags)
        enriched.append(r)
    return enriched


# ===========================================================================
# GHL CSV Export
# ===========================================================================

def export_ghl_csv(records: list, path: str):
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
            owner  = r.get("owner", "")
            parts  = owner.split(" ", 1) if owner else ["", ""]
            writer.writerow({
                "First Name":            parts[0] if parts else "",
                "Last Name":             parts[1] if len(parts) > 1 else "",
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", "SC"),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", "SC"),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat_label", ""),
                "Document Type":         r.get("doc_type", ""),
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", ""),
                "Seller Score":          r.get("score", ""),
                "Motivated Seller Flags":"; ".join(r.get("flags", [])),
                "Source":                "Horry County Register of Deeds",
                "Public Records URL":    r.get("clerk_url", ""),
            })
    log.info("GHL CSV → %s (%d rows)", path, len(records))


# ===========================================================================
# Save JSON
# ===========================================================================

def save_records_json(records: list, *paths: str):
    start_date, end_date = date_range_str()
    payload = {
        "fetched_at":   datetime.now().isoformat(),
        "source":       "Horry County Register of Deeds",
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address", "").strip()),
        "records":      records,
    }
    for path in paths:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Saved → %s", path)


# ===========================================================================
# Main
# ===========================================================================

async def main():
    log.info("=" * 60)
    log.info("Horry County Motivated Seller Scraper")
    log.info("Source: Register of Deeds (Acclaim)")
    log.info("Look-back: %d days", LOOK_BACK_DAYS)
    log.info("=" * 60)

    # 1. Load parcel data
    parcel = ParcelLookup()
    log.info("Loading parcel/property data …")
    parcel.load()

    # 2. Scrape Register of Deeds
    scraper = AcclaimScraper()
    raw     = await scraper.scrape()

    # 3. Deduplicate
    seen, unique = set(), []
    for r in raw:
        key = (r.get("doc_num", ""), r.get("doc_type", ""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique records after dedup: %d", len(unique))

    # 4. Enrich + score
    enriched = enrich_records(unique, parcel)
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 5. Save outputs
    repo = Path(__file__).parent.parent
    save_records_json(
        enriched,
        str(repo / "dashboard" / "records.json"),
        str(repo / "data"      / "records.json"),
    )
    export_ghl_csv(enriched, str(repo / "data" / "leads_export.csv"))

    log.info("Done. Total=%d | With address=%d | Avg score=%.0f",
             len(enriched),
             sum(1 for r in enriched if r.get("prop_address","").strip()),
             (sum(r.get("score",0) for r in enriched) / len(enriched)) if enriched else 0)


if __name__ == "__main__":
    asyncio.run(main())
