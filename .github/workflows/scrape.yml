"""
Horry County SC — Motivated Seller Lead Scraper
Targets: Clerk of Court document search + County parcel bulk data
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

# ---------------------------------------------------------------------------
# Optional: playwright (may not be installed in local dev)
# ---------------------------------------------------------------------------
try:
    from playwright.async_api import async_playwright, Page, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not installed — clerk scraping disabled")

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
# Constants
# ---------------------------------------------------------------------------
CLERK_BASE = "https://www.horrycountysc.gov/departments/clerk-of-court/"
CLERK_SEARCH_URL = "https://www2.horrycountysc.gov/PublicRecordsSearch/"

# Horry County GIS / Assessor open-data endpoints (try in order)
PARCEL_CANDIDATES = [
    "https://gis.horrycountysc.gov/arcgis/rest/services/HorryCounty/MapServer/0/query",
    "https://opendata.arcgis.com/datasets/horry-county-parcels.zip",
]

# Document type → category mapping
DOC_CATEGORIES = {
    "LP":       ("LP",       "Lis Pendens"),
    "NOFC":     ("NOFC",     "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED",  "Tax Deed"),
    "JUD":      ("JUD",      "Judgment"),
    "CCJ":      ("JUD",      "Certified Judgment"),
    "DRJUD":    ("JUD",      "Domestic Judgment"),
    "LNCORPTX": ("LN",       "Corp Tax Lien"),
    "LNIRS":    ("LN",       "IRS Lien"),
    "LNFED":    ("LN",       "Federal Lien"),
    "LN":       ("LN",       "Lien"),
    "LNMECH":   ("LN",       "Mechanic Lien"),
    "LNHOA":    ("LN",       "HOA Lien"),
    "MEDLN":    ("LN",       "Medicaid Lien"),
    "PRO":      ("PRO",      "Probate Document"),
    "NOC":      ("NOC",      "Notice of Commencement"),
    "RELLP":    ("RELLP",    "Release Lis Pendens"),
}

TARGET_DOC_TYPES = list(DOC_CATEGORIES.keys())

LOOK_BACK_DAYS = 7
MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds


# ===========================================================================
# Utility helpers
# ===========================================================================

def retry(fn, *args, retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """Synchronous retry wrapper."""
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(delay)
    return None


async def async_retry(coro_fn, *args, retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """Async retry wrapper."""
    for attempt in range(1, retries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                await asyncio.sleep(delay)
    return None


def parse_amount(text: str) -> Optional[float]:
    """Extract dollar amount from text."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def normalize_name(name: str) -> str:
    """Normalize owner name for matching."""
    return re.sub(r"\s+", " ", name.strip().upper())


def name_variants(full_name: str) -> list[str]:
    """Generate lookup variants: FIRST LAST, LAST FIRST, LAST, FIRST."""
    parts = normalize_name(full_name).split()
    if len(parts) < 2:
        return [normalize_name(full_name)]
    first = parts[0]
    last = " ".join(parts[1:])
    return [
        normalize_name(full_name),
        f"{last} {first}",
        f"{last}, {first}",
    ]


def date_range_str() -> tuple[str, str]:
    """Return (start_date, end_date) as MM/DD/YYYY strings."""
    end = datetime.now()
    start = end - timedelta(days=LOOK_BACK_DAYS)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")


# ===========================================================================
# Scoring
# ===========================================================================

def compute_flags(record: dict) -> list[str]:
    flags = []
    cat = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    owner = record.get("owner", "")
    filed = record.get("filed", "")
    amount = record.get("amount")

    if cat == "LP" or doc_type == "LP":
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

    # New this week
    try:
        filed_dt = datetime.strptime(filed, "%Y-%m-%d")
        if (datetime.now() - filed_dt).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    return list(dict.fromkeys(flags))  # deduplicate, preserve order


def compute_score(record: dict, flags: list[str]) -> int:
    score = 30  # base

    for flag in flags:
        if flag in ("Lis pendens", "Pre-foreclosure", "Judgment lien",
                    "Tax lien", "Mechanic lien", "Probate / estate",
                    "LLC / corp owner", "New this week"):
            score += 10

    # LP + FC combo bonus
    cat = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    if "Lis pendens" in flags and ("Pre-foreclosure" in flags or doc_type == "NOFC"):
        score += 20

    amount = record.get("amount")
    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    if "New this week" in flags:
        score += 5

    prop_address = record.get("prop_address", "")
    if prop_address and prop_address.strip():
        score += 5

    return min(score, 100)


# ===========================================================================
# Parcel / Property Appraiser lookup
# ===========================================================================

class ParcelLookup:
    """Loads Horry County parcel data and builds owner-name index."""

    def __init__(self):
        self.index: dict[str, dict] = {}  # normalized_name → parcel dict
        self.loaded = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self):
        """Attempt all known parcel data sources in order."""
        if self._load_from_arcgis_rest():
            return
        if self._load_from_bulk_zip():
            return
        log.warning("Parcel data unavailable — address enrichment skipped")

    def lookup(self, owner_name: str) -> Optional[dict]:
        """Return best matching parcel record or None."""
        if not self.loaded:
            return None
        for variant in name_variants(owner_name):
            result = self.index.get(variant)
            if result:
                return result
        return None

    # ------------------------------------------------------------------
    # Source 1: ArcGIS REST API (paginated JSON)
    # ------------------------------------------------------------------

    def _load_from_arcgis_rest(self) -> bool:
        url = "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Horry_County_Parcel/FeatureServer/0/query"
        params = {
            "where": "1=1",
            "outFields": "OWNER,OWN1,SITE_ADDR,SITEADDR,SITE_CITY,SITE_ZIP,ADDR_1,MAILADR1,CITY,MAILCITY,STATE,ZIP,MAILZIP",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": 0,
            "resultRecordCount": 2000,
        }
        records = []
        try:
            while True:
                resp = retry(requests.get, url, params=params, timeout=30)
                if not resp or resp.status_code != 200:
                    break
                data = resp.json()
                features = data.get("features", [])
                if not features:
                    break
                for feat in features:
                    attrs = feat.get("attributes", {})
                    records.append(self._normalize_parcel(attrs))
                params["resultOffset"] += len(features)
                if len(features) < 2000:
                    break
            if records:
                self._build_index(records)
                log.info("Parcel index built from ArcGIS REST: %d records", len(records))
                return True
        except Exception as exc:
            log.debug("ArcGIS REST parcel load failed: %s", exc)
        return False

    # ------------------------------------------------------------------
    # Source 2: Bulk ZIP / DBF download
    # ------------------------------------------------------------------

    def _load_from_bulk_zip(self) -> bool:
        if not DBFREAD_AVAILABLE:
            return False

        # Known SC open-data parcel endpoints to try
        candidate_urls = [
            "https://gis.horrycountysc.gov/data/parcels.zip",
            "https://opendata.horrycountysc.gov/datasets/parcels.zip",
        ]
        for url in candidate_urls:
            try:
                resp = retry(requests.get, url, timeout=60, stream=True)
                if not resp or resp.status_code != 200:
                    continue
                content = resp.content
                if not content:
                    continue
                records = self._extract_dbf_from_zip(content)
                if records:
                    self._build_index(records)
                    log.info("Parcel index built from ZIP/DBF: %d records", len(records))
                    return True
            except Exception as exc:
                log.debug("ZIP/DBF load failed for %s: %s", url, exc)
        return False

    def _extract_dbf_from_zip(self, content: bytes) -> list[dict]:
        records = []
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                dbf_files = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_files:
                    return []
                dbf_bytes = zf.read(dbf_files[0])
                # Write temp file (dbfread needs a path)
                tmp_path = "/tmp/parcels.dbf"
                with open(tmp_path, "wb") as f:
                    f.write(dbf_bytes)
                table = DBF(tmp_path, encoding="latin-1", ignore_missing_memofile=True)
                for row in table:
                    records.append(self._normalize_parcel(dict(row)))
        except Exception as exc:
            log.debug("DBF extraction failed: %s", exc)
        return records

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _normalize_parcel(self, attrs: dict) -> dict:
        """Map various field name conventions to canonical keys."""
        def pick(*keys):
            for k in keys:
                v = attrs.get(k) or attrs.get(k.upper()) or attrs.get(k.lower())
                if v and str(v).strip():
                    return str(v).strip()
            return ""

        return {
            "owner":        pick("OWNER", "OWN1"),
            "site_addr":    pick("SITE_ADDR", "SITEADDR"),
            "site_city":    pick("SITE_CITY"),
            "site_zip":     pick("SITE_ZIP"),
            "mail_addr":    pick("ADDR_1", "MAILADR1"),
            "mail_city":    pick("CITY", "MAILCITY"),
            "mail_state":   pick("STATE"),
            "mail_zip":     pick("ZIP", "MAILZIP"),
        }

    def _build_index(self, records: list[dict]):
        for rec in records:
            owner = rec.get("owner", "")
            if not owner:
                continue
            for variant in name_variants(owner):
                self.index[variant] = rec
        self.loaded = bool(self.index)


# ===========================================================================
# Clerk of Court — Playwright scraper
# ===========================================================================

class ClerkScraper:
    """
    Scrapes the Horry County Clerk of Court public records search portal.
    Uses Playwright because the portal uses ASP.NET WebForms with __doPostBack.
    """

    SEARCH_URL = "https://www2.horrycountysc.gov/PublicRecordsSearch/SearchDocument.aspx"
    # Fallback if the above 302-redirects
    ALT_URL = "https://www2.horrycountysc.gov/PublicRecordsSearch/"

    def __init__(self):
        self.records: list[dict] = []

    async def scrape(self) -> list[dict]:
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright unavailable — skipping clerk scrape")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()
            try:
                await self._run(page)
            except Exception as exc:
                log.error("Clerk scraper fatal error: %s", exc)
            finally:
                await browser.close()

        log.info("Clerk scraper collected %d raw records", len(self.records))
        return self.records

    async def _run(self, page: "Page"):
        log.info("Navigating to clerk portal …")
        try:
            await page.goto(self.SEARCH_URL, wait_until="networkidle", timeout=30000)
        except Exception:
            try:
                await page.goto(self.ALT_URL, wait_until="networkidle", timeout=30000)
            except Exception as exc:
                log.error("Could not reach clerk portal: %s", exc)
                return

        start_date, end_date = date_range_str()

        for doc_type in TARGET_DOC_TYPES:
            log.info("Searching doc type: %s", doc_type)
            try:
                await async_retry(
                    self._search_doc_type,
                    page, doc_type, start_date, end_date
                )
            except Exception as exc:
                log.warning("Failed searching %s: %s", doc_type, exc)

    async def _search_doc_type(self, page: "Page", doc_type: str,
                                start_date: str, end_date: str):
        """Fill out and submit the clerk search form for one doc type."""
        # Re-navigate to reset form state
        try:
            await page.goto(self.SEARCH_URL, wait_until="networkidle", timeout=20000)
        except Exception:
            pass

        await asyncio.sleep(1)

        # Try to find doc type dropdown
        selectors = [
            "select[id*='DocType']",
            "select[name*='DocType']",
            "select[id*='doctype']",
            "#ctl00_ContentPlaceHolder1_ddlDocumentType",
            "select",
        ]
        dropdown = None
        for sel in selectors:
            try:
                dropdown = await page.wait_for_selector(sel, timeout=3000)
                if dropdown:
                    break
            except Exception:
                pass

        if dropdown:
            try:
                await dropdown.select_option(value=doc_type)
            except Exception:
                try:
                    await dropdown.select_option(label=doc_type)
                except Exception:
                    log.debug("Could not select doc type %s", doc_type)

        # Fill date range
        await self._fill_date(page, ["input[id*='StartDate']", "input[name*='StartDate']",
                                     "input[id*='FromDate']", "#ctl00_ContentPlaceHolder1_txtFromDate"],
                              start_date)
        await self._fill_date(page, ["input[id*='EndDate']", "input[name*='EndDate']",
                                     "input[id*='ToDate']", "#ctl00_ContentPlaceHolder1_txtToDate"],
                              end_date)

        # Submit
        submit_selectors = [
            "input[type='submit'][value*='Search']",
            "input[type='submit']",
            "button[type='submit']",
            "#ctl00_ContentPlaceHolder1_btnSearch",
        ]
        for sel in submit_selectors:
            try:
                btn = await page.wait_for_selector(sel, timeout=3000)
                if btn:
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    break
            except Exception:
                pass

        await asyncio.sleep(1)

        # Parse results pages
        await self._parse_results_pages(page, doc_type)

    async def _fill_date(self, page: "Page", selectors: list[str], value: str):
        for sel in selectors:
            try:
                field = await page.wait_for_selector(sel, timeout=2000)
                if field:
                    await field.triple_click()
                    await field.type(value)
                    return
            except Exception:
                pass

    async def _parse_results_pages(self, page: "Page", doc_type: str):
        page_num = 1
        while True:
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            rows_found = self._parse_results_table(soup, doc_type, page.url)

            if rows_found == 0:
                break

            # Check for next page
            next_btn = await self._find_next_button(page, soup)
            if not next_btn:
                break

            log.info("  → page %d, found %d rows; going to next page", page_num, rows_found)
            try:
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                await asyncio.sleep(1)
                page_num += 1
            except Exception as exc:
                log.debug("Pagination failed: %s", exc)
                break

    async def _find_next_button(self, page: "Page", soup: BeautifulSoup):
        next_selectors = [
            "a:has-text('Next')",
            "input[value='Next']",
            "a[id*='Next']",
            ".pagination a:last-child",
        ]
        for sel in next_selectors:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    is_disabled = await btn.get_attribute("disabled")
                    if not is_disabled:
                        return btn
            except Exception:
                pass
        return None

    def _parse_results_table(self, soup: BeautifulSoup, doc_type: str, current_url: str) -> int:
        """Parse result rows from HTML table. Returns count of rows found."""
        tables = soup.find_all("table")
        best_table = None
        best_count = 0
        for tbl in tables:
            rows = tbl.find_all("tr")
            if len(rows) > best_count:
                best_count = len(rows)
                best_table = tbl

        if not best_table or best_count < 2:
            return 0

        headers = []
        header_row = best_table.find("tr")
        if header_row:
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

        data_rows = best_table.find_all("tr")[1:]
        count = 0
        for row in data_rows:
            cells = row.find_all("td")
            if not cells:
                continue
            try:
                record = self._parse_row(cells, headers, doc_type, current_url, row)
                if record:
                    self.records.append(record)
                    count += 1
            except Exception as exc:
                log.debug("Row parse error: %s", exc)

        return count

    def _parse_row(self, cells, headers, doc_type, current_url, row) -> Optional[dict]:
        """Map table cells to record dict."""
        def cell_text(idx):
            if idx < len(cells):
                return cells[idx].get_text(strip=True)
            return ""

        def find_col(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return cell_text(i)
            return ""

        # Try header-based extraction first
        doc_num  = find_col("doc", "instrument", "book", "number")
        filed    = find_col("date", "filed", "recorded")
        grantor  = find_col("grantor", "owner", "seller")
        grantee  = find_col("grantee", "buyer")
        legal    = find_col("legal", "description")
        amount   = find_col("amount", "consideration")

        # Fallback: positional extraction (common pattern: col 0=docnum, 1=type, 2=date, 3=grantor, 4=grantee, 5=legal, 6=amount)
        if not doc_num and len(cells) >= 3:
            doc_num  = cell_text(0)
            filed    = cell_text(2)
            grantor  = cell_text(3) if len(cells) > 3 else ""
            grantee  = cell_text(4) if len(cells) > 4 else ""
            legal    = cell_text(5) if len(cells) > 5 else ""
            amount   = cell_text(6) if len(cells) > 6 else ""

        # Build direct link
        link_tag = row.find("a", href=True)
        if link_tag:
            href = link_tag["href"]
            if href.startswith("http"):
                clerk_url = href
            else:
                clerk_url = f"https://www2.horrycountysc.gov{href}" if href.startswith("/") else current_url
        else:
            clerk_url = current_url

        # Normalize filed date
        filed_norm = self._normalize_date(filed)

        if not doc_num and not grantor:
            return None

        cat, cat_label = DOC_CATEGORIES.get(doc_type, ("MISC", doc_type))

        return {
            "doc_num":  doc_num,
            "doc_type": doc_type,
            "filed":    filed_norm,
            "cat":      cat,
            "cat_label":cat_label,
            "owner":    grantor,
            "grantee":  grantee,
            "amount":   parse_amount(amount),
            "legal":    legal,
            "clerk_url":clerk_url,
        }

    @staticmethod
    def _normalize_date(raw: str) -> str:
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return raw.strip()


# ===========================================================================
# Enrichment — merge parcel data into clerk records
# ===========================================================================

def enrich_records(records: list[dict], parcel: ParcelLookup) -> list[dict]:
    """Add property + mailing addresses from parcel lookup."""
    enriched = []
    for rec in records:
        r = dict(rec)
        owner = r.get("owner", "")
        parcel_data = parcel.lookup(owner) if owner else None

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
            r.setdefault("prop_address", "")
            r.setdefault("prop_city", "")
            r.setdefault("prop_state", "SC")
            r.setdefault("prop_zip", "")
            r.setdefault("mail_address", "")
            r.setdefault("mail_city", "")
            r.setdefault("mail_state", "SC")
            r.setdefault("mail_zip", "")

        # Compute flags and score
        flags = compute_flags(r)
        r["flags"] = flags
        r["score"] = compute_score(r, flags)
        enriched.append(r)

    return enriched


# ===========================================================================
# GHL CSV Export
# ===========================================================================

def export_ghl_csv(records: list[dict], path: str):
    """Export GoHighLevel-compatible CSV."""
    fieldnames = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]

    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in records:
            owner = r.get("owner", "")
            parts = owner.split(" ", 1) if owner else ["", ""]
            first = parts[0] if parts else ""
            last  = parts[1] if len(parts) > 1 else ""

            writer.writerow({
                "First Name":           first,
                "Last Name":            last,
                "Mailing Address":      r.get("mail_address", ""),
                "Mailing City":         r.get("mail_city", ""),
                "Mailing State":        r.get("mail_state", "SC"),
                "Mailing Zip":          r.get("mail_zip", ""),
                "Property Address":     r.get("prop_address", ""),
                "Property City":        r.get("prop_city", ""),
                "Property State":       r.get("prop_state", "SC"),
                "Property Zip":         r.get("prop_zip", ""),
                "Lead Type":            r.get("cat_label", ""),
                "Document Type":        r.get("doc_type", ""),
                "Date Filed":           r.get("filed", ""),
                "Document Number":      r.get("doc_num", ""),
                "Amount/Debt Owed":     r.get("amount", ""),
                "Seller Score":         r.get("score", ""),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":               "Horry County Clerk of Court",
                "Public Records URL":   r.get("clerk_url", ""),
            })

    log.info("GHL CSV exported → %s (%d rows)", path, len(records))


# ===========================================================================
# Save records.json
# ===========================================================================

def save_records_json(records: list[dict], *paths: str):
    start_date, end_date = date_range_str()
    with_address = sum(1 for r in records if r.get("prop_address", "").strip())

    payload = {
        "fetched_at":   datetime.now().isoformat(),
        "source":       "Horry County SC Clerk of Court",
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }

    for path in paths:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Records saved → %s", path)


# ===========================================================================
# Main
# ===========================================================================

async def main():
    log.info("=" * 60)
    log.info("Horry County Motivated Seller Scraper")
    log.info("Look-back: %d days", LOOK_BACK_DAYS)
    log.info("=" * 60)

    # 1. Load parcel data
    parcel = ParcelLookup()
    log.info("Loading parcel data …")
    parcel.load()

    # 2. Scrape clerk of court
    clerk = ClerkScraper()
    raw_records = await clerk.scrape()

    # 3. Deduplicate by doc_num
    seen = set()
    unique = []
    for r in raw_records:
        key = (r.get("doc_num", ""), r.get("doc_type", ""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique records after dedup: %d", len(unique))

    # 4. Enrich with parcel addresses + flags + score
    enriched = enrich_records(unique, parcel)

    # Sort by score descending
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 5. Save outputs
    repo_root = Path(__file__).parent.parent
    save_records_json(
        enriched,
        str(repo_root / "dashboard" / "records.json"),
        str(repo_root / "data" / "records.json"),
    )

    export_ghl_csv(enriched, str(repo_root / "data" / "leads_export.csv"))

    log.info("Done. Total leads: %d | With address: %d",
             len(enriched),
             sum(1 for r in enriched if r.get("prop_address", "").strip()))


if __name__ == "__main__":
    asyncio.run(main())
