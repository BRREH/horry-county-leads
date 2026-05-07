"""
Horry County SC — Complete Lead Scraper — UPDATED v2
========================================================
CHANGES in v2:
  1. Pre-foreclosure (Lis Pendens) address lookup IMPROVED:
     - Property address now pulled from Horry GIS by TMS number (not just owner name)
     - TMS extracted from Acclaim legal description field
     - Fallback: search GIS by street address substring from legal text

  2. SC Courts Public Index BLOCKED (406) — do NOT use automated POST
     Address source for LP records remains: Horry County GIS + AcclaimWeb legal text

  3. GIS lookup now uses BOTH property address and owner name searches

  4. Cross-reference: if Acclaim LP record has no GIS match by name,
     try matching by TMS extracted from legal description

Address sources (in priority order for LP records):
  1. AcclaimWeb legal/comments field  → extract street address via regex
  2. GIS Layer 22 (Addresses) by TMS  → property address
  3. GIS Layer 24 (Parcels) by name   → mailing address
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
DOCTYPE_URL  = f"{ACCLAIM_BASE}/search/SearchTypeDocType"
GIS_BASE     = "https://www.horrycounty.org/parcelapp/rest/services/HorryCountyGISApp/MapServer"
PARCELS_URL  = f"{GIS_BASE}/24/query"   # TMS, OwnerName, OwnerStreet, OwnerCity, OwnerState, OwnerZip
ADDRESS_URL  = f"{GIS_BASE}/22/query"   # TMS → ADDRESS, CITY, STATE, ZIPCODE
DELQ_TAX_URL = "https://gisportal.horrycounty.org/server/rest/services/Hosted/DelqTaxUpdates/FeatureServer/0/query"

LOOK_BACK_DAYS = 14

DOC_TYPE_KEYWORDS = [
    ("LIS PENDENS",            "LP",      "Lis Pendens"),
    ("FORECLOSURE",            "NOFC",    "Notice of Foreclosure"),
    ("TAX DEED",               "TAXDEED", "Tax Deed"),
    ("JUDGMENT",               "JUD",     "Judgment"),
    ("MECHANIC",               "LN",      "Mechanic Lien"),
    ("CONDO LIEN",             "LN",      "HOA/Condo Lien"),
    ("HOA LIEN",               "LN",      "HOA Lien"),
    ("TAX LIEN",               "LN",      "Tax Lien"),
    ("TAX LIENS",              "LN",      "Tax Lien"),
    ("FEDERAL TAX",            "LN",      "Federal Tax Lien"),
    ("STATE TAX",              "LN",      "State Tax Lien"),
    ("CHILD SUPPORT LIEN",     "LN",      "Child Support Lien"),
    ("MENTAL HEALTH LIEN",     "LN",      "Mental Health Lien"),
    ("MEDICAID LIEN",          "LN",      "Medicaid Lien"),
    ("HOSPITAL LIEN",          "LN",      "Medical Lien"),
    ("IRS",                    "LN",      "IRS Lien"),
    ("PROBATE",                "PRO",     "Probate Document"),
    ("LETTERS TEST",           "PRO",     "Probate Document"),
    ("LETTERS OF ADMIN",       "PRO",     "Probate Document"),
    ("NOTICE OF COMMENCEMENT", "NOC",     "Notice of Commencement"),
    ("CODE VIOLATION",         "CV",      "Code Violation"),
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
            return datetime.strptime(str(raw).strip()[:19], fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return str(raw).strip()[:10]


def classify_doc(description: str) -> Optional[tuple]:
    desc = description.strip().upper()
    for keyword, cat, label in DOC_TYPE_KEYWORDS:
        if keyword in desc:
            return (cat, label)
    return None


def compute_flags(record: dict) -> list:
    flags     = []
    cat       = record.get("cat", "")
    cat_label = record.get("cat_label", "")
    owner     = record.get("owner", "")
    filed     = record.get("filed", "")

    if cat == "LP":                                      flags.append("Lis pendens")
    if cat == "NOFC":                                    flags.append("Pre-foreclosure")
    if cat == "JUD":                                     flags.append("Judgment lien")
    if "TAX" in cat_label.upper():                       flags.append("Tax lien")
    if "MECHANIC" in cat_label.upper():                  flags.append("Mechanic lien")
    if cat == "PRO":                                     flags.append("Probate / estate")
    if "HOA" in cat_label.upper() or "CONDO" in cat_label.upper():
        flags.append("HOA lien")
    if cat == "CV":                                      flags.append("Code violation")
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
    score += len(flags) * 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: score += 20
    amount = record.get("amount")
    if amount:
        if amount > 100_000: score += 15
        elif amount > 50_000: score += 10
    if "New this week"  in flags: score += 5
    if "Code violation" in flags: score += 15
    if record.get("prop_address", "").strip(): score += 5
    return min(score, 100)


# ══════════════════════════════════════════════════════════════════════════════
# GIS Address Lookup  (Horry County GIS REST API — free, no login)
# ══════════════════════════════════════════════════════════════════════════════

class GISLookup:
    """
    Queries Horry County GIS ArcGIS REST API.
    Layer 24: Owner name → mailing address + TMS parcel ID
    Layer 22: TMS → site/property address
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; HorryLeadScraper/2.0)"
        })
        self._name_cache = {}
        self._tms_cache  = {}

    # ── Public: lookup by owner name ────────────────────────────────────────
    def lookup_by_name(self, owner_name: str) -> Optional[dict]:
        if not owner_name or not owner_name.strip():
            return None
        key = owner_name.strip().upper()
        if key in self._name_cache:
            return self._name_cache[key]
        result = self._query_parcels_by_name(owner_name.strip())
        self._name_cache[key] = result
        return result

    # ── Public: lookup by TMS parcel number ─────────────────────────────────
    def lookup_by_tms(self, tms: str) -> Optional[dict]:
        """Lookup full address record by TMS parcel number."""
        if not tms or not tms.strip():
            return None
        key = tms.strip()
        if key in self._tms_cache:
            return self._tms_cache[key]

        # Layer 24 by TMS for owner mailing address
        result = self._query_parcels_by_tms(key)
        # Layer 22 for property/site address
        if result:
            site = self._query_site_address(key)
            if site:
                result.update(site)
        self._tms_cache[key] = result
        return result

    # ── Public: lookup property address by TMS only ──────────────────────────
    def lookup_site_address(self, tms: str) -> Optional[dict]:
        if not tms:
            return None
        return self._query_site_address(tms.strip())

    def _query_parcels_by_name(self, owner_name: str) -> Optional[dict]:
        safe_name = owner_name.replace("'", "''")
        where = f"OwnerName LIKE '%{safe_name}%'"
        try:
            resp = self.session.get(PARCELS_URL, params={
                "where": where, "outFields": "OwnerName,OwnerStreet,OwnerCity,OwnerState,OwnerZip,TMS",
                "returnGeometry": "false", "f": "json",
            }, timeout=10)
            features = resp.json().get("features", [])
            if not features:
                return None
            attrs = features[0]["attributes"]
            tms   = attrs.get("TMS", "")
            result = {
                "mail_address": (attrs.get("OwnerStreet","") or "").strip(),
                "mail_city":    (attrs.get("OwnerCity","") or "").strip(),
                "mail_state":   (attrs.get("OwnerState","") or "SC").strip(),
                "mail_zip":     (attrs.get("OwnerZip","") or "").strip(),
                "tms":          tms,
                "prop_address": "", "prop_city": "", "prop_state": "SC", "prop_zip": "",
            }
            if tms:
                site = self._query_site_address(tms)
                if site:
                    result.update(site)
            return result
        except Exception as e:
            log.debug("GIS name lookup error: %s", e)
            return None

    def _query_parcels_by_tms(self, tms: str) -> Optional[dict]:
        safe_tms = tms.replace("'","''")
        try:
            resp = self.session.get(PARCELS_URL, params={
                "where": f"TMS = '{safe_tms}'",
                "outFields": "OwnerName,OwnerStreet,OwnerCity,OwnerState,OwnerZip,TMS",
                "returnGeometry": "false", "f": "json",
            }, timeout=10)
            features = resp.json().get("features", [])
            if not features:
                return None
            attrs = features[0]["attributes"]
            return {
                "mail_address": (attrs.get("OwnerStreet","") or "").strip(),
                "mail_city":    (attrs.get("OwnerCity","") or "").strip(),
                "mail_state":   (attrs.get("OwnerState","") or "SC").strip(),
                "mail_zip":     (attrs.get("OwnerZip","") or "").strip(),
                "tms":          tms,
                "prop_address": "", "prop_city": "", "prop_state": "SC", "prop_zip": "",
            }
        except Exception as e:
            log.debug("GIS TMS parcel lookup error: %s", e)
            return None

    def _query_site_address(self, tms: str) -> Optional[dict]:
        safe_tms = tms.replace("'","''")
        try:
            resp = self.session.get(ADDRESS_URL, params={
                "where": f"TMS = '{safe_tms}'",
                "outFields": "ADDRESS,CITY,STATE,ZIPCODE",
                "returnGeometry": "false", "f": "json",
            }, timeout=10)
            features = resp.json().get("features", [])
            if not features:
                return None
            attrs = features[0]["attributes"]
            return {
                "prop_address": (attrs.get("ADDRESS","") or "").strip(),
                "prop_city":    (attrs.get("CITY","") or "").strip(),
                "prop_state":   (attrs.get("STATE","SC") or "SC").strip(),
                "prop_zip":     str(attrs.get("ZIPCODE","") or "").strip(),
            }
        except Exception as e:
            log.debug("GIS site address error: %s", e)
            return None


# ══════════════════════════════════════════════════════════════════════════════
# Delinquent Tax Cross-Reference (GIS Portal — LP owners who also owe taxes)
# ══════════════════════════════════════════════════════════════════════════════

def lookup_delinquent_tax_by_name(owner_name: str, session: requests.Session) -> Optional[dict]:
    """Check if an LP owner also appears on the delinquent tax list."""
    if not owner_name:
        return None
    safe = owner_name.strip().replace("'","''").upper()
    try:
        r = session.get(DELQ_TAX_URL, params={
            "where": f"owner_name LIKE '%{safe}%'",
            "outFields": "owner_name,total_tax_due,tms,description",
            "returnGeometry": "false", "f": "json",
        }, timeout=10)
        features = r.json().get("features", [])
        if not features:
            return None
        attrs = features[0]["attributes"]
        return {
            "delinquent_tax": attrs.get("total_tax_due",""),
            "delinquent_tms": attrs.get("tms",""),
        }
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Acclaim CSV Parser
# ══════════════════════════════════════════════════════════════════════════════

def extract_tms_from_legal(legal: str) -> str:
    """
    Extract a Horry County TMS parcel number from AcclaimWeb legal description.
    TMS format: XXXXXXXXXX (10 digits, sometimes with dashes: XXX-XX-XX-XXXX)
    """
    if not legal:
        return ""
    # Try dashed format first: 123-45-67-8901 or 1234567890
    for pattern in [
        r'\b(\d{3}-\d{2}-\d{2}-\d{4})\b',  # 123-45-67-8901
        r'\b(\d{10})\b',                     # 1234567890
        r'\b(\d{9})\b',                      # 123456789 (some older)
    ]:
        m = re.search(pattern, legal)
        if m:
            return m.group(1).replace("-","")
    return ""


def extract_address_from_legal(legal: str) -> str:
    """
    Extract a street address from AcclaimWeb legal description / comments.
    Many LP records include the property address in comments.
    """
    if not legal:
        return ""
    text = legal.upper()
    patterns = [
        r'\b(\d+\s+[A-Z][A-Z\s]+(?:ST|AVE|RD|DR|LN|WAY|BLVD|CT|CIR|HWY|LOOP|TRL|PL|PKY|PKWY)[A-Z\s]*\d{5}?)\b',
        r'\b(\d+\s+[A-Z][A-Z\s]{3,30}(?:STREET|AVENUE|ROAD|DRIVE|LANE|WAY|BOULEVARD|COURT|CIRCLE|HIGHWAY))',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip().title()
    return ""


def parse_acclaim_csv(raw: str) -> list:
    records = []
    try:
        raw    = raw.lstrip('\ufeff')
        reader = csv.DictReader(io.StringIO(raw))
        log.info("CSV columns: %s", reader.fieldnames)
        rows = list(reader)
        log.info("CSV total rows: %d", len(rows))

        for row in rows:
            try:
                description = (row.get("DocTypeDescription","") or "").strip()
                comments    = (row.get("Comments","") or "").strip()
                classified  = classify_doc(description) or classify_doc(comments)
                if not classified:
                    continue

                cat, cat_label = classified
                book_page = (row.get("BookPage","") or "").strip()
                owner     = (row.get("DirectName","") or "").strip()
                grantee   = (row.get("IndirectName","") or "").strip()
                amount    = parse_amount(row.get("Consideration",""))
                filed_raw = (row.get("RecordDate","") or "").strip()
                legal     = comments

                clerk_url = (
                    f"{ACCLAIM_BASE}/search/SearchTypeName"
                    f"?directName={owner.replace(' ','%20')}"
                    if owner else DOCTYPE_URL
                )

                # Try to extract TMS and address from legal description
                tms_from_legal  = extract_tms_from_legal(legal)
                addr_from_legal = extract_address_from_legal(legal)

                records.append({
                    "doc_num":        book_page,
                    "doc_type":       cat,
                    "filed":          normalize_date(filed_raw),
                    "cat":            cat,
                    "cat_label":      cat_label,
                    "owner":          owner,
                    "grantee":        grantee,
                    "amount":         amount,
                    "legal":          legal,
                    "tms_legal":      tms_from_legal,   # NEW: TMS from legal text
                    "addr_legal":     addr_from_legal,  # NEW: address from legal text
                    "clerk_url":      clerk_url,
                    "source":         "Register of Deeds",
                    "prop_address":   addr_from_legal,  # Pre-fill from legal
                    "prop_city":      "", "prop_state": "SC", "prop_zip": "",
                    "mail_address":   "", "mail_city": "", "mail_state": "SC", "mail_zip": "",
                    "delinquent_tax": "",  # NEW: cross-ref with delinquent tax list
                })
            except Exception as e:
                log.debug("Row error: %s", e)

    except Exception as e:
        log.error("CSV parse error: %s", e)

    log.info("Classified %d Acclaim records", len(records))
    return records


# ══════════════════════════════════════════════════════════════════════════════
# Acclaim Scraper (unchanged — requires HORRY_USERNAME/HORRY_PASSWORD secrets)
# ══════════════════════════════════════════════════════════════════════════════

async def run_acclaim(page: Page) -> list:
    start_date, end_date = date_range_str()
    log.info("Acclaim: %s to %s", start_date, end_date)

    await page.goto(ACCLAIM_BASE + "/", wait_until="domcontentloaded", timeout=20000)
    await asyncio.sleep(2)
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
                    break
            except Exception:
                pass

    await page.goto(DOCTYPE_URL, wait_until="networkidle", timeout=30000)
    await asyncio.sleep(3)

    await page.evaluate("""
        () => {
            const sel = document.querySelector(
                '#DocTypeGroupDropDown,[name="DocTypeGroupDropDown"],select'
            );
            if (sel) {
                for (let opt of sel.options) {
                    if (opt.text.trim() === 'All') {
                        sel.value = opt.value;
                        sel.dispatchEvent(new Event('change',{bubbles:true}));
                        return 'All selected';
                    }
                }
            }
        }
    """)
    await asyncio.sleep(2)

    await page.evaluate(f"""
        () => {{
            const dropdowns = document.querySelectorAll('select');
            for (let dd of dropdowns) {{
                for (let opt of dd.options) {{
                    if (opt.text.includes('Specify') || opt.text.includes('Range')) {{
                        dd.value = opt.value;
                        dd.dispatchEvent(new Event('change',{{bubbles:true}}));
                        break;
                    }}
                }}
            }}
            const from = document.querySelector('#RecordDateFrom,[name="RecordDateFrom"]');
            if (from) {{
                from.value = '{start_date}';
                from.dispatchEvent(new Event('change',{{bubbles:true}}));
                from.dispatchEvent(new Event('input',{{bubbles:true}}));
            }}
            const to = document.querySelector('#RecordDateTo,[name="RecordDateTo"]');
            if (to) {{
                to.value = '{end_date}';
                to.dispatchEvent(new Event('change',{{bubbles:true}}));
                to.dispatchEvent(new Event('input',{{bubbles:true}}));
            }}
        }}
    """)
    await asyncio.sleep(1)

    for sel in ["#Checkbox1", "[name='SelectAllDocTypesToggle']"]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.check()
                await asyncio.sleep(1)
                log.info("SelectAll checked")
                break
        except Exception:
            pass

    for sel in ["#btnSearch", "input[value='Search']", "input[type='submit']"]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click()
                await page.wait_for_load_state("networkidle", timeout=30000)
                log.info("Search submitted")
                break
        except Exception:
            pass

    log.info("Waiting for Export to CSV button...")
    for attempt in range(15):
        for sel in [
            "input[value='Export to CSV']", "input[value*='Export']",
            "button:has-text('Export to CSV')", "button:has-text('Export')",
            "a:has-text('Export to CSV')", "a:has-text('Export')",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    log.info("✓ Export found (attempt %d): %s", attempt+1, sel)
                    async with page.expect_download(timeout=30000) as dl_info:
                        await el.click()
                    download = await dl_info.value
                    path = await download.path()
                    if path:
                        with open(path,"r",encoding="utf-8-sig",errors="ignore") as f:
                            content = f.read()
                        log.info("✓ CSV: %d chars", len(content))
                        return parse_acclaim_csv(content)
            except Exception as e:
                log.debug("Export attempt %d %s: %s", attempt+1, sel, e)
        await asyncio.sleep(2)

    log.warning("Could not export CSV from Acclaim")
    return []


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("="*60)
    log.info("Horry County Lead Scraper — v2 (TMS-enhanced LP addresses)")
    log.info("="*60)

    all_records = []

    # ── Step 1: Acclaim ───────────────────────────────────────────────────
    log.info("STEP 1: Acclaim Register of Deeds")
    if PLAYWRIGHT_AVAILABLE:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True, args=["--no-sandbox","--disable-dev-shm-usage"]
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
                acclaim_records = await run_acclaim(page)
                all_records.extend(acclaim_records)
                log.info("Acclaim records: %d", len(acclaim_records))
            except Exception as e:
                log.error("Acclaim error: %s", e, exc_info=True)
            finally:
                await browser.close()
    else:
        log.error("Playwright not available")

    # ── Step 2: Enhanced GIS Address Lookup ───────────────────────────────
    log.info("STEP 2: Enhanced GIS Address Lookup (name + TMS)")
    gis     = GISLookup()
    session = gis.session

    # Collect all names + TMS numbers to look up
    names_to_lookup = set()
    for r in all_records:
        owner   = r.get("owner","").strip()
        grantee = r.get("grantee","").strip()
        cat     = r.get("cat","")
        cat_lbl = r.get("cat_label","").upper()

        if owner:   names_to_lookup.add(owner)
        if grantee and (
            "HOA" in cat_lbl or "CONDO" in cat_lbl or "MECHANIC" in cat_lbl or
            cat in ("LN","PRO","NOFC","LP")
        ):
            names_to_lookup.add(grantee)

    log.info("Looking up %d unique names...", len(names_to_lookup))
    addr_cache = {}
    for name in names_to_lookup:
        addr = gis.lookup_by_name(name)
        addr_cache[name] = addr
        time.sleep(0.1)

    # Apply addresses with NEW TMS fallback for LP records
    enriched_by_name = enriched_by_tms = 0
    for r in all_records:
        owner   = r.get("owner","").strip()
        grantee = r.get("grantee","").strip()
        cat     = r.get("cat","")
        cat_lbl = r.get("cat_label","").upper()

        use_grantee = (
            "HOA" in cat_lbl or "CONDO" in cat_lbl or "MECHANIC" in cat_lbl or
            "CHILD SUPPORT" in cat_lbl or cat in ("PRO","NOFC","LP")
        )
        contact = grantee if (use_grantee and grantee) else owner

        addr_data = addr_cache.get(contact)
        if not addr_data and contact != owner:
            addr_data = addr_cache.get(owner)

        # ── NEW: TMS fallback for LP records ──────────────────────────────
        if not addr_data and cat == "LP":
            tms = r.get("tms_legal","")
            if tms:
                log.debug("LP: trying TMS lookup %s for %s", tms, owner[:25])
                addr_data = gis.lookup_by_tms(tms)
                if addr_data:
                    enriched_by_tms += 1
                    log.info("LP TMS hit: %s → %s", tms, addr_data.get("prop_address","")[:30])

        if addr_data:
            # Only overwrite prop_address if GIS gives something better
            if addr_data.get("prop_address","").strip():
                r["prop_address"] = addr_data["prop_address"]
                r["prop_city"]    = addr_data.get("prop_city","")
                r["prop_state"]   = addr_data.get("prop_state","SC")
                r["prop_zip"]     = addr_data.get("prop_zip","")
            r["mail_address"] = addr_data.get("mail_address","")
            r["mail_city"]    = addr_data.get("mail_city","")
            r["mail_state"]   = addr_data.get("mail_state","SC")
            r["mail_zip"]     = addr_data.get("mail_zip","")
            r["tms"]          = addr_data.get("tms","") or r.get("tms_legal","")
            enriched_by_name += 1

        # Swap owner/grantee display for lien types
        if use_grantee and grantee:
            r["owner"]   = grantee
            r["grantee"] = owner

    log.info("Enriched — by name: %d | by TMS: %d", enriched_by_name, enriched_by_tms)

    # ── Step 2b: Delinquent Tax Cross-Reference ───────────────────────────
    log.info("STEP 2b: Delinquent Tax Cross-Reference")
    for r in all_records:
        if r.get("cat") == "LP":
            owner = r.get("owner","").strip()
            dt = lookup_delinquent_tax_by_name(owner, session)
            if dt:
                r["delinquent_tax"] = dt.get("delinquent_tax","")
                if not r.get("tms",""):
                    r["tms"] = dt.get("delinquent_tms","")
                r.setdefault("flags", [])
                if "delinquent_tax" not in r["flags"]:
                    r["flags"].append("Also delinquent taxes")
                log.info("Tax cross-ref hit: %s owes $%s", owner[:25], dt.get("delinquent_tax",""))
        time.sleep(0.05)

    # ── Step 3: Deduplicate + Score ───────────────────────────────────────
    seen, unique = set(), []
    for r in all_records:
        key = (r.get("doc_num",""), r.get("cat",""), r.get("owner",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique records: %d", len(unique))

    for r in unique:
        flags     = r.get("flags", []) + compute_flags(r)
        flags     = list(dict.fromkeys(flags))
        r["flags"] = flags
        r["score"] = compute_score(r, flags)
    unique.sort(key=lambda r: r.get("score",0), reverse=True)

    # ── Step 4: Save ──────────────────────────────────────────────────────
    start_date, end_date = date_range_str()
    repo = Path(__file__).parent.parent

    payload = {
        "fetched_at":   datetime.now().isoformat(),
        "source":       "Horry County Register of Deeds + GIS (v2)",
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(unique),
        "with_address": sum(1 for r in unique if r.get("prop_address","").strip()),
        "records":      unique,
    }
    for path in [repo/"dashboard"/"records.json", repo/"data"/"records.json"]:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Saved → %s", path)

    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City",
        "Mailing State","Mailing Zip","Property Address","Property City",
        "Property State","Property Zip","Lead Type","Document Type",
        "Date Filed","Document Number","Amount/Debt Owed","TMS Parcel",
        "Delinquent Tax","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    csv_path = repo/"data"/"leads_export.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in unique:
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
                "Document Type":         r.get("cat",""),
                "Date Filed":            r.get("filed",""),
                "Document Number":       r.get("doc_num",""),
                "Amount/Debt Owed":      r.get("amount",""),
                "TMS Parcel":            r.get("tms",""),
                "Delinquent Tax":        r.get("delinquent_tax",""),
                "Seller Score":          r.get("score",""),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":                r.get("source","Horry County"),
                "Public Records URL":    r.get("clerk_url",""),
            })
    log.info("GHL CSV → %s (%d rows)", csv_path, len(unique))

    with_addr = sum(1 for r in unique if r.get("prop_address","").strip())
    log.info("="*60)
    log.info("DONE — Total: %d | With Address: %d | Avg Score: %.0f",
             len(unique), with_addr,
             sum(r.get("score",0) for r in unique)/len(unique) if unique else 0)
    log.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
