"""
Horry County SC — Complete Lead Scraper — FINAL
Address source: Horry County GIS ArcGIS REST API (free, no login, instant)
  Layer 24 (Parcels): OwnerName, OwnerStreet, OwnerCity, OwnerState, OwnerZip, TMS
  Layer 22 (Addresses): ADDRESS, CITY, STATE, ZIPCODE (site/property address by TMS)
API: https://www.horrycounty.org/parcelapp/rest/services/HorryCountyGISApp/MapServer
"""

import asyncio
import csv
import io
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

ACCLAIM_BASE = "https://acclaimweb.horrycounty.org/AcclaimWeb"
DOCTYPE_URL  = f"{ACCLAIM_BASE}/search/SearchTypeDocType"
GIS_BASE     = "https://www.horrycounty.org/parcelapp/rest/services/HorryCountyGISApp/MapServer"
PARCELS_URL  = f"{GIS_BASE}/24/query"   # Owner name → mailing address + TMS
ADDRESS_URL  = f"{GIS_BASE}/22/query"   # TMS → site/property address
LOOK_BACK_DAYS = 7

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
# GIS Address Lookup — FREE REST API, no login needed
# ══════════════════════════════════════════════════════════════════════════════

class GISLookup:
    """
    Queries Horry County GIS ArcGIS REST API.
    Layer 24: Owner name → mailing address + TMS parcel ID
    Layer 22: TMS → site/property address
    Completely free, no login, instant JSON responses.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; HorryLeadScraper/1.0)"
        })
        self._cache = {}

    def lookup(self, owner_name: str) -> Optional[dict]:
        """Return address data for owner name or None."""
        if not owner_name or not owner_name.strip():
            return None
        key = owner_name.strip().upper()
        if key in self._cache:
            return self._cache[key]

        result = self._query_parcels(owner_name.strip())
        self._cache[key] = result
        return result

    def _query_parcels(self, owner_name: str) -> Optional[dict]:
        """Query Layer 24 by owner name."""
        # Escape single quotes in owner name
        safe_name = owner_name.replace("'", "''")
        # Use LIKE with wildcards for partial matching
        where = f"OwnerName LIKE '%{safe_name}%'"

        try:
            resp = self.session.get(PARCELS_URL, params={
                "where":          where,
                "outFields":      "OwnerName,OwnerStreet,OwnerCity,OwnerState,OwnerZip,TMS",
                "returnGeometry": "false",
                "f":              "json",
            }, timeout=10)

            if resp.status_code != 200:
                log.debug("GIS parcel query failed: %d", resp.status_code)
                return None

            data     = resp.json()
            features = data.get("features", [])

            if not features:
                log.debug("No parcel found for: %s", owner_name[:30])
                return None

            # Take first match
            attrs = features[0]["attributes"]
            tms   = attrs.get("TMS", "")

            result = {
                "mail_address": (attrs.get("OwnerStreet","") or "").strip(),
                "mail_city":    (attrs.get("OwnerCity","") or "").strip(),
                "mail_state":   (attrs.get("OwnerState","") or "SC").strip(),
                "mail_zip":     (attrs.get("OwnerZip","") or "").strip(),
                "tms":          tms,
                # Will fill prop_address from Layer 22
                "prop_address": "",
                "prop_city":    "",
                "prop_state":   "SC",
                "prop_zip":     "",
            }

            # Now get the site/property address from Layer 22
            if tms:
                site = self._query_site_address(tms)
                if site:
                    result.update(site)

            log.info("GIS found: %s → mail: %s | site: %s",
                     owner_name[:25],
                     result.get("mail_address","")[:30],
                     result.get("prop_address","")[:30])
            return result

        except Exception as e:
            log.debug("GIS lookup error for %s: %s", owner_name[:25], e)
            return None

    def _query_site_address(self, tms: str) -> Optional[dict]:
        """Query Layer 22 by TMS to get site/property address."""
        try:
            safe_tms = tms.replace("'","''")
            resp = self.session.get(ADDRESS_URL, params={
                "where":          f"TMS = '{safe_tms}'",
                "outFields":      "ADDRESS,CITY,STATE,ZIPCODE",
                "returnGeometry": "false",
                "f":              "json",
            }, timeout=10)

            if resp.status_code != 200:
                return None

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
            log.debug("Site address lookup error for TMS %s: %s", tms, e)
            return None


# ══════════════════════════════════════════════════════════════════════════════
# Acclaim CSV Parser
# ══════════════════════════════════════════════════════════════════════════════

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
                clerk_url = (
                    f"{ACCLAIM_BASE}/search/SearchTypeName"
                    f"?directName={owner.replace(' ','%20')}"
                    if owner else DOCTYPE_URL
                )

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
                    "source":      "Register of Deeds",
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
            except Exception as e:
                log.debug("Row error: %s", e)

    except Exception as e:
        log.error("CSV parse error: %s", e)

    log.info("Classified %d Acclaim records", len(records))
    return records


# ══════════════════════════════════════════════════════════════════════════════
# Acclaim Scraper
# ══════════════════════════════════════════════════════════════════════════════

async def run_acclaim(page: Page) -> list:
    start_date, end_date = date_range_str()
    log.info("Acclaim: %s to %s", start_date, end_date)

    # Load + disclaimer
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

    # Search page
    await page.goto(DOCTYPE_URL, wait_until="networkidle", timeout=30000)
    await asyncio.sleep(3)

    # Select All doc types
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

    # Set dates via JS
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

    # SelectAll checkbox
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

    # Submit
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

    # Poll for Export button up to 30 seconds
    log.info("Waiting for Export to CSV button...")
    for attempt in range(15):
        for sel in [
            "input[value='Export to CSV']",
            "input[value*='Export']",
            "button:has-text('Export to CSV')",
            "button:has-text('Export')",
            "a:has-text('Export to CSV')",
            "a:has-text('Export')",
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

def extract_address_from_legal(legal: str) -> str:
    """
    Try to extract a street address from Acclaim legal description / comments.
    Many records include the property address in the comments field.
    Examples:
      "1234 OCEAN BLVD UNIT 5 MYRTLE BEACH SC"
      "LOT 11 BL A - 456 MAIN ST"
    """
    if not legal:
        return ""
    # Look for patterns like: number + street name + street type
    patterns = [
        r'\b(\d+\s+[A-Z][A-Z\s]+(?:ST|AVE|RD|DR|LN|WAY|BLVD|CT|CIR|HWY|LOOP|TRL|PL|PKY|PKWY)[A-Z\s]*\d{5}?)\b',
        r'\b(\d+\s+[A-Z][A-Z\s]{3,30}(?:STREET|AVENUE|ROAD|DRIVE|LANE|WAY|BOULEVARD|COURT|CIRCLE|HIGHWAY))',
    ]
    text = legal.upper()
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
    return ""


async def main():
    log.info("="*60)
    log.info("Horry County Lead Scraper — FINAL")
    log.info("Address source: Horry County GIS REST API (free)")
    log.info("="*60)

    all_records = []

    # ── Step 1: Acclaim ───────────────────────────────────────────────────
    log.info("STEP 1: Acclaim Register of Deeds")
    if PLAYWRIGHT_AVAILABLE:
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
                acclaim_records = await run_acclaim(page)
                all_records.extend(acclaim_records)
                log.info("Acclaim records: %d", len(acclaim_records))
            except Exception as e:
                log.error("Acclaim error: %s", e, exc_info=True)
            finally:
                await browser.close()
    else:
        log.error("Playwright not available")

    # ── Step 2: GIS Address Lookup ────────────────────────────────────────
    log.info("STEP 2: GIS Address Lookup")
    gis = GISLookup()

    # Build list of names to look up:
    # - For HOA/lien records: use GRANTEE (distressed owner)
    # - For all others: use GRANTOR (owner)
    names_to_lookup = set()
    for r in all_records:
        owner    = r.get("owner","").strip()
        grantee  = r.get("grantee","").strip()
        cat_label= r.get("cat_label","")
        cat      = r.get("cat","")

        if owner:
            names_to_lookup.add(owner)
        # Also look up grantee for lien records AND probate AND pre-foreclosure
        if grantee and (
            "HOA" in cat_label.upper() or
            "CONDO" in cat_label.upper() or
            "MECHANIC" in cat_label.upper() or
            cat == "LN" or
            cat == "PRO" or   # Probate — look up the heir
            cat == "NOFC"     # Pre-Foreclosure — look up the homeowner
        ):
            names_to_lookup.add(grantee)

    unique_owners = list(names_to_lookup)
    log.info("Looking up %d unique names via GIS API...", len(unique_owners))

    addr_cache = {}
    enriched   = 0
    for owner in unique_owners:
        addr = gis.lookup(owner)
        addr_cache[owner] = addr
        if addr:
            enriched += 1
        time.sleep(0.1)  # be polite to the API

    # Apply addresses — smart contact selection by record type
    for r in all_records:
        owner     = r.get("owner","").strip()
        grantee   = r.get("grantee","").strip()
        cat       = r.get("cat","")
        cat_label = r.get("cat_label","").upper()

        # Determine who the CONTACT is based on record type
        # Grantee = distressed party for these types:
        use_grantee = (
            "HOA" in cat_label or
            "CONDO" in cat_label or
            "MECHANIC" in cat_label or
            "CHILD SUPPORT" in cat_label or
            cat == "PRO" or   # Probate — heir is the grantee
            cat == "NOFC"     # Pre-Foreclosure — homeowner is the grantee
        )

        if use_grantee and grantee:
            contact_name = grantee
            log.debug("Using GRANTEE for %s: %s", cat_label[:20], grantee[:25])
        else:
            contact_name = owner
            log.debug("Using GRANTOR for %s: %s", cat_label[:20], owner[:25])

        # Look up address for the contact
        addr_data = addr_cache.get(contact_name)
        if not addr_data and contact_name != owner:
            # Fallback to owner if grantee not found
            addr_data = addr_cache.get(owner)

        if addr_data:
            r["prop_address"] = addr_data.get("prop_address","")
            r["prop_city"]    = addr_data.get("prop_city","")
            r["prop_state"]   = addr_data.get("prop_state","SC")
            r["prop_zip"]     = addr_data.get("prop_zip","")
            r["mail_address"] = addr_data.get("mail_address","")
            r["mail_city"]    = addr_data.get("mail_city","")
            r["mail_state"]   = addr_data.get("mail_state","SC")
            r["mail_zip"]     = addr_data.get("mail_zip","")

        # Update owner display to show the contact person
        if use_grantee and grantee:
            r["owner"]   = grantee   # Show heir/homeowner as primary contact
            r["grantee"] = owner     # Move original grantor to grantee field

        # Last resort — extract address from legal description
        if not r.get("prop_address","").strip():
            legal = r.get("legal","")
            addr  = extract_address_from_legal(legal)
            if addr:
                r["prop_address"] = addr

    log.info("Addresses enriched: %d / %d owners", enriched, len(unique_owners))

    # ── Step 3: Deduplicate + Score ───────────────────────────────────────
    seen, unique = set(), []
    for r in all_records:
        key = (r.get("doc_num",""), r.get("cat",""), r.get("owner",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique records: %d", len(unique))

    for r in unique:
        flags     = compute_flags(r)
        r["flags"]= flags
        r["score"]= compute_score(r, flags)
    unique.sort(key=lambda r: r.get("score",0), reverse=True)

    # ── Step 4: Save ──────────────────────────────────────────────────────
    start_date, end_date = date_range_str()
    repo = Path(__file__).parent.parent

    payload = {
        "fetched_at":   datetime.now().isoformat(),
        "source":       "Horry County Register of Deeds + GIS",
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

    # GHL CSV
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City",
        "Mailing State","Mailing Zip","Property Address","Property City",
        "Property State","Property Zip","Lead Type","Document Type",
        "Date Filed","Document Number","Amount/Debt Owed","Seller Score",
        "Motivated Seller Flags","Source","Public Records URL",
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
                "Seller Score":          r.get("score",""),
                "Motivated Seller Flags":"; ".join(r.get("flags",[])),
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
