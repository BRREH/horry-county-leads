"""
Horry County SC — Complete Lead Scraper — UPDATED v13
========================================================
CHANGES in v5 (address accuracy — fixes the "Wyndham cluster" bug where many
different people all got one lienholder's address):
  1. Removed the lienholder fallback. When the actual contact can't be found
     in the parcel data, the address is left BLANK instead of borrowing the
     grantor/lienholder's parcel.
  2. Name lookups must resolve to EXACTLY ONE parcel. Zero matches (people who
     don't own property, e.g. timeshare holders) or multiple matches (common
     names) return nothing instead of guessing a wrong/neighbor parcel.
  3. A corporate/lienholder name is never used as an address source.
  4. Output now stamped "(v5)" so the running version is verifiable.

CHANGES in v4:
  1. Lead-quality filters added (applied right before save):
     - Business/corporate owners EXCLUDED (LLC, INC, HOA, PROPERTIES, etc.)
     - Records with NO property address EXCLUDED
  2. Probate coverage broadened (DEED OF DISTRIBUTION, PERSONAL REPRESENTATIVE)
  3. Diagnostic logging: logs every doc-type description AcclaimWeb returns plus
     the unclassified ones, so it is clear whether LP / JUD / PRO are present

CHANGES in v3:
  1. Code Violations removed entirely (EnerGov is staff-only / unscrapable)
     - Removed CODE VIOLATION keyword, flag, and scoring bump
  2. Dashboard/output sort order changed:
     - Records now ordered NEWEST FILED FIRST, then HIGHEST SCORE within a day
  3. Reliability guard added:
     - If a scrape returns 0 records, the run FAILS (exit 1) and does NOT
       overwrite records.json — preserving the last good data on the dashboard

CHANGES in v2 (retained):
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
from urllib.parse import quote

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
    ("DEED OF DISTRIBUTION",   "INH",     "Inherited"),
    ("PERSONAL REPRESENTATIVE","PRO",     "Probate Document"),
    ("NOTICE OF COMMENCEMENT", "NOC",     "Notice of Commencement"),
]

# Business / corporate owner patterns. Records whose contact owner matches any
# of these are FILTERED OUT of the final lead list (we want individual
# homeowners, not entities). ESTATE and TRUST are intentionally NOT included:
# "estate of ..." are probate leads, and trusts are often individual sellers.
BUSINESS_RE = re.compile(
    r"\b("
    r"LLC|L L C|LLP|PLLC|INC|INCORPORATED|CORP|CORPORATION|LTD|"
    r"COMPANY|ASSOCIATION|ASSOC|HOA|PROPERTIES|ENTERPRISE|ENTERPRISES|"
    r"HOLDINGS|INVESTMENT|INVESTMENTS|GROUP|PARTNERS|PARTNERSHIP|"
    r"BANK|CREDIT UNION|MANAGEMENT|REALTY|DEVELOPMENT|BUILDERS|"
    r"CONSTRUCTION|FUND|CAPITAL|VENTURES|MORTGAGE|FINANCIAL|RENTALS|SERVICES"
    r")\b"
)


def is_business_entity(name: str) -> bool:
    if not name:
        return False
    return bool(BUSINESS_RE.search(name.upper()))


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
    if not raw:
        return ""
    s = str(raw).strip()
    # Drop any time component first (e.g. "6/3/2026 1:23 PM" -> "6/3/2026"),
    # then parse the date. AcclaimWeb's RecordDate uses mixed time formats
    # (some with seconds, some without); the old version fell through on the
    # no-seconds case and produced junk like "6/3/2026 1". Splitting off the
    # time guarantees a clean YYYY-MM-DD for the sort and the date flags.
    date_part = s.split(" ", 1)[0].split("T", 1)[0]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_part, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return date_part


# Documents that RESOLVE a distress event rather than create one. A
# "CONDO LIEN SATISFACTION" means the lien was PAID OFF — the owner is not a
# distressed seller. Without this filter ~70% of "HOA/Condo Lien" leads were
# actually satisfactions (309 of 439 in a typical 14-day window), plus
# "LIS PENDENS DEED RELEASE", "FEDERAL TAX SATISFACTION", etc.
RESOLVED_RE = re.compile(
    r"\b(SATISFACTION|RELEASE|RESCISSION|TERMINATION|WITHDRAWAL|CANCEL|"
    r"CANCELLATION|DISMISSAL|EXPUNGE|DISCHARGE)\b"
)


def classify_doc(description: str) -> Optional[tuple]:
    desc = description.strip().upper()
    if RESOLVED_RE.search(desc):
        return None  # lien/LP was satisfied or released — not a distress lead
    for keyword, cat, label in DOC_TYPE_KEYWORDS:
        if keyword in desc:
            return (cat, label)
    return None


def split_owner_name(owner: str):
    """Horry County records store owner names LAST FIRST (e.g. 'STRONG PATRICK A').
    Return (first_name, last_name), dropping a trailing ET AL / ETAL."""
    s = re.sub(r"\s*ET\s*AL\.?\s*$", "", (owner or "").strip(), flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ("", "")
    parts = s.split(" ")
    if len(parts) == 1:
        return ("", parts[0])
    return (" ".join(parts[1:]), parts[0])   # first = remainder, last = first token


def _norm_addr(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip().upper())
    for a, b in ((" STREET", " ST"), (" AVENUE", " AVE"), (" ROAD", " RD"),
                 (" DRIVE", " DR"), (" LANE", " LN"), (" BOULEVARD", " BLVD"),
                 (" COURT", " CT"), (" CIRCLE", " CIR"), (" HIGHWAY", " HWY")):
        s = s.replace(a, b)
    return s


def compute_flags(record: dict) -> list:
    flags     = []
    cat       = record.get("cat", "")
    cat_label = record.get("cat_label", "").upper()
    owner     = record.get("owner", "")
    filed     = record.get("filed", "")
    amount    = record.get("amount") or 0

    if cat == "LP":                                      flags.append("Lis pendens")
    if cat == "NOFC":                                    flags.append("Pre-foreclosure")
    if cat == "JUD":                                     flags.append("Judgment lien")
    if "TAX" in cat_label and cat != "TAX":              flags.append("Tax lien")
    if "MECHANIC" in cat_label:                          flags.append("Mechanic lien")
    if cat == "PRO":                                     flags.append("Probate / estate")
    if cat == "INH":                                     flags.append("Inherited / estate")
    if cat == "TAX":                                     flags.append("Tax delinquent")
    if "HOA" in cat_label or "CONDO" in cat_label:       flags.append("HOA lien")

    # Tax-debt size tiers (amount on a tax lead is the total owed)
    if cat == "TAX" and amount:
        if   amount >= 50_000: flags.append("Tax debt >$50k")
        elif amount >= 25_000: flags.append("Tax debt >$25k")
        elif amount >= 10_000: flags.append("Tax debt >$10k")

    # Owner-motivation signals
    mail_state = (record.get("mail_state", "") or "").strip().upper()
    prop_state = (record.get("prop_state", "SC") or "SC").strip().upper()
    if mail_state and mail_state not in ("SC", prop_state):
        flags.append("Out-of-state owner")
    ma = _norm_addr(record.get("mail_address", ""))
    pa = _norm_addr(record.get("prop_address", ""))
    if ma and pa and ma != pa:
        flags.append("Absentee owner")

    if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|HOLDINGS)\b", owner.upper()):
        flags.append("LLC / corp owner")
    try:
        if (datetime.now() - datetime.strptime(filed, "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:
        pass
    return list(dict.fromkeys(flags))


def compute_score(record: dict, flags: list) -> int:
    f = set(flags)
    score = 20

    # Per-signal distress weights
    WEIGHTS = {
        "Pre-foreclosure": 18, "Lis pendens": 18, "Judgment lien": 14,
        "Tax delinquent": 12, "Tax lien": 12, "Mechanic lien": 12,
        "Probate / estate": 12, "Inherited / estate": 12, "HOA lien": 10,
    }
    score += sum(w for k, w in WEIGHTS.items() if k in f)

    # Stacked distress — multiple INDEPENDENT signals on one property = hottest.
    cats = set()
    if f & {"Pre-foreclosure", "Lis pendens"}:        cats.add("foreclosure")
    if f & {"Tax delinquent", "Tax lien"}:            cats.add("tax")
    if "Judgment lien" in f:                          cats.add("judgment")
    if "Mechanic lien" in f:                          cats.add("mechanic")
    if "HOA lien" in f:                               cats.add("hoa")
    if f & {"Probate / estate", "Inherited / estate"}: cats.add("estate")
    if   len(cats) >= 3: score += 35
    elif len(cats) == 2: score += 18
    if "Lis pendens" in f and "Pre-foreclosure" in f: score += 15

    # Owner-motivation modifiers
    if "Out-of-state owner" in f: score += 10
    if "Absentee owner"     in f: score += 8

    # Debt size
    if   "Tax debt >$50k" in f: score += 18
    elif "Tax debt >$25k" in f: score += 12
    elif "Tax debt >$10k" in f: score += 8
    amount = record.get("amount") or 0
    if   amount > 100_000: score += 10
    elif amount > 50_000:  score += 6

    if "New this week" in f: score += 4
    if record.get("prop_address", "").strip(): score += 3
    return max(0, min(score, 100))


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
            "User-Agent": "Mozilla/5.0 (compatible; HorryLeadScraper/3.0)"
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

    # ── Public: batch-resolve many TMS numbers to address (50 per query) ────
    def lookup_many_by_tms(self, tms_list) -> dict:
        """Resolve a list of TMS numbers to mailing + property address in
        batches via 'TMS IN (...)'. Returns {tms: {mail_*, prop_*, tms}}."""
        out = {}
        uniq = list(dict.fromkeys(
            t.strip() for t in tms_list if t and str(t).strip()
        ))
        for i in range(0, len(uniq), 50):
            batch  = uniq[i:i+50]
            inlist = ",".join("'" + t.replace("'", "''") + "'" for t in batch)
            # Layer 24 — current owner name + mailing address
            try:
                r = self.session.get(PARCELS_URL, params={
                    "where": f"TMS IN ({inlist})",
                    "outFields": "OwnerName,OwnerStreet,OwnerCity,OwnerState,OwnerZip,TMS",
                    "returnGeometry": "false", "f": "json",
                }, timeout=20)
                for f in r.json().get("features", []):
                    a = f["attributes"]; t = str(a.get("TMS","") or "").strip()
                    if t:
                        out.setdefault(t, {}).update({
                            "owner_name":   (a.get("OwnerName","") or "").strip(),
                            "mail_address": (a.get("OwnerStreet","") or "").strip(),
                            "mail_city":    (a.get("OwnerCity","") or "").strip(),
                            "mail_state":   (a.get("OwnerState","") or "SC").strip(),
                            "mail_zip":     (a.get("OwnerZip","") or "").strip(),
                            "tms": t,
                        })
            except Exception as e:
                log.debug("batch parcel lookup error: %s", e)
            # Layer 22 — property/situs address
            try:
                r = self.session.get(ADDRESS_URL, params={
                    "where": f"TMS IN ({inlist})",
                    "outFields": "ADDRESS,CITY,STATE,ZIPCODE,TMS",
                    "returnGeometry": "false", "f": "json",
                }, timeout=20)
                for f in r.json().get("features", []):
                    a = f["attributes"]; t = str(a.get("TMS","") or "").strip()
                    if t:
                        out.setdefault(t, {}).update({
                            "prop_address": (a.get("ADDRESS","") or "").strip(),
                            "prop_city":    (a.get("CITY","") or "").strip(),
                            "prop_state":   (a.get("STATE","SC") or "SC").strip(),
                            "prop_zip":     str(a.get("ZIPCODE","") or "").strip(),
                        })
            except Exception as e:
                log.debug("batch address lookup error: %s", e)
            time.sleep(0.1)
        return out

    def _query_parcels_by_name(self, owner_name: str) -> Optional[dict]:
        safe_name = owner_name.replace("'", "''")
        where = f"OwnerName LIKE '%{safe_name}%'"
        try:
            resp = self.session.get(PARCELS_URL, params={
                "where": where, "outFields": "OwnerName,OwnerStreet,OwnerCity,OwnerState,OwnerZip,TMS",
                "returnGeometry": "false", "f": "json",
            }, timeout=10)
            features = resp.json().get("features", [])
            # Only trust a name lookup that resolves to EXACTLY one parcel.
            # 0 = not a property owner (e.g. timeshare/lien-only individuals);
            # >1 = ambiguous (common name) and we must not guess a neighbor.
            if len(features) != 1:
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


def fetch_delinquent_tax(gis: "GISLookup") -> list:
    """
    Pull the full Horry County delinquent-tax parcel list as its own lead
    category. This list is a static annual snapshot (county updates it ~once a
    year), so records are NOT date-windowed and carry no recording date — they
    are dated blank so they don't crowd out fresh Register-of-Deeds leads in the
    newest-first view. Addresses are resolved in batch from each parcel's TMS.
    """
    sess = gis.session
    raw, offset, page = [], 0, 1000
    while True:
        feats = None
        for attempt in range(3):
            try:
                r = sess.get(DELQ_TAX_URL, params={
                    "where": "1=1",
                    "outFields": "owner_name,new_owner_name,total_tax_due,tms,description,item_number",
                    "returnGeometry": "false", "f": "json",
                    "resultOffset": offset, "resultRecordCount": page,
                    "orderByFields": "objectid",
                }, timeout=30)
                feats = r.json().get("features", [])
                break
            except Exception as e:
                log.warning("Delinquent tax fetch error at offset %d (attempt %d/3): %s",
                            offset, attempt+1, e)
                time.sleep(2 * (attempt + 1))
        if feats is None:
            log.error("Delinquent tax: giving up at offset %d after 3 attempts", offset)
            break
        if not feats:
            break
        raw.extend(feats)
        if len(feats) < page:
            break
        offset += page
        time.sleep(0.1)
    log.info("Delinquent tax: pulled %d parcels", len(raw))

    # Batch-resolve addresses by TMS
    addr_map = gis.lookup_many_by_tms(
        [str(f["attributes"].get("tms","") or "") for f in raw]
    )

    records = []
    for f in raw:
        a     = f["attributes"]
        tms   = str(a.get("tms","") or "").strip()
        addr  = addr_map.get(tms, {})
        tax_list_owner = (a.get("owner_name") or a.get("new_owner_name") or "").strip()
        # Prefer the CURRENT owner from the parcel record (same source as the
        # address) so name + mailing + property all describe today's owner.
        # The delinquent-tax list's name can be stale when a property changed
        # hands after going delinquent. Fall back to the tax-list name only if
        # the parcel record has no owner.
        owner = (addr.get("owner_name") or "").strip() or tax_list_owner
        records.append({
            "doc_num":        (a.get("item_number","") or "").strip(),
            "doc_type":       "TAX",
            "cat":            "TAX",
            "cat_label":      "Delinquent Tax",
            "filed":          "",   # static annual list — no recording date
            "owner":          owner,
            "grantee":        "",
            "amount":         parse_amount(a.get("total_tax_due","")),
            "legal":          (a.get("description","") or "").strip(),
            "tms_legal":      tms,
            "clerk_url":      (
                f"{ACCLAIM_BASE}/search/SearchTypeName?directName={quote(owner)}"
                if owner else DOCTYPE_URL
            ),
            "source":         "Delinquent Tax (Horry County GIS)",
            "prop_address":   addr.get("prop_address",""),
            "prop_city":      addr.get("prop_city",""),
            "prop_state":     addr.get("prop_state","SC"),
            "prop_zip":       addr.get("prop_zip",""),
            "mail_address":   addr.get("mail_address",""),
            "mail_city":      addr.get("mail_city",""),
            "mail_state":     addr.get("mail_state","SC"),
            "mail_zip":       addr.get("mail_zip",""),
            "tms":            tms,
            "delinquent_tax": (a.get("total_tax_due","") or ""),
        })
    log.info("Delinquent tax: built %d leads", len(records))
    return records


def load_previous_rod_records() -> list:
    """Return the Register-of-Deeds records (everything that is NOT the
    delinquent-tax category) from the existing records.json. Used when the
    AcclaimWeb scrape fails or returns nothing: a transient site outage or a
    slow CSV export must not silently wipe every lien / inherited / probate
    lead off the dashboard (this happened on 2026-06-12 — the daily run
    published 856 tax-only records and all deed leads vanished)."""
    repo = Path(__file__).parent.parent
    for path in (repo/"dashboard"/"records.json", repo/"data"/"records.json"):
        try:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8"))
                rod = [r for r in data.get("records", []) if r.get("cat") != "TAX"]
                if rod:
                    return rod
        except Exception as e:
            log.debug("Could not read previous ROD records from %s: %s", path, e)
    return []


def load_previous_tax_records() -> list:
    """Return the TAX-category records from the existing records.json, so a
    transient tax-service outage doesn't drop the whole category. The tax list
    is a static annual snapshot, so reusing the last good pull is safe."""
    repo = Path(__file__).parent.parent
    for path in (repo/"dashboard"/"records.json", repo/"data"/"records.json"):
        try:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8"))
                tax = [r for r in data.get("records", []) if r.get("cat") == "TAX"]
                if tax:
                    return tax
        except Exception as e:
            log.debug("Could not read previous tax records from %s: %s", path, e)
    return []


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
    from collections import Counter
    all_types    = Counter()   # every DocTypeDescription seen in the export
    unclassified = Counter()   # descriptions that matched no keyword (dropped)
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
                all_types[description] += 1
                classified  = classify_doc(description) or classify_doc(comments)
                if not classified:
                    unclassified[description] += 1
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
                    "tms_legal":      tms_from_legal,   # TMS from legal text
                    "addr_legal":     addr_from_legal,  # address from legal text
                    "clerk_url":      clerk_url,
                    "source":         "Register of Deeds",
                    "prop_address":   addr_from_legal,  # Pre-fill from legal
                    "prop_city":      "", "prop_state": "SC", "prop_zip": "",
                    "mail_address":   "", "mail_city": "", "mail_state": "SC", "mail_zip": "",
                    "delinquent_tax": "",  # cross-ref with delinquent tax list
                })
            except Exception as e:
                log.debug("Row error: %s", e)

    except Exception as e:
        log.error("CSV parse error: %s", e)

    # Diagnostic: show exactly what AcclaimWeb returned so we can confirm whether
    # LP / JUD / PRO documents are present (possibly under unexpected names).
    log.info("ALL doc-types seen (top 40): %s", dict(all_types.most_common(40)))
    if unclassified:
        log.info("UNCLASSIFIED doc-types dropped (top 40): %s",
                 dict(unclassified.most_common(40)))
    by_cat = Counter(r.get("cat","?") for r in records)
    log.info("Classified by category: %s", dict(by_cat))
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

    # networkidle is flaky on this Kendo-UI page (long-lived connections can
    # keep it from ever firing, which throws and aborts the whole deed scrape).
    # Wait for DOM + the search form's group dropdown instead.
    await page.goto(DOCTYPE_URL, wait_until="domcontentloaded", timeout=45000)
    try:
        await page.wait_for_selector("select", timeout=20000)
    except Exception:
        log.warning("Doc-type search form did not render a <select> in 20s")
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

    # The SelectAll checkbox (#Checkbox1) exists but is NOT visible, so
    # Playwright's .check() spins for its full timeout (2 selectors x 20s =
    # 40s wasted every run) and never succeeds. Set it via JS instead and call
    # the page's own ToggleChk handler so every doc-type checkbox is ticked.
    res = await page.evaluate("""
        () => {
            const cb = document.querySelector(
                '#Checkbox1,[name="SelectAllDocTypesToggle"]');
            if (!cb) return 'no SelectAll checkbox';
            cb.checked = true;
            try { if (typeof ToggleChk === 'function')
                      ToggleChk(cb, 'DocTypeInfoCheckBox'); } catch (e) {}
            // belt and suspenders: tick every doc-type checkbox directly
            const boxes = document.querySelectorAll(
                'input[name="DocTypeInfoCheckBox"]');
            boxes.forEach(b => { b.checked = true; });
            return `SelectAll set; ${boxes.length} doc-type boxes ticked`;
        }
    """)
    log.info("Doc-type selection: %s", res)
    await asyncio.sleep(1)

    searched = False
    for sel in ["#btnSearch", "input[value='Search']", "input[type='submit']"]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click()
                # networkidle can hang on Kendo grids; wait for DOM then poll
                # for the export control in the loop below instead.
                await page.wait_for_load_state("domcontentloaded", timeout=30000)
                log.info("Search submitted via %s", sel)
                searched = True
                break
        except Exception as e:
            log.debug("Search click failed %s: %s", sel, e)
    if not searched:
        log.warning("No Search button found on doc-type page")

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
                    # The CSV is ~840 KB and server-side generation can take a
                    # while on a cold morning; 30s was tight enough to cause
                    # whole-run failures. Allow 90s for the download.
                    async with page.expect_download(timeout=90000) as dl_info:
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
    log.info("Horry County Lead Scraper — v3 (no code violations; newest+score sort)")
    log.info("="*60)

    all_records = []

    # ── Step 1: Acclaim ───────────────────────────────────────────────────
    log.info("STEP 1: Acclaim Register of Deeds")
    if PLAYWRIGHT_AVAILABLE:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True, args=["--no-sandbox","--disable-dev-shm-usage"]
            )
            # Retry the whole AcclaimWeb sequence up to 3 times with a fresh
            # page each attempt — a single transient timeout (slow morning,
            # site hiccup) must not zero out the deed half of the pipeline.
            acclaim_records = []
            for attempt in range(1, 4):
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
                    if acclaim_records:
                        log.info("Acclaim attempt %d: %d records",
                                 attempt, len(acclaim_records))
                        break
                    log.warning("Acclaim attempt %d returned 0 records", attempt)
                except Exception as e:
                    log.error("Acclaim attempt %d error: %s", attempt, e,
                              exc_info=True)
                finally:
                    await context.close()
                if attempt < 3:
                    await asyncio.sleep(20 * attempt)
            await browser.close()
            all_records.extend(acclaim_records)
            log.info("Acclaim records: %d", len(acclaim_records))
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
            cat in ("LN","PRO","NOFC","LP","INH")
        ):
            names_to_lookup.add(grantee)

    log.info("Looking up %d unique names...", len(names_to_lookup))
    addr_cache = {}
    for name in names_to_lookup:
        addr = gis.lookup_by_name(name)
        addr_cache[name] = addr
        time.sleep(0.1)

    # Apply addresses with TMS fallback for LP records
    enriched_by_name = enriched_by_tms = 0
    for r in all_records:
        owner   = r.get("owner","").strip()
        grantee = r.get("grantee","").strip()
        cat     = r.get("cat","")
        cat_lbl = r.get("cat_label","").upper()

        use_grantee = (
            "HOA" in cat_lbl or "CONDO" in cat_lbl or "MECHANIC" in cat_lbl or
            "CHILD SUPPORT" in cat_lbl or cat in ("PRO","NOFC","LP","INH")
        )
        contact = grantee if (use_grantee and grantee) else owner

        # Look up ONLY the actual contact (the distressed party). Do NOT fall
        # back to the grantor/lienholder — that is what stamped Wyndham's
        # corporate parcel onto 30 different timeshare owners. If the contact
        # isn't a property owner, leave the address blank (the no-address
        # filter will then drop the record).
        addr_data = addr_cache.get(contact)

        # Hard guard: never enrich from a corporate / lienholder name. Its
        # parcel is not the individual's address (this is what produced the
        # Wyndham cluster — 30 people stamped with Wyndham's Orlando office).
        if is_business_entity(contact):
            addr_data = None

        # ── TMS fallback: pull the property address straight from the parcel
        #    number in the legal description. Used for Lis Pendens AND for
        #    Probate / Deed-of-Distribution records, where the heir often
        #    isn't yet the parcel's GIS owner so a name lookup misses, but the
        #    deed's legal text carries the TMS. ──────────────────────────────
        if not addr_data and cat in ("LP", "PRO", "INH"):
            tms = r.get("tms_legal","")
            if tms:
                log.debug("%s: trying TMS lookup %s for %s", cat, tms, owner[:25])
                addr_data = gis.lookup_by_tms(tms)
                if addr_data:
                    enriched_by_tms += 1
                    log.info("%s TMS hit: %s -> %s", cat, tms,
                             addr_data.get("prop_address","")[:30])

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
                if "Also delinquent taxes" not in r["flags"]:
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

    # ── Step 3b: Lead-quality filters ─────────────────────────────────────
    # Drop (a) business/corporate owners and (b) records with no property
    # address — neither is usable as a direct-mail seller lead.
    before = len(unique)
    kept, dropped_corp, dropped_noaddr = [], 0, 0
    for r in unique:
        if is_business_entity(r.get("owner","")):
            dropped_corp += 1
            continue
        if not r.get("prop_address","").strip():
            dropped_noaddr += 1
            continue
        kept.append(r)
    unique = kept
    log.info("Quality filter: %d -> %d kept (dropped %d corporate, %d no-address)",
             before, len(unique), dropped_corp, dropped_noaddr)

    # ── Step 3b-2: Deed carry-forward guard ────────────────────────────────
    # Mirrors the delinquent-tax guard below. If the AcclaimWeb scrape failed
    # (site outage, slow export, blocked runner IP) we end up here with zero
    # Register-of-Deeds leads. Publishing that would silently wipe every
    # lien / inherited / probate lead off the dashboard (this happened on
    # 2026-06-12). Previous-run records are already enriched and filtered, so
    # they slot straight in; the dashboard keeps the last good deed data and
    # the log shouts about it.
    if not unique:
        prev_rod = load_previous_rod_records()
        if prev_rod:
            log.warning("No Register-of-Deeds leads this run — carrying "
                        "forward %d deed leads from the previous run", len(prev_rod))
            unique = prev_rod
        else:
            log.error("No Register-of-Deeds leads and no previous deed leads "
                      "to carry forward — dashboard will be tax-only")

    # ── Step 3c: Delinquent Tax leads (full annual list, NOT filtered) ──────
    log.info("STEP 3c: Delinquent Tax parcels")
    try:
        tax_records = fetch_delinquent_tax(gis)
    except Exception as e:
        log.error("Delinquent tax error: %s", e, exc_info=True)
        tax_records = []

    # Guard against a transient tax-service failure silently wiping the whole
    # category. The list is a static annual snapshot, so if we got nothing but
    # the service still reports records, reuse last run's tax leads.
    if not tax_records:
        try:
            cnt = int(gis.session.get(DELQ_TAX_URL, params={
                "where": "1=1", "returnCountOnly": "true", "f": "json",
            }, timeout=15).json().get("count", 0))
        except Exception:
            cnt = 0
        if cnt > 0:
            prev = load_previous_tax_records()
            if prev:
                log.warning("Tax pull empty but service has %d records — "
                            "carrying forward %d tax leads from last run", cnt, len(prev))
                tax_records = prev
            else:
                log.error("Tax pull empty (service has %d) and no previous tax "
                          "leads to carry forward", cnt)

    for r in tax_records:
        flags      = compute_flags(r)
        r["flags"] = list(dict.fromkeys(flags))
        r["score"] = compute_score(r, r["flags"])
    # Tax leads must clear the SAME two gates as Register-of-Deeds leads:
    #   1. real property address (no vacant land / un-addressed parcels)
    #   2. not a business entity (LLC/INC/HOA/etc.) — keep individuals/estates
    tax_total = len(tax_records)
    tax_records = [
        r for r in tax_records
        if r.get("prop_address","").strip()
        and not is_business_entity(r.get("owner",""))
    ]
    dropped = tax_total - len(tax_records)
    log.info("Delinquent tax: kept %d of %d (dropped %d for no-address or corporate)",
             len(tax_records), tax_total, dropped)
    unique = unique + tax_records
    log.info("Combined total (ROD %d + tax %d) = %d",
             len(unique) - len(tax_records), len(tax_records), len(unique))

    # ── Final dedup: one lead per OWNER + PARCEL ──────────────────────────
    # The county often records several documents against the same parcel
    # (e.g. two Deeds of Distribution back-to-back), and a parcel can appear
    # more than once on the tax list. Keying on document number lets those
    # through as duplicate rows. Key on owner + TMS instead (owner + property
    # address when no TMS), so the same person on the same property is ONE
    # lead no matter how many instruments exist. Flags/score are merged so no
    # signal is lost when two records collapse.
    def _dedup_key(r):
        owner = (r.get("owner","") or "").strip().upper()
        tms   = str(r.get("tms","") or "").strip()
        if owner and tms:
            return ("T", owner, tms)
        prop = (r.get("prop_address","") or "").strip().upper()
        if owner and prop:
            return ("A", owner, prop)
        return ("D", owner, r.get("cat",""), r.get("doc_num",""))

    merged, order = {}, []
    for r in unique:
        k = _dedup_key(r)
        if k not in merged:
            merged[k] = r
            order.append(k)
        else:
            keep = merged[k]
            keep["flags"] = list(dict.fromkeys(
                (keep.get("flags",[]) or []) + (r.get("flags",[]) or [])
            ))
            keep["score"] = max(keep.get("score",0), r.get("score",0))
            if not keep.get("amount") and r.get("amount"):
                keep["amount"] = r["amount"]
            if not (keep.get("prop_address","") or "").strip() \
               and (r.get("prop_address","") or "").strip():
                for f in ("prop_address","prop_city","prop_state","prop_zip"):
                    keep[f] = r.get(f, keep.get(f,""))
    removed = len(unique) - len(merged)
    unique  = [merged[k] for k in order]
    log.info("Final dedup: removed %d duplicate lead(s) -> %d unique", removed, len(unique))

    # Re-score from the MERGED flag set. Taking max(score_a, score_b) loses
    # the stacked-distress bonus: an owner with an HOA lien (score ~55) who is
    # ALSO on the delinquent tax list (score ~53) is two independent distress
    # signals (+18) and should clear 70 — the hottest leads on the board. The
    # merge above combines the flags; this recompute lets the bonus fire.
    for r in unique:
        r["score"] = compute_score(r, r.get("flags", []))

    # Sort: HOTTEST FIRST — highest score on top, newest filed as the tiebreaker.
    unique.sort(key=lambda r: (r.get("score",0), r.get("filed","")), reverse=True)

    # ── Reliability guard: never overwrite good data with an empty scrape ──
    if not unique:
        log.error("0 records left after scrape + quality filters.")
        log.error("Either the AcclaimWeb export failed, or nothing in this "
                  "window had a non-corporate owner AND a property address.")
        log.error("Preserving existing records.json and failing the run.")
        sys.exit(1)

    # ── Step 4: Save ──────────────────────────────────────────────────────
    start_date, end_date = date_range_str()
    repo = Path(__file__).parent.parent

    # Pre-split owner into First/Last (county order is LAST FIRST) so the
    # dashboard export and the server CSV use the same, correct columns.
    for r in unique:
        fn, ln = split_owner_name(r.get("owner",""))
        r["first_name"] = fn
        r["last_name"]  = ln

    payload = {
        "fetched_at":   datetime.now().isoformat(),
        "source":       "Horry County Register of Deeds + GIS (v13)",
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
        "Property State","Property Zip","Phone 1","Phone 2","Email 1","Email 2",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","TMS Parcel","Delinquent Tax","Seller Score",
        "Motivated Seller Flags","Source","Public Records URL",
    ]
    csv_path = repo/"data"/"leads_export.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in unique:
            owner = r.get("owner", "")
            first_name, last_name = split_owner_name(owner)
            writer.writerow({
                "First Name":            first_name,
                "Last Name":             last_name,
                "Mailing Address":       r.get("mail_address",""),
                "Mailing City":          r.get("mail_city",""),
                "Mailing State":         r.get("mail_state","SC"),
                "Mailing Zip":           r.get("mail_zip",""),
                "Property Address":      r.get("prop_address",""),
                "Property City":         r.get("prop_city",""),
                "Property State":        r.get("prop_state","SC"),
                "Property Zip":          r.get("prop_zip",""),
                "Phone 1":               r.get("phone1",""),
                "Phone 2":               r.get("phone2",""),
                "Email 1":               r.get("email1",""),
                "Email 2":               r.get("email2",""),
                "Lead Type":             r.get("cat_label",""),
                "Document Type":         r.get("cat",""),
                "Date Filed":            r.get("filed",""),
                "Document Number":       r.get("doc_num",""),
                "Amount/Debt Owed":      r.get("amount",""),
                "TMS Parcel":            r.get("tms",""),
                "Delinquent Tax":        r.get("delinquent_tax",""),
                "Seller Score":          r.get("score",""),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":                "Horry County Register of Deeds",
                "Public Records URL":    (
                    f"https://acclaimweb.horrycounty.org/AcclaimWeb/"
                    f"search/SearchTypeName?directName={quote(owner)}"
                    if owner else ""
                ),
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
