"""
Horry County SC — Complete Motivated Seller Lead Scraper
Sources:
  1. Acclaim Register of Deeds — liens, lis pendens, foreclosures, probate etc.
  2. Horry County QPay Tax DB — property + mailing address lookup by owner name
  3. Horry County EnerGov — code violation cases with property addresses
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

ACCLAIM_BASE   = "https://acclaimweb.horrycounty.org/AcclaimWeb"
DOCTYPE_URL    = f"{ACCLAIM_BASE}/search/SearchTypeDocType"
ENERGOV_BASE   = "https://egweb.horrycounty.org/EnerGov_prod/selfservice"
QPAY_URL       = "https://horrycountytreasurer.qpaybill.com/Taxes/TaxesDefaultType4.aspx"
LOOK_BACK_DAYS = 7

# ── Doc type classification ──────────────────────────────────────────────────
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
                "%m/%d/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
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
    flags    = []
    cat      = record.get("cat", "")
    cat_label= record.get("cat_label", "")
    owner    = record.get("owner", "")
    filed    = record.get("filed", "")

    if cat == "LP":                              flags.append("Lis pendens")
    if cat == "NOFC":                            flags.append("Pre-foreclosure")
    if cat == "JUD":                             flags.append("Judgment lien")
    if "TAX" in cat_label.upper():               flags.append("Tax lien")
    if "MECHANIC" in cat_label.upper():          flags.append("Mechanic lien")
    if cat == "PRO":                             flags.append("Probate / estate")
    if "HOA" in cat_label.upper() or "CONDO" in cat_label.upper():
        flags.append("HOA lien")
    if cat == "CV":                              flags.append("Code violation")
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
    if "New this week" in flags:  score += 5
    if "Code violation" in flags: score += 15  # bonus for violations
    if record.get("prop_address","").strip(): score += 5
    return min(score, 100)


# ══════════════════════════════════════════════════════════════════════════════
# ADDRESS LOOKUP — Horry County QPay Tax Database
# ══════════════════════════════════════════════════════════════════════════════

class AddressLookup:
    """
    Queries Horry County Treasurer QPay by owner name.
    Returns property address + mailing address.
    Public — no login required.
    """

    BASE = "https://horrycountytreasurer.qpaybill.com"
    SEARCH = f"{BASE}/Taxes/TaxesDefaultType4.aspx"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        })
        self._cache = {}

    def lookup(self, owner_name: str) -> Optional[dict]:
        """Look up property address by owner name. Returns first real estate match."""
        if not owner_name or not owner_name.strip():
            return None

        key = owner_name.strip().upper()
        if key in self._cache:
            return self._cache[key]

        try:
            result = self._search(owner_name)
            self._cache[key] = result
            return result
        except Exception as e:
            log.debug("QPay lookup failed for %s: %s", owner_name, e)
            return None

    def _search(self, owner_name: str) -> Optional[dict]:
        # Get the search page first to get any hidden fields/viewstate
        try:
            resp = self.session.get(self.SEARCH, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")
        except Exception:
            return None

        # Try the JSON/AJAX endpoint that QPay uses
        try:
            # QPay uses a REST-style API
            api_url = f"{self.BASE}/Taxes/GetTaxData"
            payload = {
                "searchType": "REAL ESTATE",
                "paymentStatus": "ALL",
                "taxYear": "ALL",
                "searchBy": "OWNER",
                "searchValue": owner_name.strip(),
            }
            resp = self.session.post(api_url, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return self._parse_json(data)
        except Exception:
            pass

        # Fallback: try form POST
        try:
            form_data = {
                "searchType": "4",   # Real Estate
                "paymentStatus": "ALL",
                "taxYear": "0",
                "searchBy": "OWNER",
                "txtSearch": owner_name.strip(),
            }
            # Get any hidden form fields
            for inp in soup.find_all("input", {"type": "hidden"}):
                if inp.get("name"):
                    form_data[inp["name"]] = inp.get("value", "")

            resp = self.session.post(self.SEARCH, data=form_data, timeout=15)
            soup2 = BeautifulSoup(resp.text, "lxml")
            return self._parse_html(soup2)
        except Exception as e:
            log.debug("QPay form POST failed: %s", e)
            return None

    def _parse_json(self, data) -> Optional[dict]:
        """Parse JSON response from QPay API."""
        try:
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                for key in ["data","items","results","records"]:
                    if key in data and isinstance(data[key], list):
                        items = data[key]
                        break

            for item in items:
                if not isinstance(item, dict):
                    continue
                # Look for real estate records
                prop_type = str(item.get("propertyType","") or item.get("type","")).upper()
                if "REAL" in prop_type or "RE" in prop_type or not prop_type:
                    def g(*keys):
                        for k in keys:
                            v = item.get(k) or item.get(k.lower()) or item.get(k.upper())
                            if v and str(v).strip():
                                return str(v).strip()
                        return ""

                    addr = g("siteAddress","address","propertyAddress","siteAddr","location")
                    if addr:
                        return {
                            "prop_address": addr,
                            "prop_city":    g("siteCity","city","propertyCity"),
                            "prop_state":   "SC",
                            "prop_zip":     g("siteZip","zip","propertyZip"),
                            "mail_address": g("mailingAddress","mailAddress","ownerAddress"),
                            "mail_city":    g("mailingCity","mailCity","ownerCity"),
                            "mail_state":   g("mailingState","mailState","ownerState") or "SC",
                            "mail_zip":     g("mailingZip","mailZip","ownerZip"),
                        }
        except Exception as e:
            log.debug("QPay JSON parse error: %s", e)
        return None

    def _parse_html(self, soup: BeautifulSoup) -> Optional[dict]:
        """Parse HTML table results from QPay."""
        try:
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                if len(rows) < 2:
                    continue
                headers = [th.get_text(strip=True).lower()
                           for th in rows[0].find_all(["th","td"])]

                for row in rows[1:]:
                    cells = row.find_all("td")
                    if len(cells) < 3:
                        continue

                    def cell(i):
                        return cells[i].get_text(strip=True) if i < len(cells) else ""

                    def find(*names):
                        for name in names:
                            for i, h in enumerate(headers):
                                if name in h:
                                    return cell(i)
                        return ""

                    # Look for address in any column
                    addr = find("address","location","property","site")
                    if not addr:
                        # Try positional — address is usually 3rd or 4th column
                        for i in range(2, min(6, len(cells))):
                            c = cell(i)
                            if re.search(r'\d+\s+\w+', c) and len(c) > 5:
                                addr = c
                                break

                    if addr:
                        return {
                            "prop_address": addr,
                            "prop_city":    find("city") or "Horry County",
                            "prop_state":   "SC",
                            "prop_zip":     find("zip","postal") or "",
                            "mail_address": find("mail","mailing") or addr,
                            "mail_city":    find("mail city") or "Horry County",
                            "mail_state":   "SC",
                            "mail_zip":     find("mail zip") or "",
                        }
        except Exception as e:
            log.debug("QPay HTML parse error: %s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# CODE VIOLATIONS — Horry County EnerGov
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_code_violations(page: Page) -> list:
    """
    Scrape open code violation cases from Horry County EnerGov portal.
    Returns list of records with property addresses.
    """
    records = []
    start_date, end_date = date_range_str()

    try:
        log.info("Loading EnerGov portal...")
        # Use the base selfservice URL — hash routes don't work with goto
        await page.goto(
            "https://egweb.horrycounty.org/EnerGov_prod/selfservice",
            wait_until="domcontentloaded", timeout=20000
        )
        await asyncio.sleep(3)

        # Log what's on the page
        title = await page.title()
        log.info("EnerGov page title: %s | URL: %s", title, page.url)

        # Try to find search options
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        # Look for any search forms or case listings
        links = await page.evaluate("""
            () => Array.from(document.querySelectorAll('a, button'))
                .map(el => ({text: el.innerText.trim(), href: el.href || ''}))
                .filter(el => el.text.length > 0 && el.text.length < 60)
        """)
        log.info("EnerGov links found:")
        for link in links[:20]:
            log.info("  '%s' -> %s", link['text'], link['href'][:60])

        # Try different EnerGov search URLs
        for search_path in [
            "#/search/code-enforcement",
            "#/search/cases",
            "#/search",
        ]:
            try:
                await page.goto(f"{ENERGOV_BASE}{search_path}",
                               wait_until="networkidle", timeout=15000)
                await asyncio.sleep(2)
                url = page.url
                log.info("EnerGov %s -> %s", search_path, url)

                # Look for case listings
                cases = await page.evaluate("""
                    () => {
                        const rows = document.querySelectorAll(
                            'tr, .case-row, .search-result, [class*="result"]'
                        );
                        return Array.from(rows).slice(0,50).map(r => r.innerText.trim())
                            .filter(t => t.length > 10);
                    }
                """)
                if cases:
                    log.info("Found %d case rows at %s", len(cases), search_path)
                    for case_text in cases[:5]:
                        log.info("  Case: %s", case_text[:100])
            except Exception as e:
                log.debug("EnerGov %s failed: %s", search_path, e)

        # Try the EnerGov API directly
        try:
            api_resp = await page.evaluate("""
                async () => {
                    try {
                        const resp = await fetch('/EnerGov_prod/SelfService/api/energov/search/search', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                "keyword": "",
                                "ExactMatch": false,
                                "SearchModule": 4,
                                "FilterModule": 1,
                                "SearchMainAddress": true,
                                "page": 1,
                                "pageSize": 100
                            })
                        });
                        return await resp.text();
                    } catch(e) {
                        return 'error: ' + e.message;
                    }
                }
            """)
            log.info("EnerGov API response: %s", str(api_resp)[:300])
        except Exception as e:
            log.debug("EnerGov API call failed: %s", e)

    except Exception as e:
        log.warning("Code violation scrape error: %s", e)

    log.info("Code violations collected: %d", len(records))
    return records


# ══════════════════════════════════════════════════════════════════════════════
# ACCLAIM SCRAPER — Register of Deeds
# ══════════════════════════════════════════════════════════════════════════════

def parse_acclaim_csv(raw: str) -> list:
    """Parse Acclaim CSV export with known columns."""
    records = []
    try:
        raw = raw.lstrip('\ufeff')
        reader = csv.DictReader(io.StringIO(raw))
        log.info("CSV columns: %s", reader.fieldnames)
        rows = list(reader)
        log.info("CSV total rows: %d", len(rows))

        for row in rows:
            try:
                description = (row.get("DocTypeDescription","") or "").strip()
                comments    = (row.get("Comments","") or "").strip()

                classified = classify_doc(description)
                if not classified:
                    classified = classify_doc(comments)
                if not classified:
                    continue

                cat, cat_label = classified
                book_page = (row.get("BookPage","") or "").strip()
                filed_raw = (row.get("RecordDate","") or "").strip()
                owner     = (row.get("DirectName","") or "").strip()
                grantee   = (row.get("IndirectName","") or "").strip()
                amount    = parse_amount(row.get("Consideration",""))
                comments_text = comments

                if book_page:
                    parts = book_page.split("/")
                    if len(parts) == 2:
                        clerk_url = (
                            f"https://acclaimweb.horrycounty.org/AcclaimWeb"
                            f"/search/SearchTypeName?directName={requests.utils.quote(owner)}"
                        )
                    else:
                        clerk_url = DOCTYPE_URL
                else:
                    clerk_url = DOCTYPE_URL

                records.append({
                    "doc_num":     book_page,
                    "doc_type":    cat,
                    "filed":       normalize_date(filed_raw),
                    "cat":         cat,
                    "cat_label":   cat_label,
                    "owner":       owner,
                    "grantee":     grantee,
                    "amount":      amount,
                    "legal":       comments_text,
                    "clerk_url":   clerk_url,
                    "source":      "Register of Deeds",
                    "prop_address":"","prop_city":"","prop_state":"SC","prop_zip":"",
                    "mail_address":"","mail_city":"","mail_state":"SC","mail_zip":"",
                })
            except Exception as e:
                log.debug("CSV row error: %s", e)

    except Exception as e:
        log.error("CSV parse error: %s", e)

    log.info("Classified %d Acclaim records", len(records))
    return records


async def run_acclaim_scraper(page: Page) -> list:
    """Run the Acclaim scraper and return CSV records."""
    start_date, end_date = date_range_str()
    log.info("Acclaim date range: %s to %s", start_date, end_date)

    # Load portal and accept disclaimer
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

    # Load search page
    await page.goto(DOCTYPE_URL, wait_until="networkidle", timeout=30000)
    await asyncio.sleep(3)

    # Select All doc types
    result = await page.evaluate("""
        () => {
            const sel = document.querySelector('#DocTypeGroupDropDown, [name="DocTypeGroupDropDown"], select');
            if (sel) {
                for (let opt of sel.options) {
                    if (opt.text.trim() === 'All') {
                        sel.value = opt.value;
                        sel.dispatchEvent(new Event('change', {bubbles: true}));
                        return 'selected All';
                    }
                }
            }
            return 'not found';
        }
    """)
    log.info("Doc type dropdown: %s", result)
    await asyncio.sleep(2)

    # Set dates via JavaScript
    date_result = await page.evaluate(f"""
        () => {{
            const results = {{}};
            const dropdowns = document.querySelectorAll('select');
            for (let dd of dropdowns) {{
                for (let opt of dd.options) {{
                    if (opt.text.includes('Specify') || opt.text.includes('Range')) {{
                        dd.value = opt.value;
                        dd.dispatchEvent(new Event('change', {{bubbles: true}}));
                        results.dropdown = opt.text;
                        break;
                    }}
                }}
            }}
            const from = document.querySelector('#RecordDateFrom, [name="RecordDateFrom"]');
            if (from) {{
                from.value = '{start_date}';
                from.dispatchEvent(new Event('change', {{bubbles: true}}));
                from.dispatchEvent(new Event('input', {{bubbles: true}}));
                results.from = '{start_date}';
            }}
            const to = document.querySelector('#RecordDateTo, [name="RecordDateTo"]');
            if (to) {{
                to.value = '{end_date}';
                to.dispatchEvent(new Event('change', {{bubbles: true}}));
                to.dispatchEvent(new Event('input', {{bubbles: true}}));
                results.to = '{end_date}';
            }}
            return results;
        }}
    """)
    log.info("Date setting: %s", date_result)
    await asyncio.sleep(1)

    # Select All checkboxes
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

    # Submit search
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
    await asyncio.sleep(5)

    # Wait up to 30s for Export to CSV button to appear after results load
    log.info("Waiting for Export to CSV button...")
    for wait_attempt in range(15):
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
                    log.info("✓ Found export button: %s (attempt %d)", sel, wait_attempt+1)
                    async with page.expect_download(timeout=30000) as dl_info:
                        await el.click()
                    download = await dl_info.value
                    path = await download.path()
                    if path:
                        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
                            content = f.read()
                        log.info("✓ CSV downloaded: %d chars", len(content))
                        return parse_acclaim_csv(content)
            except Exception as e:
                log.debug("Export %s: %s", sel, e)
        # Not found yet — wait and retry
        await asyncio.sleep(2)

    log.warning("Could not export CSV from Acclaim after waiting")
    return []


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

async def main_scrape() -> list:
    """Run all scrapers and return combined enriched records."""
    all_records = []

    if not PLAYWRIGHT_AVAILABLE:
        log.error("Playwright not available")
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

        try:
            # ── 1. Acclaim Register of Deeds ──────────────────────────────
            log.info("="*50)
            log.info("STEP 1: Acclaim Register of Deeds")
            log.info("="*50)
            acclaim_records = await run_acclaim_scraper(page)
            log.info("Acclaim records: %d", len(acclaim_records))
            all_records.extend(acclaim_records)

            # ── 2. Code Violations from EnerGov ───────────────────────────
            log.info("="*50)
            log.info("STEP 2: EnerGov Code Violations")
            log.info("="*50)
            cv_records = await scrape_code_violations(page)
            log.info("Code violation records: %d", len(cv_records))
            all_records.extend(cv_records)

        except Exception as e:
            log.error("Scraper error: %s", e, exc_info=True)
        finally:
            await browser.close()

    # ── 3. Address enrichment via QPay ────────────────────────────────────
    log.info("="*50)
    log.info("STEP 3: Address Enrichment via QPay")
    log.info("="*50)
    addr_lookup = AddressLookup()
    enriched = 0

    for rec in all_records:
        owner = rec.get("owner","").strip()
        if not owner:
            continue
        # Only look up if no address yet
        if rec.get("prop_address","").strip():
            continue

        addr_data = addr_lookup.lookup(owner)
        if addr_data:
            rec.update(addr_data)
            enriched += 1
            log.info("Address found for %s: %s", owner[:30], addr_data.get("prop_address",""))
        else:
            log.debug("No address for: %s", owner[:30])

    log.info("Addresses enriched: %d / %d", enriched, len(all_records))

    # ── 4. Deduplicate ────────────────────────────────────────────────────
    seen, unique = set(), []
    for r in all_records:
        key = (r.get("doc_num",""), r.get("cat",""), r.get("owner",""))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info("Unique records after dedup: %d", len(unique))

    # ── 5. Score all records ──────────────────────────────────────────────
    for r in unique:
        flags     = compute_flags(r)
        r["flags"]= flags
        r["score"]= compute_score(r, flags)

    unique.sort(key=lambda r: r.get("score",0), reverse=True)
    return unique


# ══════════════════════════════════════════════════════════════════════════════
# SAVE + EXPORT
# ══════════════════════════════════════════════════════════════════════════════

def save_records_json(records: list, *paths: str):
    start_date, end_date = date_range_str()
    payload = {
        "fetched_at":   datetime.now().isoformat(),
        "source":       "Horry County — Register of Deeds + Code Enforcement",
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address","").strip()),
        "records":      records,
    }
    for path in paths:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info("Saved → %s (%d records)", path, len(records))


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
    log.info("GHL CSV → %s (%d rows)", path, len(records))


async def main():
    log.info("="*60)
    log.info("Horry County Complete Lead Scraper")
    log.info("Sources: Acclaim + EnerGov + QPay Address Lookup")
    log.info("="*60)

    records = await main_scrape()

    repo = Path(__file__).parent.parent
    save_records_json(records,
        str(repo/"dashboard"/"records.json"),
        str(repo/"data"/"records.json"),
    )
    export_ghl_csv(records, str(repo/"data"/"leads_export.csv"))

    with_addr = sum(1 for r in records if r.get("prop_address","").strip())
    cv_count  = sum(1 for r in records if r.get("cat") == "CV")
    log.info("="*60)
    log.info("DONE — Total: %d | With Address: %d | Code Violations: %d",
             len(records), with_addr, cv_count)
    log.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
