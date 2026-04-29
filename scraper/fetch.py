"""
Horry County SC — Complete Lead Scraper v12
Sources:
  1. Acclaim Register of Deeds — liens, foreclosures, probate etc.
  2. Horry County QPay — property + mailing address via Playwright
  3. Horry County EnerGov — code violations (logged for structure)
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

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, Page, BrowserContext
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
QPAY_URL     = "https://horrycountytreasurer.qpaybill.com/Taxes/TaxesDefaultType4.aspx"
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

    if cat == "LP":                             flags.append("Lis pendens")
    if cat == "NOFC":                           flags.append("Pre-foreclosure")
    if cat == "JUD":                            flags.append("Judgment lien")
    if "TAX" in cat_label.upper():              flags.append("Tax lien")
    if "MECHANIC" in cat_label.upper():         flags.append("Mechanic lien")
    if cat == "PRO":                            flags.append("Probate / estate")
    if "HOA" in cat_label.upper() or "CONDO" in cat_label.upper():
        flags.append("HOA lien")
    if cat == "CV":                             flags.append("Code violation")
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
    if "Code violation" in flags: score += 15
    if record.get("prop_address", "").strip(): score += 5
    return min(score, 100)


# ══════════════════════════════════════════════════════════════════════════════
# QPay Address Lookup — Playwright based
# ══════════════════════════════════════════════════════════════════════════════

async def lookup_address_qpay(page: Page, owner_name: str) -> Optional[dict]:
    """
    Look up property address on QPay by owner name.
    QPay is JavaScript-rendered so we use Playwright.
    """
    try:
        # Navigate to QPay search
        await page.goto(QPAY_URL, wait_until="networkidle", timeout=20000)
        await asyncio.sleep(2)

        # Set search type to Real Estate
        await page.evaluate("""
            () => {
                // Click Real Estate radio button
                const radios = document.querySelectorAll('input[type=radio]');
                for (let r of radios) {
                    if (r.value && (r.value.includes('Real') || r.value === '4' || r.value === 'RE')) {
                        r.click();
                        break;
                    }
                }
                // Set payment status to All
                const status = document.querySelectorAll('input[type=radio]');
                for (let r of status) {
                    if (r.value === 'All' || r.value === 'ALL') {
                        r.click();
                    }
                }
            }
        """)

        # Set search by Owner Name
        await page.evaluate("""
            () => {
                const selects = document.querySelectorAll('select');
                for (let sel of selects) {
                    for (let opt of sel.options) {
                        if (opt.text.includes('Owner') || opt.value.includes('OWNER') || opt.value.includes('Owner')) {
                            sel.value = opt.value;
                            sel.dispatchEvent(new Event('change', {bubbles: true}));
                            break;
                        }
                    }
                }
            }
        """)

        # Fill search box
        for sel in ["#txtSearch", "input[type='text']", "input[placeholder*='search' i]",
                    "input[placeholder*='name' i]", ".search-input"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click(triple_click=True)
                    await el.fill(owner_name)
                    log.debug("QPay: filled search with %s", owner_name[:30])
                    break
            except Exception:
                pass

        # Click search button
        for sel in ["#btnSearch", "input[value*='Search' i]",
                    "button:has-text('Search')", "input[type='submit']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    await asyncio.sleep(2)
                    break
            except Exception:
                pass

        # Read results
        content = await page.content()
        soup    = BeautifulSoup(content, "lxml")

        # Log structure for debugging
        page_text = await page.evaluate("() => document.body.innerText")
        log.debug("QPay result preview for %s: %s", owner_name[:20], page_text[:200])

        # Try to find address in results table
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            headers = [th.get_text(strip=True).lower()
                       for th in rows[0].find_all(["th","td"])]
            log.debug("QPay table headers: %s", headers)

            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                def cell(i):
                    return cells[i].get_text(strip=True) if i < len(cells) else ""

                def find(*names):
                    for name in names:
                        for i, h in enumerate(headers):
                            if name in h:
                                return cell(i)
                    return ""

                # Look for address column
                addr = (find("address","location","property","site","street") or
                        find("situs"))

                # If no header match, scan cells for address pattern
                if not addr:
                    for i, c in enumerate(cells):
                        text = c.get_text(strip=True)
                        if re.search(r'\d+\s+[A-Za-z]', text) and len(text) > 8:
                            addr = text
                            break

                if addr and addr.strip():
                    log.info("QPay address found for %s: %s",
                             owner_name[:25], addr[:40])
                    return {
                        "prop_address": addr.strip(),
                        "prop_city":    find("city") or "",
                        "prop_state":   "SC",
                        "prop_zip":     find("zip","postal") or "",
                        "mail_address": find("mail","mailing") or addr.strip(),
                        "mail_city":    find("mail city") or "",
                        "mail_state":   "SC",
                        "mail_zip":     find("mail zip") or "",
                    }

        # Also try reading from page text directly
        lines = [l.strip() for l in page_text.split('\n') if l.strip()]
        for i, line in enumerate(lines):
            if re.search(r'\d+\s+[A-Za-z].*(?:ST|AVE|RD|DR|LN|WAY|BLVD|CT|CIR|HWY)',
                         line.upper()):
                log.info("QPay address from text for %s: %s",
                         owner_name[:25], line[:40])
                return {
                    "prop_address": line,
                    "prop_city":    lines[i+1] if i+1 < len(lines) else "",
                    "prop_state":   "SC",
                    "prop_zip":     "",
                    "mail_address": line,
                    "mail_city":    "",
                    "mail_state":   "SC",
                    "mail_zip":     "",
                }

    except Exception as e:
        log.debug("QPay lookup error for %s: %s", owner_name[:25], e)

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
            return 'not found';
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

    # Wait for Export button — poll up to 30 seconds
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
                    log.info("✓ Export button found (attempt %d): %s", attempt+1, sel)
                    async with page.expect_download(timeout=30000) as dl_info:
                        await el.click()
                    download = await dl_info.value
                    path = await download.path()
                    if path:
                        with open(path,"r",encoding="utf-8-sig",errors="ignore") as f:
                            content = f.read()
                        log.info("✓ CSV downloaded: %d chars", len(content))
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
    log.info("Horry County Lead Scraper v12")
    log.info("="*60)

    if not PLAYWRIGHT_AVAILABLE:
        log.error("Playwright not available")
        return

    all_records = []

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
            # ── Step 1: Acclaim ───────────────────────────────────────────
            log.info("STEP 1: Acclaim Register of Deeds")
            acclaim_records = await run_acclaim(page)
            log.info("Acclaim records: %d", len(acclaim_records))
            all_records.extend(acclaim_records)

            # ── Step 2: QPay address lookup ───────────────────────────────
            log.info("STEP 2: QPay Address Lookup")
            # Get unique owners that need addresses
            needs_addr = [r for r in all_records if not r.get("prop_address","").strip()]
            unique_owners = list(dict.fromkeys(
                r["owner"] for r in needs_addr if r.get("owner","").strip()
            ))
            log.info("Looking up addresses for %d unique owners...", len(unique_owners))

            addr_cache = {}
            enriched   = 0

            for owner in unique_owners:
                if owner in addr_cache:
                    continue
                log.info("QPay lookup: %s", owner[:40])
                addr = await lookup_address_qpay(page, owner)
                addr_cache[owner] = addr
                if addr:
                    enriched += 1
                    log.info("  → Found: %s", addr.get("prop_address","")[:40])
                else:
                    log.info("  → No address found")
                await asyncio.sleep(0.5)

            # Apply addresses to records
            for r in all_records:
                owner = r.get("owner","")
                if owner in addr_cache and addr_cache[owner]:
                    r.update(addr_cache[owner])

            log.info("Addresses found: %d / %d owners", enriched, len(unique_owners))

        except Exception as e:
            log.error("Main error: %s", e, exc_info=True)
        finally:
            await browser.close()

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

    for path in [repo/"dashboard"/"records.json", repo/"data"/"records.json"]:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "fetched_at":   datetime.now().isoformat(),
                "source":       "Horry County Register of Deeds",
                "date_range":   {"start": start_date, "end": end_date},
                "total":        len(unique),
                "with_address": sum(1 for r in unique if r.get("prop_address","").strip()),
                "records":      unique,
            }, f, indent=2, default=str)
        log.info("Saved → %s", path)

    # GHL CSV
    csv_path = repo/"data"/"leads_export.csv"
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City",
        "Mailing State","Mailing Zip","Property Address","Property City",
        "Property State","Property Zip","Lead Type","Document Type",
        "Date Filed","Document Number","Amount/Debt Owed","Seller Score",
        "Motivated Seller Flags","Source","Public Records URL",
    ]
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
