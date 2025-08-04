import pandas as pd
import asyncio
import csv
from typing import List, Dict, Any
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from playwright.async_api import async_playwright

def extract_emails(text: str) -> List[str]:
    return list(set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)))

def extract_phones(text: str) -> List[str]:
    return list(set(re.findall(r"(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}", text)))

def extract_meta(soup: BeautifulSoup):
    meta = soup.find("meta", attrs={"name": "description"})
    desc = meta["content"][:200] if meta and meta.get("content") else ""
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    kw = meta_kw["content"][:200] if meta_kw and meta_kw.get("content") else ""
    return desc, kw

def extract_offer_text(soup: BeautifulSoup):
    offer_texts = []

    # 1. Check meta tags for description-like content
    meta_props = [
        {"name": "description"},
        {"name": "keywords"},
        {"property": "og:description"},
        {"name": "twitter:description"},
    ]
    for props in meta_props:
        meta = soup.find("meta", attrs=props)
        if meta and meta.get("content") and len(meta["content"]) > 30:
            offer_texts.append(meta["content"].strip())

    # 2. Extract from class-based tags with useful keywords
    semantic_keywords = ["desc", "about", "service", "industry", "summary", "company", "bio"]
    for tag in soup.find_all(["div", "span", "p"], class_=True):
        cls = tag.get("class")
        if cls and any(any(k in c.lower() for k in semantic_keywords) for c in cls):
            txt = tag.get_text(strip=True)
            if len(txt) > 30:
                offer_texts.append(txt)

    # 3. Extract from visible tags with keywords
    keywords = [
        "service", "offer", "about", "solution", "team", "who we are", "what we do",
        "our mission", "faq", "contact", "company", "vision", "product", "features", "benefits"
    ]
    tags_to_check = ['h1', 'h2', 'h3', 'p', 'li', 'span']
    for tag in soup.find_all(tags_to_check):
        txt = tag.get_text(separator=" ", strip=True)
        if any(kw in txt.lower() for kw in keywords) and len(txt) > 30:
            offer_texts.append(txt)

    # 4. Fallback: Large content blocks
    for blk in soup.find_all(['section', 'div', 'main']):
        txt = blk.get_text(separator=" ", strip=True)
        if any(kw in txt.lower() for kw in keywords) and len(txt) > 50:
            offer_texts.append(txt[:300])

    # Deduplicate and return
    return list(set([t for t in offer_texts if len(t) > 30]))

def get_visible_text_blocks(soup: BeautifulSoup):
    blocks = []
    for tag in soup.find_all(['header', 'footer', 'section', 'main', 'div']):
        txt = tag.get_text(separator=" ", strip=True)
        if len(txt) > 20:
            blocks.append(txt)
    return blocks

def extract_names(company_name):
    parts = company_name.strip().split()
    if len(parts) >= 2:
        return parts[0], parts[-1]
    elif len(parts) == 1:
        return parts[0], ""
    return "", ""

def is_us_phone_number(phone: str) -> bool:
    # Basic US phone number validation: 10 digits with optional country code +1
    phone_digits = re.sub(r"\D", "", phone)
    if len(phone_digits) == 10:
        return True
    if len(phone_digits) == 11 and phone_digits.startswith("1"):
        return True
    return False

async def scrape_website_info_async(website_url: str, fallback_data: Dict[str, Any]) -> Dict[str, Any]:
    import time
    result = {
        "Email": "",
        "First Name": "",
        "Last Name": "",
        "Company Name": fallback_data.get("name", ""),
        "Phone Number": "",
        "Address": fallback_data.get("address", ""),
        "Custom 1": website_url,
        "Custom 2": "",
        "Custom 3": ""
    }
    found_emails = set()
    found_phones = set()
    found_offers = set()
    found_meta_descs = set()
    found_meta_keywords = set()
    all_blocks = []
    max_pages = 5
    visited = set()
    info_paths = ["contact", "about", "team", "services", "faq"]
    info_regex = re.compile(r"/(" + "|".join(info_paths) + r")([/?#]|$)", re.I)
    subpages_to_visit = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800}
            )
            page = await context.new_page()
            await page.goto(website_url, wait_until="networkidle", timeout=20000)
            screenshot_path = "/Users/kevinnovanta/backend_for_ai_agency/Debugging/GoogleMaps_Scraper_Debugging/first_page_debug.png"
            await page.screenshot(path=screenshot_path, full_page=True)
            print(f"[+] Screenshot saved: {screenshot_path}")
            print(f"[+] Navigated to {website_url}")
            await asyncio.sleep(1.5)
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            print("[+] Parsed main page HTML")
            blocks = get_visible_text_blocks(soup)
            all_blocks.extend(blocks)
            print(f"[+] Found {len(blocks)} text blocks on main page")
            desc, kw = extract_meta(soup)
            print(f"[+] Extracted meta description and keywords: '{desc[:60]}...', '{kw[:60]}...'")
            if desc:
                found_meta_descs.add(desc)
            if kw:
                found_meta_keywords.add(kw)
            offers = extract_offer_text(soup)
            found_offers.update(offers)
            print(f"[+] Found {len(offers)} offer-related text blocks")
            for block in blocks:
                found_emails.update(extract_emails(block))
                found_phones.update(extract_phones(block))
            print(f"[+] Found {len(found_emails)} emails, {len(found_phones)} phone numbers on main page")
            print(f"    Emails: {list(found_emails)[:3]}")
            print(f"    Phones: {list(found_phones)[:3]}")
            homepage_url = page.url
            parsed_home = urlparse(homepage_url)
            base = f"{parsed_home.scheme}://{parsed_home.netloc}"
            visited.add(homepage_url.rstrip("/"))
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("mailto:") or href.startswith("tel:"):
                    continue
                abs_url = urljoin(base, href)
                parsed = urlparse(abs_url)
                if parsed.netloc != parsed_home.netloc:
                    continue
                if info_regex.search(parsed.path) and abs_url.rstrip("/") not in visited:
                    links.append(abs_url.rstrip("/"))
            links = list(dict.fromkeys(links))[:max_pages-1]
            subpages_to_visit = links
            print(f"[+] Discovered {len(subpages_to_visit)} subpages to visit: {subpages_to_visit}")
            for sub_url in subpages_to_visit:
                try:
                    await page.goto(sub_url, wait_until="networkidle", timeout=15000)
                    await asyncio.sleep(1.5)
                    html = await page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    blocks = get_visible_text_blocks(soup)
                    all_blocks.extend(blocks)
                    desc, kw = extract_meta(soup)
                    if desc:
                        found_meta_descs.add(desc)
                    if kw:
                        found_meta_keywords.add(kw)
                    offers = extract_offer_text(soup)
                    found_offers.update(offers)
                    for block in blocks:
                        found_emails.update(extract_emails(block))
                        found_phones.update(extract_phones(block))
                    visited.add(sub_url)
                    print(f"[+] Visited subpage: {sub_url}")
                    print(f"[+] Found {len(blocks)} text blocks on subpage")
                    print(f"[+] Found {len(found_emails)} total emails, {len(found_phones)} total phone numbers so far")
                    print(f"    Emails so far: {list(found_emails)[:3]}")
                    print(f"    Phones so far: {list(found_phones)[:3]}")
                except Exception:
                    continue
                await asyncio.sleep(1.2)
            await browser.close()
    except Exception:
        pass
    if found_emails:
        result["Email"] = sorted(found_emails)[0]
    # Leave First Name and Last Name blank as per instructions
    result["First Name"] = ""
    result["Last Name"] = ""
    # Address update only if missing or "N/A"
    if not result["Address"] or result["Address"].strip().upper() == "N/A":
        # Try to extract address from found text blocks (not implemented here, so keep fallback)
        pass
    # Phone Number validation for US format only
    valid_phones = [p for p in sorted(found_phones) if is_us_phone_number(p)]
    if valid_phones:
        result["Phone Number"] = valid_phones[0]
    # Custom 1 is from fallback_data already set
    # Custom 2: Offer Type from about or services section, prefer longer/more descriptive
    if found_offers:
        result["Custom 2"] = sorted(found_offers, key=len, reverse=True)[0]
    elif found_meta_descs:
        result["Custom 2"] = sorted(found_meta_descs, key=len, reverse=True)[0]
    # Custom 3: alternate offer or meta description
    alt = ""
    if len(found_offers) > 1:
        alt = sorted(found_offers)[1]
    elif len(found_meta_descs) > 1:
        alt = sorted(found_meta_descs)[1]
    if alt:
        result["Custom 3"] = alt

    import usaddress

    try:
        parsed_address = usaddress.tag(result["Address"])[0]
        city = parsed_address.get("PlaceName", "")
        state = parsed_address.get("StateName", "")
        if city and state:
            result["Address"] = f"{city}, {state}"
    except Exception as e:
        print(f"[!] Address parsing failed: {e}")

    print("[+] Finished scraping website")
    return result

# --- Enrichment coroutine for a list of websites ---
async def enrich_website_data(websites: list, fallback_map: Dict[str, Dict[str, Any]]) -> list:
    import random
    enriched = []
    sem = asyncio.Semaphore(8)  # Hard set concurrency to 8
    async def process(url):
        async with sem:
            print(f"🔎 Scraping: {url}")
            fallback = fallback_map.get(url, {"name": url, "address": "", "category": "", "custom_1": ""})
            result = await scrape_website_info_async(url, fallback)
            print(f"✅ Finished: {url}")
            return result
    tasks = [process(url) for url in websites]
    counter = 0
    for coro in asyncio.as_completed(tasks):
        try:
            result = await coro
            if result.get("Email"):
                enriched.append(result)
                # Save only exact columns and structure
                df_partial = pd.DataFrame(enriched)
                df_partial = df_partial[["Email", "First Name", "Last Name", "Company Name", "Phone Number", "Address", "Custom 1", "Custom 2", "Custom 3"]]
                df_partial.to_csv(OUTPUT_PATH, index=False)
                print(f"💾 Partial data saved to {OUTPUT_PATH}")
                counter += 1
                print(f"✅ Scraped {counter}/{len(websites)} websites")
            else:
                print("⛔ Skipped site — no email found.")
        except Exception as e:
            print(f"❌ Error scraping site: {e}")
            continue
    return enriched


RAW_INPUT_PATH = "/Users/kevinnovanta/backend_for_ai_agency/data/exports/Google_Leads/Raw_Google_Maps_Data/raw_data.csv"
OUTPUT_PATH = "/Users/kevinnovanta/backend_for_ai_agency/data/exports/Google_Leads/Cleaned_Google_Maps_Data/enriched_data.csv"

def load_website_links(csv_path):
    # Load URLs from the CSV file (expects a column with website URLs)
    df = pd.read_csv(csv_path)
    # Try 'Website' column, else 'Custom 1'
    if "Website" in df.columns:
        websites = df["Website"].dropna().unique().tolist()
    elif "Custom 1" in df.columns:
        websites = df["Custom 1"].dropna().unique().tolist()
    else:
        raise ValueError("Missing a column with website URLs (expected 'Website' or 'Custom 1').")
    # Remove "N/A" or empty
    websites = [url for url in websites if url and url != "N/A"]
    # Build fallback map from website URL to original row data for reuse
    fallback_map = {}
    for _, row in df.iterrows():
        url = None
        if "Website" in df.columns and pd.notna(row["Website"]) and row["Website"] != "N/A":
            url = row["Website"]
        elif "Custom 1" in df.columns and pd.notna(row["Custom 1"]) and row["Custom 1"] != "N/A":
            url = row["Custom 1"]
        if url:
            fallback_map[url] = {
                "name": row.get("Company Name", "") if "Company Name" in row else "",
                "address": row.get("Address", "") if "Address" in row else "",
                "category": row.get("Category", "") if "Category" in row else "",
                "custom_1": row.get("Custom 1", "") if "Custom 1" in row else ""
            }
    return websites, fallback_map

async def main():
    # Load URLs from raw Google Maps data CSV
    websites, fallback_map = load_website_links(RAW_INPUT_PATH)
    print(f"Loaded {len(websites)} websites to enrich")
    enriched_data = await enrich_website_data(websites, fallback_map)
    df_enriched = pd.DataFrame(enriched_data)
    df_enriched = df_enriched[["Email", "First Name", "Last Name", "Company Name", "Phone Number", "Address", "Custom 1", "Custom 2", "Custom 3"]]
    df_enriched.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Enriched website data saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    asyncio.run(main())