from typing import List, Dict, Any, Optional
import requests
from typing import Dict, Any, Optional, List
EXPORT_PATH_ENRICHED = "/Users/kevinnovanta/backend_for_ai_agency/data/exports/Google_Leads/Cleaned_Google_Maps_Data/enriched_data.csv"
def scrape_website_info(website_url: str, fallback_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scrape website for emails, phone numbers, offer types, and social/company links.
    fallback_data: dict with at least {name, address, category}
    Returns dict with: Email, First Name, Last Name, Company Name, Phone Number, Address, Custom 1, Custom 2, Custom 3
    """
    import re
    from bs4 import BeautifulSoup
    import requests
    def extract_emails(text: str) -> List[str]:
        # Basic email regex
        return re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)

    def extract_phones(text: str) -> List[str]:
        # US/international phone regex (very basic)
        return re.findall(r"(?:\+?\d{1,3}[-.\s]?)?(?:\(?\d{2,4}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}", text)

    def extract_offer_types(soup: BeautifulSoup) -> str:
        # Look for meta tags or about sections
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return meta["content"][:100]
        about = soup.find(string=re.compile(r"about", re.I))
        if about:
            return about.strip()[:100]
        return ""

    def extract_social_links(soup: BeautifulSoup) -> List[str]:
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if any(kw in href for kw in ["linkedin", "facebook", "instagram", "twitter", "company"]):
                links.append(a["href"])
        return links

    # Defaults
    result = {
        "Email": "",
        "First Name": "",
        "Last Name": "",
        "Company Name": fallback_data.get("name", ""),
        "Phone Number": "",
        "Address": fallback_data.get("address", ""),
        "Custom 1": "",
        "Custom 2": "",
        "Custom 3": ""
    }
    try:
        resp = requests.get(website_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")
        # Dynamically detect if page contains structured business info
        tag_summary = {}
        for tag in soup.find_all():
            tag_name = tag.name
            tag_summary[tag_name] = tag_summary.get(tag_name, 0) + 1
        print(f"📊 HTML Tag Summary: {dict(sorted(tag_summary.items(), key=lambda x: -x[1])[:10])}")

        # Prioritize extracting from structured sections
        text_blocks = []
        for section in soup.find_all(['footer', 'header', 'section', 'div']):
            section_text = section.get_text(separator=" ", strip=True)
            if len(section_text) > 20:
                text_blocks.append(section_text)
        combined_text = " ".join(text_blocks)
        emails = extract_emails(combined_text)
        phones = extract_phones(combined_text)
        offer_type = extract_offer_types(soup)
        socials = extract_social_links(soup)

        if emails:
            result["Email"] = emails[0]
        if phones:
            result["Phone Number"] = phones[0]
        if offer_type:
            result["Custom 1"] = offer_type
        if socials:
            result["Custom 2"] = socials[0]
            if len(socials) > 1:
                result["Custom 3"] = socials[1]
    except Exception as e:
        pass
    return result
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import re
from bs4 import BeautifulSoup
import os
import concurrent.futures

# Fixed export path for all scraping runs
EXPORT_PATH = "/Users/kevinnovanta/backend_for_ai_agency/data/exports/Google_Leads/Raw_Google_Maps_Data/raw_data.csv"

async def scrape_google_maps(search_term, max_results):
    print(f"🔍 Starting scrape for: '{search_term}'")
    export_path = EXPORT_PATH
    columns = ["Business Name", "Category", "Address", "Star Rating", "Website"]
    async with async_playwright() as p:
        # 🎯 Headless anti-bot evasion: user agent spoof, navigator patching, and headless detection mitigation
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()
        await page.add_init_script("""
        Object.defineProperty(navigator, 'plugins', {
          get: () => [1, 2, 3, 4, 5]
        });
        Object.defineProperty(navigator, 'languages', {
          get: () => ['en-US', 'en']
        });
        Object.defineProperty(navigator, 'platform', {
          get: () => 'Win32'
        });
        window.chrome = {
          runtime: {},
          loadTimes: () => {},
          csi: () => {},
        };
        """)
        await page.goto("https://www.google.com/maps", timeout=60000)
        print("🌐 Loaded Google Maps")

        await page.wait_for_selector("input#searchboxinput")
        await page.fill("input#searchboxinput", search_term)
        await page.click("button#searchbox-searchbutton")
        try:
            await page.wait_for_selector('div[role="feed"]', timeout=30000)
        except Exception as e:
            print(f"[!] Timeout while waiting for search results container for: {search_term}")
            await page.screenshot(path=f"/Users/kevinnovanta/backend_for_ai_agency/Debugging/{search_term.replace(' ', '_')}_timeout.png")
            # Optional retry attempt
            try:
                await page.reload()
                await page.wait_for_selector('div[role="feed"]', timeout=15000)
            except Exception as e:
                print(f"[!] Retry failed for: {search_term}")
                await page.screenshot(path=f"/Users/kevinnovanta/backend_for_ai_agency/Debugging/{search_term.replace(' ', '_')}_retry_failed.png")
                return []
        print(f"🔎 Searching for: {search_term}")
        # Dynamically detect most common listing-like container class
        common_classes = await page.evaluate("""
        () => {
          const classMap = {};
          document.querySelectorAll('div').forEach(div => {
            const cls = div.className;
            if (cls) classMap[cls] = (classMap[cls] || 0) + 1;
          });
          return Object.entries(classMap)
            .filter(([cls, count]) => count > 5 && cls.includes("Nv2PK"))
            .sort((a, b) => b[1] - a[1]);
        }
        """)
        most_common_class = common_classes[0][0] if common_classes else "Nv2PK"
        print(f"🔍 Most common listing class detected: {most_common_class}")
        await page.wait_for_timeout(5000)

        # Add deduplication and continuous scraping logic here
        seen_businesses = set()
        data = []

        previous_height = 0
        stable_scrolls = 0
        scroll_count = 0

        while True:
            await page.click('div[role="feed"]')  # Try to focus scrollable container
            listings = await page.query_selector_all(f'div.{most_common_class.split(" ")[0]}')
            print(f"🔄 Found {len(listings)} listings on current scroll")

            for listing in listings:
                try:
                    html = await listing.inner_html()
                    soup = BeautifulSoup(html, 'html.parser')

                    text_blocks = soup.stripped_strings
                    blocks = list(text_blocks)

                    name = next((t for t in blocks if re.match(r"^[A-Z][a-z]+.*", t)), None)
                    if name in seen_businesses:
                        continue

                    category = next((t for t in blocks if any(kw in t.lower() for kw in ["agency", "consulting", "marketing", "services", "school", "firm"])), "N/A")
                    address = next((t for t in blocks if re.search(r"\d{1,5} .* (Ave|St|Blvd|Rd|Dr|Way|Ct|Ln)", t)), "N/A")
                    rating = next((t for t in blocks if re.match(r"^\d\.\d$", t)), "N/A")

                    website_link = "N/A"
                    for a_tag in soup.find_all("a", href=True):
                        if "http" in a_tag["href"] and "website" in a_tag.text.lower():
                            website_link = a_tag["href"]
                            break

                    seen_businesses.add(name)
                    data.append({
                        "Business Name": name or "N/A",
                        "Category": category,
                        "Address": address,
                        "Star Rating": rating,
                        "Website": website_link
                    })
                    print(f"✅ Collected: {name} | {category} | {address} | {rating} | {website_link}")
                    if len(data) >= max_results:
                        break
                except Exception as e:
                    print(f"❌ Failed to parse listing: {e}")

            if len(data) >= max_results:
                break

            # Dynamically determine scroll method
            scroll_method_js = """
            () => {
              const feed = document.querySelector('div[role="feed"]');
              if (feed && typeof feed.scrollBy === 'function') {
                feed.scrollBy(0, 1000);
                return 'scrollBy';
              } else if (feed && typeof feed.scrollTop !== 'undefined') {
                feed.scrollTop += 1000;
                return 'scrollTop';
              } else {
                window.scrollBy(0, 1000);
                return 'windowScroll';
              }
            }
            """

            scroll_result = await page.evaluate(scroll_method_js)
            print(f"🛠️ Scroll method used: {scroll_result}")

            if scroll_result == 'windowScroll':
                await page.keyboard.press('PageDown')
                print("🧪 Fallback: Triggered PageDown key")

            await page.wait_for_timeout(3000)

            current_height = await page.evaluate("""
                () => {
                    const scrollContainer = document.querySelector('div[role="feed"]');
                    return scrollContainer ? scrollContainer.scrollHeight : 0;
                }
            """)
            if current_height == previous_height:
                stable_scrolls += 1
            else:
                stable_scrolls = 0
            if stable_scrolls >= 3:
                break
            previous_height = current_height

        if data:
            pd.DataFrame(data).to_csv(export_path, mode='a', header=False, index=False)

        await browser.close()
        print(f"🏁 Finished scraping '{search_term}': Collected {len(data)} entries\n")
        return data

# Removed export_to_csv function as per instructions

# Create CSV with headers once
pd.DataFrame(columns=["Business Name", "Category", "Address", "Star Rating", "Website"]).to_csv(EXPORT_PATH, index=False)
# Create enriched CSV with headers once per run
pd.DataFrame(columns=["Email", "First Name", "Last Name", "Company Name", "Phone Number", "Address", "Custom 1", "Custom 2", "Custom 3"]).to_csv(EXPORT_PATH_ENRICHED, index=False)

def run_google_maps_scraper(search_terms: List[str], total_results: Optional[int]) -> List[Dict[str, Any]]:
    locations = [
        "San Francisco Bay Area, CA", "New York City, NY", "Los Angeles / Orange County, CA",
        "Seattle, WA", "Austin / Dallas–Ft Worth, TX", "Boston–Cambridge, MA", "Chicago, IL",
        "Washington DC metro (MD/VA)", "Atlanta, GA", "Denver / Boulder, CO",
        "Raleigh–Durham NC", "San Diego, CA", "Phoenix, AZ",
        "Minneapolis–St. Paul, MN", "Miami / South Florida"
    ]

    if total_results:
        per_term = max(1, total_results // (len(search_terms) * len(locations)))
    else:
        per_term = 999999  # Arbitrary large number to ensure full scrape
    import concurrent.futures

    all_results = []
    batch_size = 5

    def scrape_term(term):
        print(f"\n--- Processing search term: '{term}' ---")
        result = asyncio.run(scrape_google_maps(term, per_term))
        print(f"🔢 Scraped {len(result)} businesses for '{term}'")
        return result

    combined_queries = []
    for term in search_terms:
        for location in locations:
            full_query = f"{term} in {location}"
            combined_queries.append(full_query)

    print(f"📈 Total combined search queries to process: {len(combined_queries)}")

    for i in range(0, len(combined_queries), batch_size):
        current_batch = combined_queries[i:i + batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(scrape_term, query) for query in current_batch]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    all_results.extend(result)

    # Count total websites to scrape for enrichment
    total_websites_to_scrape = sum(1 for entry in all_results if entry.get("Website", "") and entry.get("Website", "") != "N/A")
    print(f"📦 Total raw entries scraped: {len(all_results)}")
    print(f"🌐 Total websites to parse for data enrichment: {total_websites_to_scrape}")

    def process_enrichment(entry):
        website = entry.get("Website", "")
        if website and website != "N/A":
            if website in seen_websites:
                return None
            fallback_data = {
                "name": entry.get("Business Name", ""),
                "address": entry.get("Address", ""),
                "category": entry.get("Category", "")
            }
            enriched = scrape_website_info(website, fallback_data)
            if enriched["Email"]:
                return enriched
        return None

    seen_websites = set()
    enriched_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_enrichment, entry) for entry in all_results]
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            result = future.result()
            if result:
                website = result.get("Custom 1", "")
                if website not in seen_websites:
                    seen_websites.add(website)
                    enriched_results.append(result)
                    print(f"✅ [{len(enriched_results)}/{total_websites_to_scrape}] Enriched: {result}")
                    pd.DataFrame([result]).to_csv(EXPORT_PATH_ENRICHED, mode='a', header=False, index=False)
                else:
                    print(f"⚠️ Duplicate skipped for: {website}")
            else:
                print(f"❌ Skipped entry {i} – No email or duplicate.")
    return enriched_results

if __name__ == "__main__":
    search_input = input("Enter your search terms (comma-separated): ")
    search_terms = [term.strip() for term in search_input.split(",") if term.strip()]
    use_limit_input = input("Do you want to limit the total number of websites to scrape? (yes/no): ").strip().lower()
    if use_limit_input == "no":
        total_to_scrape = None
    else:
        total_to_scrape = int(input("Enter total number of websites to scrape: "))
    run_google_maps_scraper(search_terms, total_to_scrape)