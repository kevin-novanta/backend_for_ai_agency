
# Imports required for the script
import pandas as pd
import re
import asyncio
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any
from playwright.async_api import async_playwright

# Constants for export paths
EXPORT_PATH = "/Users/kevinnovanta/backend_for_ai_agency/data/exports/Google_Leads/Raw_Google_Maps_Data/raw_data.csv"
EXPORT_PATH_ENRICHED = "/Users/kevinnovanta/backend_for_ai_agency/data/exports/Google_Leads/Cleaned_Google_Maps_Data/enriched_data.csv"
import pandas as pd

async def scrape_google_maps(search_term, max_results):
    print(f"[INFO] Starting scrape for: '{search_term}'")
    export_path = EXPORT_PATH
    columns = ["Business Name", "Category", "Address", "Star Rating", "Website"]
    async with async_playwright() as p:
        print("[INFO] Launching browser...")
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
        print("[INFO] Navigating to Google Maps...")
        await page.goto("https://www.google.com/maps", timeout=60000)
        print("[INFO] 🌐 Loaded Google Maps")

        await page.wait_for_selector("input#searchboxinput")
        await page.fill("input#searchboxinput", search_term)
        await page.click("button#searchbox-searchbutton")
        try:
            await page.wait_for_selector('div[role="feed"]', timeout=30000)
        except Exception as e:
            print(f"[ERROR] Timeout while waiting for search results container for: {search_term} - {e}")
            await page.screenshot(path=f"/Users/kevinnovanta/backend_for_ai_agency/Debugging/{search_term.replace(' ', '_')}_timeout.png")
            # Optional retry attempt
            try:
                print("[INFO] Attempting retry after timeout...")
                await page.reload()
                await page.wait_for_selector('div[role="feed"]', timeout=15000)
            except Exception as e:
                print(f"[ERROR] Retry failed for: {search_term} - {e}")
                await page.screenshot(path=f"/Users/kevinnovanta/backend_for_ai_agency/Debugging/{search_term.replace(' ', '_')}_retry_failed.png")
                return []
        print(f"[INFO] 🔎 Searching for: {search_term}")
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
        print(f"[INFO] 🔍 Most common listing class detected: {most_common_class}")
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
            print(f"[INFO] 🔄 Found {len(listings)} listings on current scroll")

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
                    entry = {
                        "Business Name": name or "N/A",
                        "Category": category,
                        "Address": address,
                        "Star Rating": rating,
                        "Website": website_link
                    }
                    data.append(entry)
                    print(f"[INFO] ✅ Collected: {name} | {category} | {address} | {rating} | {website_link}")

                    # Write each entry immediately to CSV
                    print(f"[INFO] Writing data for {name} to image_data.csv")
                    pd.DataFrame([entry]).to_csv(export_path, mode='a', header=False, index=False)
                    print(f"[INFO] Successfully wrote data for {name}")

                    if len(data) >= max_results:
                        break
                except Exception as e:
                    print(f"[ERROR] Failed to parse listing: {e}")

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
            print(f"[INFO] 🛠️ Scroll method used: {scroll_result}")

            if scroll_result == 'windowScroll':
                await page.keyboard.press('PageDown')
                print("[INFO] 🧪 Fallback: Triggered PageDown key")

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
                print("[INFO] No more new listings detected after multiple scrolls, stopping.")
                break
            previous_height = current_height

        await browser.close()
        print(f"[INFO] 🏁 Finished scraping '{search_term}': Collected {len(data)} entries\n")
        return data

# Removed export_to_csv function as per instructions

print("[INFO] Creating CSV files with headers if not present...")
# Create CSV with headers once
pd.DataFrame(columns=["Business Name", "Category", "Address", "Star Rating", "Website"]).to_csv(EXPORT_PATH, index=False)
# Create enriched CSV with headers once per run
pd.DataFrame(columns=["Email", "First Name", "Last Name", "Company Name", "Phone Number", "Address", "Custom 1", "Custom 2", "Custom 3"]).to_csv(EXPORT_PATH_ENRICHED, index=False)
print("[INFO] CSV files ready.")

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
        print(f"\n[INFO] --- Processing search term: '{term}' ---")
        result = asyncio.run(scrape_google_maps(term, per_term))
        print(f"[INFO] 🔢 Scraped {len(result)} businesses for '{term}'")
        return result

    combined_queries = []
    for term in search_terms:
        for location in locations:
            full_query = f"{term} in {location}"
            combined_queries.append(full_query)

    print(f"[INFO] 📈 Total combined search queries to process: {len(combined_queries)}")

    for i in range(0, len(combined_queries), batch_size):
        current_batch = combined_queries[i:i + batch_size]
        print(f"[INFO] Processing batch {i // batch_size + 1} with {len(current_batch)} queries...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(scrape_term, query) for query in current_batch]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    all_results.extend(result)

    # Count total websites to scrape for enrichment
    total_websites_to_scrape = sum(1 for entry in all_results if entry.get("Website", "") and entry.get("Website", "") != "N/A")
    print(f"[INFO] 📦 Total raw entries scraped: {len(all_results)}")
    print(f"[INFO] 🌐 Total websites to parse for data enrichment: {total_websites_to_scrape}")

    # Website scraping/enrichment removed in this script.
    return all_results

if __name__ == "__main__":
    search_input = input("Enter your search terms (comma-separated): ")
    search_terms = [term.strip() for term in search_input.split(",") if term.strip()]
    use_limit_input = input("Do you want to limit the total number of websites to scrape? (yes/no): ").strip().lower()
    if use_limit_input == "no":
        total_to_scrape = None
    else:
        total_to_scrape = int(input("Enter total number of websites to scrape: "))
    run_google_maps_scraper(search_terms, total_to_scrape)