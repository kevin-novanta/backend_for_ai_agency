

import asyncio
from playwright.async_api import async_playwright
import pandas as pd

async def scrape_google_maps(search_term, max_results):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto("https://www.google.com/maps")

        await page.wait_for_selector("input#searchboxinput")
        await page.fill("input#searchboxinput", search_term)
        await page.click("button#searchbox-searchbutton")

        await page.wait_for_timeout(5000)

        data = []
        seen = set()
        retries = 0
        while len(data) < max_results and retries < 5:
            listings = await page.query_selector_all('div[role="article"]')
            for listing in listings:
                if len(data) >= max_results:
                    break
                name = await listing.query_selector_eval('h3', 'el => el.textContent') if await listing.query_selector('h3') else "N/A"
                website_link = None
                buttons = await listing.query_selector_all('a')
                for btn in buttons:
                    href = await btn.get_attribute('href')
                    if href and "website" in href:
                        website_link = href
                        break
                name = name.strip() if name else "N/A"
                if (name, website_link) not in seen:
                    seen.add((name, website_link))
                    data.append({
                        "Business Name": name,
                        "Website": website_link or "N/A"
                    })
            await page.mouse.wheel(0, 3000)
            await page.wait_for_timeout(2000)
            retries += 1

        await browser.close()
        return data

def export_to_csv(all_data):
    df = pd.DataFrame(all_data)
    df.to_csv("google_maps_scrape_results.csv", index=False)
    print(f"\n✅ Exported {len(all_data)} results to google_maps_scrape_results.csv\n")

if __name__ == "__main__":
    search_input = input("Enter your search terms (comma-separated): ")
    search_terms = [term.strip() for term in search_input.split(",") if term.strip()]

    total_to_scrape = int(input("Enter total number of websites to scrape: "))
    per_term = max(1, total_to_scrape // len(search_terms))

    all_results = []
    for term in search_terms:
        print(f"\n🔍 Scraping '{term}' for up to {per_term} results...\n")
        result = asyncio.run(scrape_google_maps(term, per_term))
        all_results.extend(result)

    export_to_csv(all_results)