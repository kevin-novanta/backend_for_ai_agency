import asyncio
from playwright.async_api import async_playwright
from pycookiecheat import chrome_cookies
import urllib.parse
import os
import csv
from threading import Lock
import random
import time

MAX_CONCURRENT = 5
US_FILTER = "companyHqGeo=%5B%22103644278%22%5D"
SIZE_FILTER = "companySize=%5B%22D%22%2C%22E%22%2C%22F%22%5D"
BASE_URL = "https://www.linkedin.com/search/results/companies/"
OUTPUT_FILE = "/Users/kevinnovanta/backend_for_ai_agency/workflows/lead_scraper/linkedin/logic/linkedin_phase1_raw_links.csv"
csv_lock = Lock()

def simulate_human_delay(base_delay=2, jitter=2):
    delay = base_delay + random.uniform(0, jitter)
    print(f"⏳ Simulating human-like delay: {delay:.2f} seconds")
    time.sleep(delay)

def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15A372 Safari/604.1"
    ]
    selected_agent = random.choice(user_agents)
    print(f"🕵️ Using user-agent: {selected_agent}")
    return selected_agent

def get_headers():
    headers = {
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1"
    }
    print(f"📎 Injecting fake headers: {headers}")
    return headers

def load_cookies_from_chrome():
    print("🔐 Loading cookies from Chrome...")
    try:
        cookies_raw = chrome_cookies(
            "https://www.linkedin.com",
            cookie_file="~/Library/Application Support/Google/Chrome/Profile 1/Cookies"
        )
        print(f"🔍 Found {len(cookies_raw)} cookies.")
        cookies = []
        for name, value in cookies_raw.items():
            cookies.append({
                "name": name,
                "value": value,
                "domain": ".linkedin.com",
                "path": "/",
                "secure": True,
                "httpOnly": False,
                "sameSite": "Lax",
                "expires": -1
            })
        print("✅ Cookies loaded successfully.")
        return cookies
    except Exception as e:
        print(f"❌ Error loading cookies: {e}")
        return []

def build_search_url(keyword):
    print(f"🔗 Building search URL for keyword: '{keyword}'")
    encoded_keyword = urllib.parse.quote(keyword)
    url = f"{BASE_URL}?{US_FILTER}&{SIZE_FILTER}&keywords={encoded_keyword}&origin=FACETED_SEARCH"
    print(f"🌐 Search URL: {url}")
    return url

async def extract_company_links(page):
    print("🔎 Locating company links in live DOM...")
    links = set()
    try:
        print("🔍 Querying for anchor elements with '/company/' in href...")
        elements = await page.query_selector_all("a[href*='/company/']")
        print(f"🔍 Found {len(elements)} anchor elements containing '/company/' in href.")
        
        for el in elements:
            href = await el.get_attribute("href")
            if href and '/company/' in href:
                if not href.startswith("https://"):
                    href = "https://www.linkedin.com" + href
                links.add(href)

        print(f"🔗 Total unique /company/ links found: {len(links)}")
    except Exception as e:
        print(f"❌ Error extracting company links: {e}")
    return list(links)

async def click_next_if_available(page):
    print("➡️ Attempting to locate 'Next' button...")
    try:
        simulate_human_delay(base_delay=3, jitter=3)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        await page.wait_for_selector("button[aria-label='Next']", timeout=5000)

        all_buttons = await page.query_selector_all("button")
        print(f"🧮 Found {len(all_buttons)} total buttons on page.")
        for b in all_buttons:
            try:
                text = await b.inner_text()
                print(f"🔘 Button: {text}")
            except:
                continue

        # Try multiple strategies
        next_selectors = [
            "button[aria-label='Next']",
            "//button[span[contains(text(), 'Next')]]",
            "//span[text()='Next']/ancestor::button",
            "//button[contains(., 'Next')]"
        ]

        for selector in next_selectors:
            print(f"🔍 Trying selector: {selector}")
            try:
                if selector.startswith("//"):
                    button = await page.query_selector(f"xpath={selector}")
                else:
                    button = await page.query_selector(selector)

                if button:
                    visible = await button.is_visible()
                    print(f"👀 Found button with selector '{selector}', visible: {visible}")
                    if visible:
                        print("➡️ Clicking 'Next' button...")
                        await button.click()
                        simulate_human_delay(base_delay=3, jitter=3)
                        print("✅ Clicked and waited.")
                        return True
            except Exception as sub_e:
                print(f"⚠️ Error trying selector '{selector}': {sub_e}")

        button = await page.query_selector("button[aria-label='Next']")
        if button:
            print("🆗 Found fallback 'Next' button with aria-label.")
            await button.click()
            simulate_human_delay(base_delay=3, jitter=3)
            return True

        print("🚫 'Next' button not found with any known selectors.")
        # Fallback: try incrementing page number in URL manually
        print("🔁 Attempting to paginate via URL as fallback...")
        current_url = page.url
        parsed = urllib.parse.urlparse(current_url)
        query = urllib.parse.parse_qs(parsed.query)

        try:
            current_page = int(query.get("page", ["1"])[0])
            next_page = current_page + 1
            query["page"] = [str(next_page)]
            new_query = urllib.parse.urlencode(query, doseq=True)
            new_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
            print(f"🌐 Navigating to next page via URL: {new_url}")
            await page.goto(new_url)
            simulate_human_delay(base_delay=3, jitter=3)
            return True
        except Exception as e:
            print(f"❌ Fallback URL pagination failed: {e}")
    except Exception as e:
        print(f"❌ Failed to click next page: {e}")
    return False

async def scrape_keyword(keyword, cookies, use_limit, leads_per_keyword):
    print(f"🚀 Starting scrape for keyword: '{keyword}'")
    try:
        async with async_playwright() as p:
            print("🌐 Launching browser...")
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                user_agent=get_random_user_agent(),
                extra_http_headers=get_headers()
            )
            print("🔐 Adding cookies to browser context...")
            await context.add_cookies(cookies)
            page = await context.new_page()
            search_url = build_search_url(keyword)
            retry_attempts = 3
            # Simulate a real session by visiting homepage and user profile first
            print("🏠 Visiting LinkedIn homepage for session warm-up...")
            await page.goto("https://www.linkedin.com/feed")
            simulate_human_delay(base_delay=4, jitter=2)

            print("👤 Visiting user profile for session warm-up...")
            await page.goto("https://www.linkedin.com/in/")
            simulate_human_delay(base_delay=4, jitter=2)

            for attempt in range(retry_attempts):
                try:
                    print(f"🧭 Navigating to: {search_url} (Attempt {attempt + 1})")
                    await page.goto(search_url)
                    simulate_human_delay(base_delay=3, jitter=3)

                    # Check for block or error page
                    if "error" in page.url or "challenge" in page.url:
                        raise Exception("⚠️ LinkedIn may have blocked or rate-limited access.")

                    break  # Exit retry loop if successful
                except Exception as e:
                    print(f"❌ Attempt {attempt + 1} failed: {e}")
                    if attempt < retry_attempts - 1:
                        wait_time = (attempt + 1) * 60
                        print(f"⏳ Waiting {wait_time} seconds before retrying...")
                        await page.wait_for_timeout(wait_time * 1000)
                    else:
                        print("❌ All retry attempts failed.")
                        return

            scraped_links = []
            leads_scraped = 0

            while True:
                print("🖱️ Simulating human-like behavior...")
                await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
                await page.mouse.wheel(delta_y=random.randint(200, 600))
                simulate_human_delay(base_delay=1, jitter=1)

                print("🔎 Extracting links from current page...")
                links = await extract_company_links(page)
                new_links = [link for link in links if link not in scraped_links]
                print(f"🆕 Found {len(new_links)} new unique links.")

                with csv_lock:
                    print(f"📝 Writing new links to CSV for keyword '{keyword}'...")
                    with open(OUTPUT_FILE, 'a', newline='') as f:
                        writer = csv.writer(f)
                        for link in new_links:
                            if use_limit and leads_scraped >= leads_per_keyword:
                                print(f"⏹️ Lead cap reached for keyword '{keyword}'. Stopping CSV write.")
                                break
                            print(f"✍️ Writing link: {link}")
                            scraped_links.append(link)
                            leads_scraped += 1
                            print(f"✅ Scraped {leads_scraped} links so far for '{keyword}'.")
                            writer.writerow([link])
                    print(f"📝 Finished writing links to CSV for '{keyword}'.")

                if use_limit and leads_scraped >= leads_per_keyword:
                    print(f"✅ Reached lead cap of {leads_per_keyword} for '{keyword}'")
                    break
                print("➡️ Checking if next page is available...")
                has_next = await click_next_if_available(page)
                if not has_next:
                    print("🚫 No more pages available.")
                    break

            print(f"🏁 Finished scraping keyword '{keyword}'. Total leads scraped: {leads_scraped}")
            await context.close()
            await browser.close()
    except Exception as e:
        print(f"❌ Error during scraping keyword '{keyword}': {e}")

async def main():
    print("🚀 Script started.")
    keywords_input = input("Enter all search terms (comma-separated): ").strip()
    keywords = [kw.strip() for kw in keywords_input.split(",") if kw.strip()]
    if not keywords:
        print("❌ No valid keywords provided. Exiting.")
        return

    use_limit = input("Do you want to limit how many leads to scrape in total? (Y/N): ").strip().lower() == 'y'
    max_leads = None
    leads_per_keyword = None

    if use_limit:
        while True:
            try:
                max_leads = int(input("Enter the maximum number of leads to scrape: ").strip())
                if max_leads <= 0:
                    raise ValueError
                break
            except ValueError:
                print("Please enter a valid positive integer.")
        leads_per_keyword = max_leads // len(keywords)
        print(f"🧮 Lead cap enabled: {max_leads} total leads, ~{leads_per_keyword} per keyword.")
    else:
        print("♾️  No lead cap set. Scraping all available results.")

    if not os.path.exists(OUTPUT_FILE):
        print(f"📁 Output file does not exist. Creating: {OUTPUT_FILE}")
        with open(OUTPUT_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Company Link"])
        print("✅ Output file created with header.")

    print("🔐 Loading cookies for scraping session...")
    cookies = load_cookies_from_chrome()
    if not cookies:
        print("❌ No cookies loaded, script may not work properly.")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async def limited_scrape(keyword):
        async with semaphore:
            print(f"🔒 Acquired semaphore for keyword: '{keyword}'")
            await scrape_keyword(keyword, cookies, use_limit, leads_per_keyword)
            print(f"🔓 Released semaphore for keyword: '{keyword}'")

    print(f"📅 Starting scraping for {len(keywords)} keywords with max concurrency {MAX_CONCURRENT}.")
    await asyncio.gather(*(limited_scrape(kw) for kw in keywords))
    print("🏁 All scraping tasks completed.")

if __name__ == "__main__":
    asyncio.run(main())