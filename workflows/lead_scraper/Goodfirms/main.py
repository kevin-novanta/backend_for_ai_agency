from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import json
import concurrent.futures

selected_filters = {"Industries": [], "Employees": []}

BASE_URL = "https://www.goodfirms.co/directories"

def get_soup(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url, timeout=60000)
        html = page.content()
        browser.close()
        return BeautifulSoup(html, "html.parser")

def extract_main_categories():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(BASE_URL, timeout=60000)
        page.wait_for_selector(".explore-menu-wrapper")

        links = page.query_selector_all(".explore-menu-wrapper a.explore-title")
        categories = {}

        for link in links:
            name = link.inner_text().strip()
            href = link.get_attribute("d-href") or link.get_attribute("href")
            parent_li = link.evaluate_handle("node => node.closest('li')")
            test_id = parent_li.get_attribute("data-testid") if parent_li else "N/A"
            full_url = href if href.startswith("http") else "https://www.goodfirms.co" + href
            categories[test_id] = {
                "id": test_id,
                "name": name,
                "url": full_url
            }

        browser.close()
        return categories

def display_categories(categories):
    for idx, cat in categories.items():
        print(f"{idx}. {cat['name']}")
    print("\nEnter the IDs of categories you want to scrape (e.g., 1,3,5) or type 'all':")

def prompt_user_category_selection(categories):
    choice = input("Your choice: ").strip()
    if choice.lower() == "all":
        return list(categories.values())
    selected_ids = [id_.strip() for id_ in choice.split(",") if id_.strip() in categories]
    return [categories[id_] for id_ in selected_ids]

from playwright.async_api import async_playwright
import asyncio

async def prompt_and_set_filters(page):
    await page.wait_for_selector(".directory-filter-section", timeout=10000)
    sections = await page.query_selector_all(".directory-filter-section")

    for section in sections:
        title_el = await section.query_selector(".filter-title")
        title = (await title_el.inner_text()).strip() if title_el else ""
        if title not in selected_filters:
            continue

        print(f"\nChoose filters for {title}:")
        checkboxes = await section.query_selector_all("label")
        options = []
        for idx, label in enumerate(checkboxes):
            label_text = (await label.inner_text()).strip()
            options.append((idx + 1, label_text, label))

        for idx, label_text, _ in options:
            print(f"{idx}. {label_text}")

        choices = input("Your selection (comma-separated): ").split(",")
        for choice in choices:
            if choice.strip().isdigit():
                i = int(choice.strip()) - 1
                if 0 <= i < len(options):
                    _, _, label_el = options[i]
                    selected_filters[title].append(label_el)

    print("[INFO] Filters saved and will be reused.")

async def apply_saved_filters(page):
    for title, checkboxes in selected_filters.items():
        for checkbox in checkboxes:
            try:
                input_el = await checkbox.query_selector("input[type='checkbox']")
                if input_el:
                    await input_el.check()
            except Exception:
                continue

async def open_view_all_page(playwright, category_url, prompt_filters=False):
    browser = await playwright.chromium.launch(headless=False)
    page = await browser.new_page()
    await page.goto(category_url, timeout=60000)
    try:
        await page.wait_for_selector("a.view-more-btn", timeout=5000)
        if prompt_filters:
            await prompt_and_set_filters(page)
        await apply_saved_filters(page)
        view_all_button = await page.query_selector("a.view-more-btn")
        if view_all_button:
            await view_all_button.click()
            print(f"[INFO] Clicked 'View All' button on {category_url}")
    except Exception:
        print(f"[WARN] 'View All' button not found for {category_url}")
    await asyncio.sleep(3)  # let it load
    await browser.close()

async def main():
    print("Exploring GoodFirms categories...\n")
    categories = extract_main_categories()
    display_categories(categories)
    selected_cats = prompt_user_category_selection(categories)
    if not selected_cats:
        print("Invalid input. Exiting.")
        return

    async with async_playwright() as p:
        first = True
        for i in range(0, len(selected_cats), 5):
            batch = selected_cats[i:i+5]
            await asyncio.gather(*(open_view_all_page(p, cat["url"], first) for cat in batch))
            first = False

if __name__ == "__main__":
    asyncio.run(main())