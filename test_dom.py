import asyncio
from playwright.async_api import async_playwright

async def test():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                " (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()

        await page.goto("https://vbpl.vn/van-ban/trung-uong")
        await page.wait_for_timeout(3000)
        
        # Click the first document
        first_doc = page.locator("ul.listLaw > li > .item > p.title > a").first
        href = await first_doc.get_attribute("href")
        print("Visiting:", "https://vbpl.vn" + href)
        await page.goto("https://vbpl.vn" + href)
        
        await page.wait_for_timeout(3000)
        html = await page.content()
        with open("doc_html.html", "w", encoding="utf-8") as f:
            f.write(html)
        
        print("Text 'Tải về' exists:", "Tải về" in html)
        print("Text 'Lược đồ' exists:", "Lược đồ" in html)
        await browser.close()

asyncio.run(test())
