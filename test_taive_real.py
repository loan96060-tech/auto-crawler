import asyncio
from playwright.async_api import async_playwright
import json

async def run():
    url = "https://vbpl.vn/van-ban/trung-uong/Quyet-dinh-76-2024-QD-UBND-Ha-Noi-Quy-dinh-thoi-gian-ban-hanh-100234.aspx"
    # Using a known document that might have attachments (from user's screenshot Quyết định 76)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context()
        page = await context.new_page()
        
        print(f"Going to {url}")
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(3000)
        
        print("Looking for Tải về tab...")
        tabs = page.locator("div.ant-tabs-tab-btn")
        count = await tabs.count()
        print(f"Found {count} tabs total.")
        
        tai_ve_tab = None
        for i in range(count):
            text = await tabs.nth(i).inner_text()
            print(f"Tab {i}: {text}")
            if "Tải về" in text:
                tai_ve_tab = tabs.nth(i)
                break
                
        if tai_ve_tab:
            print("Clicking Tải về tab...")
            await tai_ve_tab.click()
            await page.wait_for_timeout(2000)
            
            # Print the HTML of the active pane
            active_pane = page.locator(".ant-tabs-tabpane-active").first
            html = await active_pane.inner_html()
            print("Active Pane HTML:")
            print(html[:500] + "..." if len(html) > 500 else html)
            
            links = active_pane.locator("a")
            link_count = await links.count()
            print(f"Found {link_count} 'a' tags in active pane")
            
            for i in range(link_count):
                a = links.nth(i)
                href = await a.get_attribute("href")
                text = await a.inner_text()
                print(f"Link {i}: text='{text}', href='{href}'")
                
                # Test downloading it
                if href and not href.startswith("javascript"):
                    full_href = href
                    if href.startswith("/"):
                        full_href = "https://vbpl.vn" + href
                    
                    print(f"Testing download for {full_href}...")
                    api_context = context.request
                    resp = await api_context.get(full_href)
                    print(f"Download status: {resp.status}")
                    
                    if resp.ok:
                        content = await resp.body()
                        print(f"Downloaded {len(content)} bytes")
                        
                        # Test uploading to aapanel
                        print("Uploading to aapanel...")
                        upload_resp = await api_context.post(
                            "https://file.timhieuluat.com/upload.php",
                            multipart={
                                "secret": "Hotromt2012!",
                                "file": {
                                    "name": f"test_file_{i}.doc",
                                    "mimeType": "application/octet-stream",
                                    "buffer": content
                                }
                            }
                        )
                        print(f"Upload status: {upload_resp.status}")
                        if upload_resp.ok:
                            print("Upload response:", await upload_resp.text())
                            
        else:
            print("Could not find Tải về tab.")
            
        await browser.close()

asyncio.run(run())
