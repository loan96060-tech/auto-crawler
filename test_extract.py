import asyncio
import sys
import json
sys.stdout.reconfigure(encoding='utf-8')
from playwright.async_api import async_playwright

async def test_extract():
    # Đi thẳng vào 1 văn bản để test luôn tab Tải Về
    url = "https://vbpl.vn/van-ban/trung-uong/Quyet-dinh-749-QD-TTg-2020-Chuong-trinh-Chuyen-doi-so-quoc-gia-149861.aspx"
    
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
        
        # Lấy 1 link bài ngẫu nhiên từ trang chủ
        await page.goto("https://vbpl.vn", timeout=60000)
        await page.wait_for_timeout(2000)
        link = page.locator("a[href*='/van-ban/trung-uong/']").first
        url = await link.get_attribute("href")
        if url.startswith("/"):
            url = "https://vbpl.vn" + url
            
        print(f"Đang truy cập bài: {url}")
        await page.goto(url, timeout=60000)
        # Test Thuộc tính tab
        try:
            print("Đang tải dữ liệu trang...")
            await page.wait_for_timeout(3000)
            
            # Find the element that contains "Số ký hiệu" or similar
            prop_div = page.locator("div, ul, table").filter(has_text="Thuộc tính văn bản").last
            if await prop_div.count() > 0:
                print("--- PROPERTIES HTML ---")
                print(await prop_div.inner_html())
            else:
                prop_tab = page.locator("#rc-tabs-0-panel-thuoc-tinh, .ant-tabs-tabpane-active").first
                if await prop_tab.count() > 0:
                    print("--- TABS PROPERTIES HTML ---")
                    print(await prop_tab.inner_html())
                else:
                    print("--- BODY TEXT ---")
                    print(await page.locator("body").inner_text())
        except Exception as e:
            print(f"Lỗi: {e}")
        
        # Test Tải về tab
        download_links = []
        try:
            print("Đang tìm tab Tải về...")
            tai_ve_tab = page.locator("#rc-tabs-0-tab-tai-ve, div.ant-tabs-tab-btn:has-text('Tải về')").first
            count = await tai_ve_tab.count()
            print(f"Số lượng tab Tải về tìm thấy: {count}")
            
            if count > 0:
                await tai_ve_tab.click(timeout=8000)
                await page.wait_for_timeout(2000)
                
                tai_ve_pane = page.locator("#rc-tabs-0-panel-tai-ve, .ant-tabs-tabpane-active").first
                pane_count = await tai_ve_pane.count()
                print(f"Số lượng pane tìm thấy: {pane_count}")
                if pane_count > 0:
                    links = tai_ve_pane.locator("a")
                    link_count = await links.count()
                    print(f"Số lượng link tìm thấy: {link_count}")
                    for i in range(link_count):
                        l = links.nth(i)
                        href = await l.get_attribute("href")
                        text = await l.inner_text()
                        if href and not href.startswith("javascript"):
                            if href.startswith("/"):
                                href = "https://vbpl.vn" + href
                            
                            print(f"Phát hiện file: {text}. Đang tải và upload lên AAPanel...")
                            
                            # Tải file & upload
                            final_url = href
                            try:
                                api_context = context.request
                                dl_resp = await api_context.get(href)
                                if dl_resp.ok:
                                    file_content = await dl_resp.body()
                                    print(f" -> Tải file thành công, kích thước: {len(file_content)} bytes")
                                    
                                    upload_resp = await api_context.post(
                                        "https://file.timhieuluat.com/upload.php",
                                        multipart={
                                            "secret": "Hotromt2012!",
                                            "file": {
                                                "name": text.strip() + ".doc",
                                                "mimeType": "application/octet-stream",
                                                "buffer": file_content
                                            }
                                        },
                                        timeout=30000
                                    )
                                    if upload_resp.ok:
                                        try:
                                            up_json = await upload_resp.json()
                                            if up_json.get('status') == 'success':
                                                final_url = up_json.get('url')
                                                print(f" -> Upload thành công! URL mới: {final_url}")
                                            else:
                                                print(f" -> Lỗi AAPanel trả về: {up_json.get('message')}")
                                        except Exception as json_err:
                                            text_resp = await upload_resp.text()
                                            print(f" -> Lỗi parse JSON AAPanel. Raw response: {text_resp[:200]}")
                                    else:
                                        print(f" -> Lỗi HTTP AAPanel: {upload_resp.status}")
                                else:
                                    print(f" -> Lỗi tải file từ VBPL: {dl_resp.status}")
                            except Exception as e:
                                print(f" -> Lỗi Exception upload/tải: {e}")
                                
                            download_links.append({"text": text.strip(), "url": final_url})
                    print(f"Đã xử lý xong {len(download_links)} link tải về")
                else:
                    print("Không tìm thấy pane nội dung tab Tải về!")
            else:
                print("Không tìm thấy tab Tải về! Cấu trúc HTML có thể đã thay đổi.")
                
                # Print toàn bộ tab có trên trang để debug
                all_tabs = page.locator("div.ant-tabs-tab-btn")
                all_count = await all_tabs.count()
                print(f"Các tab hiện có trên trang ({all_count}):")
                for i in range(all_count):
                    t = await all_tabs.nth(i).inner_text()
                    print(f" - Tab {i}: {t}")
                    
        except Exception as e:
            print(f"Lỗi khi xử lý Tải về: {e}")
            
        with open("test_taive.json", "w", encoding="utf-8") as f:
            json.dump(download_links, f, ensure_ascii=False, indent=2)

        print("Hoàn tất test!")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(test_extract())
