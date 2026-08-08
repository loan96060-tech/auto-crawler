import sys
import asyncio
import os
import json
from PyQt5.QtCore import QThread, pyqtSignal, QCoreApplication
from PyQt5.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
from playwright.async_api import async_playwright

import pymysql

import base64
import time

# --- CẤU HÌNH GITHUB STORAGE ---
GITHUB_TOKEN = os.getenv("GH_TOKEN", "")
GITHUB_USERNAME = os.getenv("GH_USERNAME", "creyt2012")
GITHUB_REPO_PREFIX = "vbpl-storage"
CUSTOM_DOMAIN = "file.timhieuluat.com"
MAX_FILES_PER_REPO = 1000

async def get_github_state(source_key):
    state_file = f"github_state_{source_key}.json"
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"repo_index": 1, "file_count": 0}

async def save_github_state(state, source_key):
    state_file = f"github_state_{source_key}.json"
    try:
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=4)
    except Exception:
        pass

async def upload_to_github(api_context, filename, file_content, log_signal, source_key, lock):
    async with lock:
        state = await get_github_state(source_key)
        repo_index = state["repo_index"]
        file_count = state["file_count"]
        
        # Tạo tiền tố kho theo source_key (vd: vbpl-storage-tu hoặc vbpl-storage-dp)
        prefix = f"{GITHUB_REPO_PREFIX}-{'tu' if source_key == 'trung_uong' else 'dp'}"
        repo_name = f"{prefix}-{repo_index}"
        
        # Nếu đầy HOẶC là file đầu tiên (chưa có kho), tạo repo mới
        if file_count >= MAX_FILES_PER_REPO or file_count == 0:
            if file_count >= MAX_FILES_PER_REPO:
                repo_index += 1
                repo_name = f"{prefix}-{repo_index}"
                file_count = 0
            
            # Tạo repo mới qua API
            create_url = "https://api.github.com/user/repos"
            headers = {
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json"
            }
            create_data = {
                "name": repo_name,
                "private": False,
                "auto_init": True
            }
            log_signal.emit(f"      -> [GitHub] Đang tạo kho mới: {repo_name}...")
            resp = await api_context.post(create_url, headers=headers, data=create_data, timeout=60000)
            if resp.ok or resp.status == 422: # 422 = Kho đã tồn tại
                log_signal.emit(f"      -> [GitHub] Kho {repo_name} đã sẵn sàng.")
                state["repo_index"] = repo_index
                state["file_count"] = 0
                await save_github_state(state, source_key)
            else:
                resp_text = await resp.text()
                log_signal.emit(f"      -> [LỖI GitHub] Không thể tạo kho {repo_name}: {resp_text[:100]}")
                return False, ""
                
        # Tiến hành Upload tuần tự (Để tránh lỗi Conflict Tree của GitHub khi push song song)
        import urllib.parse
        safe_filename = urllib.parse.quote(filename.replace(' ', '_'))
        file_path = f"files/{int(time.time())}_{safe_filename}"
        upload_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{repo_name}/contents/{file_path}"
        
        encoded_content = base64.b64encode(file_content).decode('utf-8')
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        put_data = {
            "message": f"Add {filename}",
            "content": encoded_content
        }
        
        upload_resp = await api_context.put(upload_url, headers=headers, data=put_data, timeout=60000)
        if upload_resp.ok:
            state["file_count"] += 1
            await save_github_state(state, source_key)
                
            final_url = f"https://{CUSTOM_DOMAIN}/{repo_name}/main/{file_path}"
            return True, final_url
        else:
            resp_text = await upload_resp.text()
            log_signal.emit(f"      -> [Lỗi GitHub Upload]: {resp_text[:150]}")
            return False, ""

class CrawlWorker(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()

    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.crawl_async())
        loop.close()

    async def crawl_async(self):
        target_url = self.config["url"]
        is_central = "trung-uong" in target_url
        target_table = "VBPL_TRUNG_UONG" if is_central else "VBPL_DIA_PHUONG"
        wallet_path = r"C:\Users\creyt\Documents\vanbanphapluat"
        is_headless = self.config["headless"]

        # --- QUẢN LÝ FILE LOG TRẠNG THÁI CRAWL ---
        self.source_key = "trung_uong" if is_central else "dia_phuong"
        self.github_state_lock = asyncio.Lock()
        
        progress_data = {self.source_key: {"success": [], "errors": {}}}
        source_key = self.source_key

        self.log_signal.emit(f"Đang kết nối tới Remote MySQL ({self.config['db_host']})...")
        try:
            connection = pymysql.connect(
                host=self.config["db_host"],
                user=self.config["db_user"],
                password=self.config["db_pass"],
                database=self.config["db_name"],
                charset='utf8mb4'
            )
            cursor = connection.cursor()
            
            # Tạo bảng tự động (chỉ chạy nếu bảng chưa tồn tại)
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{target_table}` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `SO_HIEU` VARCHAR(255),
                    `TEN_VAN_BAN` VARCHAR(900),
                    `NOI_DUNG` LONGTEXT,
                    `TRANG_THAI` VARCHAR(255),
                    `NGAY_BAN_HANH` DATETIME,
                    `NGAY_HIEU_LUC` DATETIME,
                    `PHAM_VI` VARCHAR(255),
                    `NGUOI_KY` VARCHAR(255),
                    `NGANH` VARCHAR(255),
                    `CHUC_DANH` VARCHAR(255),
                    `CO_QUAN_BAN_HANH` VARCHAR(255),
                    `NGAY_HET_HIEU_LUC` DATETIME,
                    `LOAI_VAN_BAN` VARCHAR(255),
                    `LUOC_DO` LONGTEXT,
                    `FILE_TAI_VE` LONGTEXT,
                    INDEX (`TEN_VAN_BAN`(255))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_table_sql)
            
            # Tự động thêm các cột bị thiếu vào DB (nếu DB cũ chưa có)
            columns_to_ensure = [
                ("NGANH", "VARCHAR(255)"),
                ("CHUC_DANH", "VARCHAR(255)"),
                ("CO_QUAN_BAN_HANH", "VARCHAR(255)"),
                ("NGAY_HET_HIEU_LUC", "DATETIME"),
                ("LOAI_VAN_BAN", "VARCHAR(255)"),
                ("LUOC_DO", "LONGTEXT"),
                ("FILE_TAI_VE", "LONGTEXT"),
                ("PHAM_VI", "VARCHAR(255)"),
                ("NGUOI_KY", "VARCHAR(255)")
            ]
            for col_name, col_type in columns_to_ensure:
                try:
                    cursor.execute(f"ALTER TABLE `{target_table}` ADD COLUMN `{col_name}` {col_type};")
                except Exception:
                    # Nếu cột đã tồn tại, MySQL sẽ quăng lỗi, ta chỉ việc bỏ qua (pass)
                    pass
                    
            # Tạo bảng CRAWL_LOG
            create_log_table_sql = """
                CREATE TABLE IF NOT EXISTS `CRAWL_LOG` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `source` VARCHAR(50),
                    `page_number` INT,
                    `status` VARCHAR(20),
                    `error_details` LONGTEXT,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY `unique_source_page` (`source`, `page_number`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_log_table_sql)
            
            # Tải danh sách các trang đã thành công từ DB
            cursor.execute("SELECT page_number FROM `CRAWL_LOG` WHERE `source` = %s AND `status` = 'SUCCESS'", (self.source_key,))
            success_pages = [row[0] for row in cursor.fetchall()]
            progress_data[self.source_key]["success"] = sorted(success_pages)
            
            connection.commit()
            
            db_lock = asyncio.Lock()
            self.log_signal.emit(f"Kết nối MySQL thành công! Đích lưu: bảng {target_table}")
        except Exception as e:
            self.log_signal.emit(f"Lỗi kết nối MySQL: {e}")
            self.finished_signal.emit()
            return

        total_saved = 0
        total_skipped = 0

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=is_headless,
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
            
            self.log_signal.emit(f"Đang truy cập cổng thông tin VBPL (Chế độ {'Ẩn' if is_headless else 'Hiện'} trình duyệt)...")
            try:
                await page.goto("https://vbpl.vn", timeout=60000)
                await page.wait_for_timeout(3000)
                await page.goto(target_url, timeout=60000)
            except Exception as e:
                self.log_signal.emit(f"Lỗi truy cập trang: {e}")
                await browser.close()
                self.finished_signal.emit()
                return

            current_page = 1
            while True:
                self.log_signal.emit(f"\n--- Đang kiểm tra TRANG {current_page} ("
                                     f"{'Trung ương' if is_central else 'Địa phương'}) ---")

                # Đợi danh sách hiển thị để đảm bảo trang đã load
                try:
                    await page.wait_for_selector(".ant-skeleton", state="hidden", timeout=30000)
                    await page.wait_for_selector(".ant-list-item", state="visible", timeout=15000)
                    await page.wait_for_timeout(2000)
                except Exception:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(5000)

                item_count = await page.locator(".ant-list-item").count()
                if item_count == 0:
                    page_title = await page.title()
                    page_content = await page.content()
                    self.log_signal.emit(f"Không tìm thấy dữ liệu văn bản nào. Đã quét hết toàn bộ trang! Title: {page_title}. Nội dung HTML: {page_content[:200]}")
                    break

                highest_page = max(progress_data[source_key]["success"]) if progress_data[source_key]["success"] else 0

                # --- FAST FORWARD NẾU TRANG HIỆN TẠI ĐÃ CRAWL ---
                if current_page in progress_data[source_key]["success"]:
                    target_page = current_page + 1
                    while target_page in progress_data[source_key]["success"]:
                        target_page += 1
                        
                    self.log_signal.emit(f"[BỎ QUA FAST-FORWARD] Trang {current_page} đã cào. Tìm đường nhảy trang nhanh tới trang {target_page}...")
                    
                    # 1. Thử dùng ô nhập số trang (Quick Jumper) của Ant Design
                    quick_jumper = page.locator('.ant-pagination-options-quick-jumper input')
                    if (await quick_jumper.count()) > 0:
                        await quick_jumper.fill(str(target_page))
                        await quick_jumper.press("Enter")
                        self.log_signal.emit(f"🚀 Đã dùng Quick Jumper nhảy thẳng tới trang {target_page}!")
                        current_page = target_page
                        await page.wait_for_timeout(4000)
                        continue
                    
                    # 2. Nếu không có Quick Jumper, thử bấm thẳng vào số trang mục tiêu
                    target_page_selector = f'li[title="{target_page}"].ant-pagination-item'
                    if (await page.locator(target_page_selector).count()) > 0:
                        await page.locator(target_page_selector).first.click()
                        self.log_signal.emit(f"🚀 Đã bấm thẳng vào nút trang {target_page}!")
                        current_page = target_page
                        await page.wait_for_timeout(4000)
                        continue

                    
                    # 3. Nếu không thấy, bấm nút nhảy 5 trang (>>)
                    jump_next = page.locator('.ant-pagination-jump-next')
                    if (await jump_next.count()) > 0:
                        await jump_next.first.click()
                        await page.wait_for_timeout(1500)
                        active_page = await page.locator('.ant-pagination-item-active').get_attribute('title')
                        current_page = int(active_page) if active_page else current_page + 5
                        continue
                    
                    # 4. Fallback bấm Next (nhưng đợi cực ngắn)
                    next_btn = page.locator('.ant-pagination-next')
                    if (await next_btn.count()) > 0:
                        await next_btn.first.click()
                        await page.wait_for_timeout(1000)
                        current_page += 1
                        continue

                # --- BẮT ĐẦU TRÍCH XUẤT NẾU LÀ TRANG MỚI ---
                self.log_signal.emit(f"-> Đã load {item_count} thẻ văn bản. Đang trích xuất URL...")
                docs = []

                # LỚP 1: TRÍCH XUẤT URL THEO CẤU TRÚC HTML
                for i in range(item_count):
                    item = page.locator(".ant-list-item").nth(i)
                    
                    title_loc = item.locator("h3, .title, .ant-typography").first
                    if await title_loc.count() > 0:
                        title_text = await title_loc.inner_text()
                    else:
                        title_text = await item.inner_text()
                    title_text = title_text.strip().split('\n')[0][:100]

                    href = None
                    tag_name = await item.evaluate("el => el.tagName.toLowerCase()")
                    if tag_name == "a":
                        href = await item.get_attribute("href")
                    
                    if not href:
                        links = item.locator("a")
                        link_count = await links.count()
                        for j in range(link_count):
                            temp_href = await links.nth(j).get_attribute("href")
                            if temp_href and "javascript:" not in temp_href:
                                href = temp_href
                                break
                    
                    if href:
                        if href.startswith("/"):
                            href = "https://vbpl.vn" + href
                        elif not href.startswith("http"):
                            href = "https://vbpl.vn/" + href
                        docs.append({"url": href, "title": title_text})

                # LỚP 2 (DỰ PHÒNG): NẾU WEBSITE ẨN LINK HOÀN TOÀN BẰNG REACT ONCLICK
                if len(docs) == 0:
                    self.log_signal.emit("-> Website ẩn URL. Kích hoạt phương án Click dò đường link tự động...")
                    for i in range(item_count):
                        item = page.locator(".ant-list-item").nth(i)
                        title_loc = item.locator("h3, .title, .ant-typography").first
                        
                        target = title_loc if await title_loc.count() > 0 else item
                        title_text = await target.inner_text()
                        title_text = title_text.strip().split('\n')[0][:60]
                        
                        tabs_before = len(context.pages)
                        current_url = page.url
                        
                        await target.click(force=True)
                        await page.wait_for_timeout(3000)
                        
                        if len(context.pages) > tabs_before:
                            new_tab = context.pages[-1]
                            docs.append({"url": new_tab.url, "title": title_text})
                            await new_tab.close()
                        elif page.url != current_url:
                            docs.append({"url": page.url, "title": title_text})
                            await page.go_back()
                            await page.wait_for_selector(".ant-list-item", state="visible", timeout=15000)
                            await page.wait_for_timeout(1000)

                self.log_signal.emit(f"-> Thu thập được {len(docs)} URL. Đẩy vào 5 luồng xử lý...")

                # CHẠY 5 LUỒNG SONG SONG
                semaphore = asyncio.Semaphore(5)
                
                page_has_error = False
                page_error_details = []

                async def process_document(doc_info, index):
                    nonlocal total_saved, total_skipped, page_has_error, page_error_details
                    link = doc_info['url']
                    short_title = doc_info['title'][:60]
                    
                    async with semaphore:
                        self.log_signal.emit(f"  [Luồng {index}/{len(docs)}] Xử lý: {short_title}...")
                        new_page = await context.new_page()
                        try:
                            await new_page.goto(link, timeout=60000)
                            await new_page.wait_for_function(
                                "() => !document.body.innerText.includes('Đang tải dữ liệu...')", 
                                timeout=20000
                            )
                            await new_page.wait_for_timeout(1500)
                            
                            title_detail = new_page.locator("h1, .document-title, .title, .vbProperties_Title").first
                            ten_vb = (await title_detail.inner_text()).strip() if (await title_detail.count()) > 0 else short_title
                            ten_vb = ten_vb[:900]
                            
                            # Kiểm tra trùng lặp NGAY TỪ ĐẦU để tránh tải file và DOM thừa
                            async with db_lock:
                                try:
                                    connection.ping(reconnect=False)
                                except Exception:
                                    connection = pymysql.connect(
                                        host=self.config["db_host"],
                                        user=self.config["db_user"],
                                        password=self.config["db_pass"],
                                        database=self.config["db_name"],
                                        charset='utf8mb4'
                                    )
                                    cursor = connection.cursor()
                                check_sql = f"SELECT COUNT(*) FROM `{target_table}` WHERE `TEN_VAN_BAN` = %s"
                                cursor.execute(check_sql, (ten_vb,))
                                exists = cursor.fetchone()[0]

                            if exists > 0:
                                total_skipped += 1
                                self.log_signal.emit(f"    -> [TRÙNG] Đã có trong Database ({short_title}), bỏ qua cào nội dung và tải file.")
                                return

                            
                            c_elem = new_page.locator(".preview-content, #rc-tabs-0-panel-toan-van, div.content, article").first
                            noi_dung = (await c_elem.inner_html()).strip() if (await c_elem.count()) > 0 else ten_vb
                            
                            # BẮT BUỘC ĐỢI RENDER ĐỂ TRÁNH LỖI MẠNG KHI CHẠY ĐA LUỒNG
                            props_dict = {}
                            try:
                                # Chờ tối đa 8 giây cho tab hoặc card Thuộc tính xuất hiện
                                # Dùng or để tìm 1 trong các chuẩn giao diện
                                prop_container = new_page.locator("#rc-tabs-0-tab-thuoc-tinh, div.ant-tabs-tab-btn:has-text('Thuộc tính'), div:has-text('Thuộc tính văn bản')").last
                                try:
                                    await prop_container.wait_for(state="attached", timeout=8000)
                                except Exception:
                                    pass
                                    
                                prop_pane = None
                                
                                # Nếu là dạng tab cũ thì click
                                prop_tab = new_page.locator("#rc-tabs-0-tab-thuoc-tinh, div.ant-tabs-tab-btn:has-text('Thuộc tính')").first
                                if (await prop_tab.count()) > 0:
                                    await prop_tab.click(timeout=8000)
                                    await new_page.wait_for_timeout(2000)
                                    prop_pane = new_page.locator("#rc-tabs-0-panel-thuoc-tinh, .ant-tabs-tabpane-active").first
                                else:
                                    # Nếu là dạng card mới (không có tab)
                                    card = new_page.locator("div.ant-card, div.card, div").filter(has_text="Thuộc tính văn bản").last
                                    if (await card.count()) > 0:
                                        prop_pane = card
                                
                                if prop_pane and (await prop_pane.count()) > 0:
                                    # Parse bằng DOM JS thay vì innerText để tránh lỗi mất cột
                                    props_dict_js = await prop_pane.evaluate("""(pane) => {
                                        let res = {};
                                        let cleanKey = (k) => k.replace(/:$/, '').trim();
                                        
                                        // 1. Thử tìm theo table row (tr > th/td)
                                        pane.querySelectorAll('tr').forEach(tr => {
                                            let cells = tr.querySelectorAll('th, td');
                                            // vbpl.vn thường ghép 2 cột (4 ô th/td) trên 1 dòng
                                            for (let i = 0; i < cells.length - 1; i += 2) {
                                                let key = cleanKey(cells[i].innerText);
                                                if (key) {
                                                    res[key] = cells[i+1].innerText.trim();
                                                }
                                            }
                                        });
                                        // 2. Thử tìm theo cấu trúc của Ant Design (Descriptions)
                                        pane.querySelectorAll('.ant-descriptions-item').forEach(item => {
                                            let label = item.querySelector('.ant-descriptions-item-label');
                                            let content = item.querySelector('.ant-descriptions-item-content');
                                            if (label && content) {
                                                res[cleanKey(label.innerText)] = content.innerText.trim();
                                            }
                                        });
                                        // 3. Fallback tìm theo các div ngang hàng (nếu web dùng grid/flex)
                                        pane.querySelectorAll('.ant-row, .row, li').forEach(row => {
                                            if (row.children.length >= 2 && !row.querySelector('table')) {
                                                for (let i = 0; i < row.children.length - 1; i += 2) {
                                                    let key = cleanKey(row.children[i].innerText);
                                                    if (key) {
                                                        res[key] = row.children[i+1].innerText.trim();
                                                    }
                                                }
                                            }
                                        });
                                        return res;
                                    }""")
                                    
                                    for k, v in props_dict_js.items():
                                        props_dict[k] = v
                                        
                                    if not props_dict:
                                        raw_props = (await prop_pane.inner_text()).strip().split('\n')
                                        known_keys = ["Số hiệu", "Loại văn bản", "Cơ quan ban hành", "Người ký", "Chức danh", "Ngành", "Phạm vi", "Ngày ban hành", "Ngày có hiệu lực", "Ngày hết hiệu lực", "Tình trạng hiệu lực"]
                                        for i in range(len(raw_props)):
                                            line = raw_props[i].strip()
                                            line_clean = line.replace(':', '').strip()
                                            if line_clean in known_keys:
                                                val = ""
                                                if i + 1 < len(raw_props):
                                                    next_line = raw_props[i+1].strip()
                                                    if next_line.replace(':', '').strip() not in known_keys:
                                                        val = next_line
                                                props_dict[line_clean] = val
                            except Exception as e:
                                pass
                                
                            # CLICK LẤY DỮ LIỆU TAB LƯỢC ĐỒ (HTML)
                            luoc_do_html = ""
                            try:
                                luoc_do_tab = new_page.locator("#rc-tabs-0-tab-luoc-do, div.ant-tabs-tab-btn:has-text('Lược đồ')").first
                                if (await luoc_do_tab.count()) > 0:
                                    await luoc_do_tab.click(timeout=8000)
                                    await new_page.wait_for_timeout(2000)
                                    
                                    luoc_do_pane = new_page.locator("#rc-tabs-0-panel-luoc-do, .ant-tabs-tabpane-active").first
                                    if (await luoc_do_pane.count()) > 0:
                                        luoc_do_html = await luoc_do_pane.inner_html()
                            except Exception:
                                pass
                                
                            # CLICK LẤY DỮ LIỆU TAB TẢI VỀ (JSON LINKS)
                            tai_ve_json = ""
                            try:
                                tai_ve_tab = new_page.locator("#rc-tabs-0-tab-tai-ve, div.ant-tabs-tab-btn:has-text('Tải về')").first
                                if (await tai_ve_tab.count()) > 0:
                                    await tai_ve_tab.click(timeout=8000)
                                    await new_page.wait_for_timeout(2000)
                                    
                                    tai_ve_pane = new_page.locator("#rc-tabs-0-panel-tai-ve, .ant-tabs-tabpane-active").first
                                    if (await tai_ve_pane.count()) > 0:
                                        links_data = []
                                        # Hỗ trợ thẻ <a> cũ
                                        links = tai_ve_pane.locator("a")
                                        link_count = await links.count()
                                        for i in range(link_count):
                                            l = links.nth(i)
                                            href = await l.get_attribute("href")
                                            if href and not href.startswith("javascript"):
                                                if href.startswith("/"):
                                                    href = "https://vbpl.vn" + href
                                                text = await l.inner_text()
                                                
                                                final_url = href
                                                if True:  # Luôn kích hoạt upload GitHub
                                                    try:
                                                        api_context = context.request
                                                        dl_resp = await api_context.get(href)
                                                        if dl_resp.ok:
                                                            file_content = await dl_resp.body()
                                                            
                                                            # Kiểm tra nếu server trả về HTML (trang lỗi hoặc trang View)
                                                            content_type = dl_resp.headers.get('content-type', '').lower()
                                                            if 'text/html' in content_type or file_content.startswith(b'<'):
                                                                self.log_signal.emit(f"      -> [Bỏ qua]: Link trả về mã HTML chứ không phải file tài liệu.")
                                                                continue
                                                            
                                                            import urllib.parse
                                                            import os
                                                            filename = "document.doc"
                                                            cd = dl_resp.headers.get('content-disposition', '')
                                                            if 'filename=' in cd:
                                                                filename = cd.split('filename=')[1].strip('"').strip("'")
                                                            else:
                                                                parsed = urllib.parse.urlparse(href)
                                                                filename = os.path.basename(parsed.path) or "document.doc"
                                                                
                                                            if filename.lower().endswith(('.html', '.htm')):
                                                                continue
                                                                
                                                            success, git_url = await upload_to_github(api_context, filename, file_content, self.log_signal, self.source_key, self.github_state_lock)
                                                            if success:
                                                                final_url = git_url
                                                                self.log_signal.emit(f"      -> [Tải file thành công]: {final_url}")
                                                            else:
                                                                self.log_signal.emit(f"      -> [Lỗi Upload GitHub]")
                                                                page_has_error = True
                                                                page_error_details.append(f"Upload lỗi: {filename}")
                                                    except Exception as e:
                                                        self.log_signal.emit(f"      -> [Lỗi Tải/Upload API]: {str(e)[:100]}")
                                                links_data.append({"text": text.strip(), "url": final_url})
                                                
                                        # Hỗ trợ nút <button> mới có icon download
                                        # Chỉ lấy các button có path d chứa 'M8 10V2' (đây là hình mũi tên tải xuống)
                                        buttons = tai_ve_pane.locator("button:has(svg path[d*='M8 10V2'])")
                                        btn_count = await buttons.count()
                                        if btn_count == 0:
                                            # Fallback nếu không có mũi tên tải xuống, thử lấy tất cả icon-only
                                            buttons = tai_ve_pane.locator("button.ant-btn-icon-only")
                                            btn_count = await buttons.count()
                                            
                                        if btn_count > 0 and len(links_data) == 0:
                                            for i in range(btn_count):
                                                btn = buttons.nth(i)
                                                try:
                                                    async with new_page.expect_download(timeout=60000) as download_info:
                                                        await btn.click()
                                                    download = await download_info.value
                                                    filename = download.suggested_filename
                                                    
                                                    # Bỏ qua nếu là file html (thường là nút Xem Trước)
                                                    if filename.lower().endswith(('.html', '.htm')):
                                                        self.log_signal.emit(f"      -> [Bỏ qua]: Nút này tải về file HTML ({filename}).")
                                                        continue
                                                        
                                                    import tempfile
                                                    import os
                                                    tmp_path = os.path.join(tempfile.gettempdir(), filename)
                                                    await download.save_as(tmp_path)
                                                    
                                                    with open(tmp_path, "rb") as f:
                                                        file_content = f.read()
                                                    os.remove(tmp_path)
                                                    
                                                    # Kiểm tra nội dung có phải là HTML không
                                                    if file_content.startswith(b'<html') or file_content.startswith(b'<!DOC'):
                                                        self.log_signal.emit(f"      -> [Bỏ qua]: Nội dung file tải về là HTML.")
                                                        continue
                                                    
                                                    final_url = ""
                                                    if True:  # Luôn kích hoạt upload GitHub
                                                        api_context = context.request
                                                        success, git_url = await upload_to_github(api_context, filename, file_content, self.log_signal, self.source_key, self.github_state_lock)
                                                        if success:
                                                            final_url = git_url
                                                            self.log_signal.emit(f"      -> [Tải file thành công (Button)]: {final_url}")
                                                        else:
                                                            self.log_signal.emit(f"      -> [Lỗi Upload GitHub]")
                                                            page_has_error = True
                                                            page_error_details.append(f"Upload lỗi: {filename}")
                                                    links_data.append({"text": filename, "url": final_url})
                                                except Exception as e:
                                                    self.log_signal.emit(f"      -> [Lỗi tải file bằng button]: {str(e)[:100]}")
                                                    
                                        if links_data:
                                            tai_ve_json = json.dumps(links_data, ensure_ascii=False)
                            except Exception:
                                pass
                            
                            # Chuẩn hóa keys trong props_dict
                            normalized_props = {}
                            for k, v in props_dict.items():
                                if k:
                                    normalized_props[k.replace('\xa0', ' ').strip()] = v
                            props_dict = normalized_props
                            
                            # Bóc tách các trường từ Dictionary Thuộc tính
                            so_hieu = props_dict.get("Số hiệu", props_dict.get("Số hiệu văn bản", "Đang cập nhật"))[:100]
                            nguoi_ky = props_dict.get("Người ký", "Cơ quan thẩm quyền")[:255]
                            trang_thai = props_dict.get("Tình trạng hiệu lực", "Còn hiệu lực")[:255]
                            nganh = props_dict.get("Ngành", "")[:255]
                            chuc_danh = props_dict.get("Chức danh", "")[:255]
                            co_quan_ban_hanh = props_dict.get("Cơ quan ban hành", "")[:255]
                            loai_van_ban = props_dict.get("Loại văn bản", "")[:255]
                            
                            # Hàm chuyển đổi chuỗi ngày DD/MM/YYYY sang format YYYY-MM-DD của MySQL
                            import datetime
                            def parse_date(date_str):
                                if not date_str or date_str == "--" or "Đang cập nhật" in date_str:
                                    return None
                                try:
                                    return datetime.datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d %H:%M:%S")
                                except:
                                    return None
                                    
                            ngay_ban_hanh = parse_date(props_dict.get("Ngày ban hành", ""))
                            ngay_hieu_luc = parse_date(props_dict.get("Ngày có hiệu lực", ""))
                            ngay_het_hieu_luc = parse_date(props_dict.get("Ngày hết hiệu lực", ""))

                            async with db_lock:
                                try:
                                    connection.ping(reconnect=False)
                                except Exception:
                                    connection = pymysql.connect(
                                        host=self.config["db_host"],
                                        user=self.config["db_user"],
                                        password=self.config["db_pass"],
                                        database=self.config["db_name"],
                                        charset='utf8mb4'
                                    )
                                    cursor = connection.cursor()
                                insert_sql = f"""
                                    INSERT INTO `{target_table}` (SO_HIEU, TEN_VAN_BAN, NOI_DUNG, TRANG_THAI, NGAY_BAN_HANH, NGAY_HIEU_LUC, PHAM_VI, NGUOI_KY, NGANH, CHUC_DANH, CO_QUAN_BAN_HANH, NGAY_HET_HIEU_LUC, LOAI_VAN_BAN, LUOC_DO, FILE_TAI_VE)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                                cursor.execute(insert_sql, (
                                    so_hieu, 
                                    ten_vb, 
                                    noi_dung, 
                                    trang_thai, 
                                    ngay_ban_hanh if ngay_ban_hanh else datetime.datetime.now(), 
                                    ngay_hieu_luc if ngay_hieu_luc else datetime.datetime.now(), 
                                    "Trung ương" if is_central else "Địa phương", 
                                    nguoi_ky,
                                    nganh,
                                    chuc_danh,
                                    co_quan_ban_hanh,
                                    ngay_het_hieu_luc if ngay_het_hieu_luc else None,
                                    loai_van_ban,
                                    luoc_do_html,
                                    tai_ve_json
                                ))
                                connection.commit()
                                total_saved += 1
                                self.log_signal.emit(f"    -> [ĐÃ LƯU] SH: {so_hieu} | Ký: {nguoi_ky}")
                                    
                        except Exception as e:
                            page_has_error = True
                            page_error_details.append(f"{short_title}: {str(e)[:100]}")
                            self.log_signal.emit(f"    -> [LỖI] Cào ({short_title}): {str(e)[:100]}...")
                        finally:
                            await new_page.close()

                tasks = []
                for idx, doc_info in enumerate(docs):
                    tasks.append(asyncio.create_task(process_document(doc_info, idx + 1)))
                    await asyncio.sleep(2)  # Mở luồng cách nhau 2 giây để tránh bị chặn IP
                
                if tasks:
                    await asyncio.gather(*tasks)

                # --- LƯU TRẠNG THÁI VÀO CRAWL_LOG (MySQL) ---
                if not page_has_error:
                    if current_page not in progress_data[source_key]["success"]:
                        progress_data[source_key]["success"].append(current_page)
                        progress_data[source_key]["success"].sort()
                    try:
                        cursor.execute("""
                            INSERT INTO `CRAWL_LOG` (source, page_number, status, error_details) 
                            VALUES (%s, %s, %s, %s) 
                            ON DUPLICATE KEY UPDATE status = VALUES(status), error_details = VALUES(error_details)
                        """, (source_key, current_page, 'SUCCESS', ''))
                        connection.commit()
                        self.log_signal.emit(f"[LƯU LOG] Đã lưu tiến độ Trang {current_page} (SUCCESS) vào MySQL.")
                    except Exception as e:
                        self.log_signal.emit(f"[CẢNH BÁO] Không thể lưu log vào MySQL: {e}")
                else:
                    self.log_signal.emit(f"[CẢNH BÁO] Trang {current_page} có lỗi, lưu chi tiết vào MySQL để cào lại sau.")
                    try:
                        cursor.execute("""
                            INSERT INTO `CRAWL_LOG` (source, page_number, status, error_details) 
                            VALUES (%s, %s, %s, %s) 
                            ON DUPLICATE KEY UPDATE status = VALUES(status), error_details = VALUES(error_details)
                        """, (source_key, current_page, 'ERROR', str(page_error_details)))
                        connection.commit()
                    except Exception as e:
                        self.log_signal.emit(f"[CẢNH BÁO] Không thể lưu log lỗi vào MySQL: {e}")
                    
                # --- CHUYỂN SANG TRANG TIẾP THEO ---
                next_page_num = current_page + 1
                self.log_signal.emit(f"\n-> Đang tìm cách chuyển sang trang {next_page_num}...")
                try:
                    await page.bring_to_front()
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    
                    next_page_clicked = False
                    selectors_to_try = [
                        f'li[title="{next_page_num}"].ant-pagination-item',
                        f'a[title="Trang {next_page_num}"]',
                        f"a:has-text('{next_page_num}')",
                        ".ant-pagination-next"
                    ]
                    
                    for selector in selectors_to_try:
                        if (await page.locator(selector).count()) > 0:
                            el = page.locator(selector).first
                            class_name = await el.get_attribute("class")
                            if class_name and "disabled" in class_name:
                                continue
                            
                            await el.click()
                            next_page_clicked = True
                            break
                            
                    if not next_page_clicked:
                        self.log_signal.emit("Đã quét tới trang cuối cùng (Không tìm thấy nút chuyển trang tiếp theo). Kết thúc!")
                        break
                        
                    await page.wait_for_timeout(4000)
                    current_page += 1
                except Exception as e:
                    self.log_signal.emit(f"Không thể chuyển trang: {e}. Hoàn tất quy trình.")
                    break

            await browser.close()
            cursor.close()
            connection.close()

        self.log_signal.emit(
            f"\n=== BÁO CÁO KẾT QUẢ TOÀN BỘ ==="
            f"\n- Đã tải và lưu thành công: {total_saved} văn bản"
            f"\n- Đã lọc bỏ (trùng dữ liệu): {total_skipped} văn bản"
        )
        self.finished_signal.emit()





class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Tool Crawl VBPL - Pro Version (Tự Động Hết Trang)")
        self.resize(950, 650)
        self.init_ui()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        tab1 = QWidget()
        tab1_layout = QVBoxLayout(tab1)
        form_layout = QFormLayout()

        self.dsn_input = QLineEdit(os.getenv("DB_HOST", "161.153.108.144"))
        self.user_input = QLineEdit(os.getenv("DB_USER", "timhieuluat"))
        self.pass_input = QLineEdit(os.getenv("DB_PASS", ""))
        self.pass_input.setEchoMode(QLineEdit.Password)
        self.db_input = QLineEdit(os.getenv("DB_NAME", "timhieuluat"))

        self.url_combo = QComboBox()
        self.url_combo.addItem("Trung Ương", "https://vbpl.vn/van-ban/trung-uong")
        self.url_combo.addItem("Địa Phương", "https://vbpl.vn/van-ban/dia-phuong")
        
        self.headless_checkbox = QCheckBox("Chạy ngầm (Ẩn trình duyệt để nhẹ máy)")
        self.headless_checkbox.setChecked(True)

        form_layout.addRow("MySQL Host IP:", self.dsn_input)
        form_layout.addRow("MySQL User:", self.user_input)
        form_layout.addRow("MySQL Password:", self.pass_input)
        form_layout.addRow("MySQL Database:", self.db_input)
        form_layout.addRow("Nguồn VBPL:", self.url_combo)
        form_layout.addRow("Tùy chọn:", self.headless_checkbox)
        
        # Đã gỡ bỏ cấu hình API Upload File cũ (chuyển sang dùng GitHub)
        
        tab1_layout.addLayout(form_layout)

        self.start_btn = QPushButton("Bắt đầu Tự Động Crawl Tất Cả Trang")
        self.start_btn.setStyleSheet(
            "background-color: #2e7d32; color: white; font-weight: bold; font-size: 14px; padding: 8px;"
        )
        self.start_btn.clicked.connect(self.start_crawling)
        tab1_layout.addWidget(self.start_btn)

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        tab1_layout.addWidget(self.log_output)

        self.tabs.addTab(tab1, "Crawl & Lọc Trùng")

        tab2 = QWidget()
        tab2_layout = QVBoxLayout(tab2)

        db_form_layout = QHBoxLayout()
        self.table_input = QLineEdit("VBPL_TRUNG_UONG")
        self.load_db_btn = QPushButton("Xem dữ liệu MySQL")
        self.load_db_btn.setStyleSheet(
            "background-color: #0288d1; color: white; font-weight: bold; padding: 6px;"
        )
        self.load_db_btn.clicked.connect(self.load_mysql_table)

        db_form_layout.addWidget(QLabel("Tên bảng (VBPL_TRUNG_UONG / VBPL_DIA_PHUONG):"))
        db_form_layout.addWidget(self.table_input)
        db_form_layout.addWidget(self.load_db_btn)
        tab2_layout.addLayout(db_form_layout)

        self.db_table_view = QTableWidget()
        tab2_layout.addWidget(self.db_table_view)

        self.tabs.addTab(tab2, "Quản lý & Xem Bảng Database")

    def start_crawling(self):
        config = {
            "db_host": self.dsn_input.text().strip(),
            "db_user": self.user_input.text().strip(),
            "db_pass": self.pass_input.text().strip(),
            "db_name": self.db_input.text().strip(),
            "url": self.url_combo.currentData(),
            "headless": self.headless_checkbox.isChecked(),

        }
        self.start_btn.setEnabled(False)
        self.log_output.clear()

        self.worker = CrawlWorker(config)
        self.worker.log_signal.connect(self.append_log)
        self.worker.finished_signal.connect(self.crawl_finished)
        self.worker.start()

    def append_log(self, text):
        self.log_output.append(text)
        print(text, flush=True)
        scrollbar = self.log_output.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def crawl_finished(self):
        self.start_btn.setEnabled(True)

    def load_mysql_table(self):
        table_name = self.table_input.text().strip().upper()
        try:
            connection = pymysql.connect(
                host=self.dsn_input.text().strip(),
                user=self.user_input.text().strip(),
                password=self.pass_input.text().strip(),
                database=self.db_input.text().strip(),
                charset='utf8mb4'
            )
            cursor = connection.cursor()

            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 100")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            self.db_table_view.setRowCount(len(rows))
            self.db_table_view.setColumnCount(len(columns))
            self.db_table_view.setHorizontalHeaderLabels(columns)

            for row_idx, row_data in enumerate(rows):
                for col_idx, col_data in enumerate(row_data):
                    item = QTableWidgetItem(str(col_data) if col_data is not None else "")
                    self.db_table_view.setItem(row_idx, col_idx, item)

            self.db_table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            cursor.close()
            connection.close()
            self.append_log(f"Đã tải thành công dữ liệu từ MySQL bảng {table_name} ({len(rows)} dòng)")
        except Exception as e:
            self.append_log(f"Lỗi tải dữ liệu bảng: {e}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    if "--auto" in sys.argv:
        # Chế độ chạy ngầm song song 2 mảng (Trung Ương + Địa Phương)
        window.headless_checkbox.setChecked(True)
        window.start_btn.setEnabled(False)
        window.log_output.clear()
        
        config = {
            "db_host": window.dsn_input.text().strip(),
            "db_user": window.user_input.text().strip(),
            "db_pass": window.pass_input.text().strip(),
            "db_name": window.db_input.text().strip(),
            "headless": True
        }
        
        config_tu = config.copy()
        config_tu["url"] = "https://vbpl.vn/van-ban/trung-uong"
        
        config_dp = config.copy()
        config_dp["url"] = "https://vbpl.vn/van-ban/dia-phuong"
        
        worker_tu = CrawlWorker(config_tu)
        worker_dp = CrawlWorker(config_dp)
        
        window.completed_workers = 0
        def on_worker_finished():
            window.completed_workers += 1
            if window.completed_workers >= 2:
                window.append_log("Hoàn thành toàn bộ quá trình cào. Đang thoát...")
                QCoreApplication.quit()
        
        worker_tu.log_signal.connect(window.append_log)
        worker_tu.finished_signal.connect(on_worker_finished)
        
        worker_dp.log_signal.connect(window.append_log)
        worker_dp.finished_signal.connect(on_worker_finished)
        
        # Lưu tham chiếu để tránh bị Garbage Collector xoá mất worker
        window.worker_tu = worker_tu
        window.worker_dp = worker_dp
        
        worker_tu.start()
        worker_dp.start()
    else:
        window.show()
    sys.exit(app.exec_())