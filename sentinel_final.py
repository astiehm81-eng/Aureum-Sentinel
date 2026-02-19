import os, time, random, re, sys, pandas as pd
from datetime import datetime
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright
import pytesseract
from PIL import Image

class AureumSentinel:
    def __init__(self):
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 110
        self.start_time = time.time()
        self.task_queue = Queue()
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, worker_id, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] W{worker_id}: {msg}", flush=True)

    def get_tradegate_pool(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            return list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)))[:15]
        except: return []

    def worker_process(self, worker_id):
        # Massive Versatzzeit pro Worker
        time.sleep(worker_id * 20.0 + random.uniform(2, 5))
        
        with sync_playwright() as p:
            # V152 nutzt spezifische Chromium-Parameter gegen Cloudflare/Datadome
            browser = p.chromium.launch(headless=True, args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-web-security'
            ])
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0"
            )
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    self.log(worker_id, f"V152-Analyse: {isin}")
                    # Direktanwahl mit 'menschlichem' Referer
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", 
                              wait_until="load", 
                              timeout=45000)
                    
                    # Interaktives Warten & Triggering
                    for _ in range(10): # Max 10 Sekunden warten
                        page.mouse.wheel(0, 100)
                        time.sleep(1)
                        if page.locator(".price-value").is_visible():
                            break
                    
                    selector = ".price-container"
                    if page.locator(selector).first.is_visible():
                        img_path = f"snap_{worker_id}.png"
                        page.locator(selector).first.screenshot(path=img_path)
                        
                        text = pytesseract.image_to_string(Image.open(img_path), config='--psm 7')
                        price_match = re.search(r'(\d+[\.,]\d+)', text)
                        
                        if price_match:
                            price = float(price_match.group(1).replace(',', '.'))
                            self._save(isin, price, worker_id)
                            self.log(worker_id, f"ERFOLG V152: {isin} -> {price}")
                        else:
                            self.log(worker_id, f"OCR ERROR bei {isin}: Kein Text gefunden.")
                    else:
                        self.log(worker_id, f"BLOCKADE V152: Preis für {isin} bleibt unsichtbar.")
                        
                except Exception as e:
                    self.log(worker_id, f"TIMEOUT V152 bei {isin}")
                
                self.task_queue.task_done()
                time.sleep(random.uniform(15, 25))

            browser.close()

    def _save(self, isin, price, worker_id):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': price, 'Source': f'W{worker_id}_V152'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        pool = self.get_tradegate_pool()
        for isin in pool: self.task_queue.put(isin)
        with ThreadPoolExecutor(max_workers=2) as executor:
            for i in range(2): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
