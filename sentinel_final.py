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
            # Holt ISINs direkt von Tradegate [2026-02-18]
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            return list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)))[:50]
        except: return []

    def worker_process(self, worker_id):
        # Versetzter Start (ms Versatz) [2026-02-18]
        time.sleep((worker_id * 10.0) + random.uniform(0.1, 0.5))
        
        with sync_playwright() as p:
            # Startet einen Browser mit Stealth-Eigenschaften
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0")
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    self.log(worker_id, f"Suche ISIN: {isin}")
                    # Navigiert zur Suche wie ein Mensch
                    page.goto("https://www.ls-x.de/de/suche", wait_until="networkidle")
                    
                    # Klick ins Suchfeld und Tippen mit Jitter
                    search_box = page.locator("input[name='q']")
                    search_box.click()
                    for char in isin:
                        page.keyboard.type(char, delay=random.randint(50, 150))
                    
                    page.keyboard.press("Enter")
                    
                    # Warten bis der Preis erscheint (Optische Validierung)
                    page.wait_for_selector(".price-value", timeout=20000)
                    time.sleep(3) # Sicherheits-Buffer für Rendering
                    
                    # Screenshot & OCR
                    img_path = f"snap_{worker_id}.png"
                    page.locator(".price-container").first.screenshot(path=img_path)
                    
                    text = pytesseract.image_to_string(Image.open(img_path), config='--psm 7')
                    price_match = re.search(r'(\d+[\.,]\d+)', text)
                    
                    if price_match:
                        price = float(price_match.group(1).replace(',', '.'))
                        self._save(isin, price, worker_id)
                        self.log(worker_id, f"GEFUNDEN: {isin} -> {price}")
                    else:
                        self.log(worker_id, f"OCR konnte nichts lesen bei {isin}")
                        
                except Exception as e:
                    self.log(worker_id, f"BLOCKADE bei {isin} (Timeout)")
                
                self.task_queue.task_done()
                time.sleep(random.uniform(5, 10)) # Abkühlphase pro ISIN

            browser.close()

    def _save(self, isin, price, worker_id):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': price, 'Source': f'W{worker_id}_V148'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        pool = self.get_tradegate_pool()
        for isin in pool: self.task_queue.put(isin)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(5): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
