import os, time, random, re, pandas as pd
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
        # Tesseract-Pfad für Standard-Linux-Installation
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def get_universe(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            return list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)))
        except: return []

    def worker_process(self, worker_id):
        # STAGGERED START: Auch Worker 0 wartet mind. 5ms
        time.sleep((worker_id * 0.8) + 0.005)
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0")
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    # Navigation mit Jitter
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="networkidle", timeout=20000)
                    
                    # Warten auf das Preis-Element
                    selector = ".price-value"
                    page.wait_for_selector(selector, timeout=10000)
                    
                    # Screenshot machen
                    img_path = f"snap_{worker_id}.png"
                    page.locator(selector).first.screenshot(path=img_path)
                    
                    # OCR - Nur Zahlen und Trenner zulassen
                    config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                    text = pytesseract.image_to_string(Image.open(img_path), config=config)
                    
                    match = re.search(r'(\d+[\.,]\d+)', text)
                    if match:
                        price = float(match.group(1).replace(',', '.'))
                        self._save(isin, price, worker_id)
                        print(f"W{worker_id}: {isin} -> {price} (OCR Erfolg)")
                    else:
                        print(f"W{worker_id}: {isin} -> OCR konnte Zahl nicht lesen.")
                except Exception as e:
                    print(f"W{worker_id}: Fehler bei {isin}")
                finally:
                    self.task_queue.task_done()
            browser.close()

    def _save(self, isin, price, worker_id):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'W{worker_id}_V139_OCR', 'Anchor_Event': 'TRUE'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        universe = self.get_universe()
        for isin in universe: self.task_queue.put(isin)
        with ThreadPoolExecutor(max_workers=3) as executor: # Reduziert auf 3 Threads für Stabilität
            for i in range(3): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
