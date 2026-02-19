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
        self.runtime_limit = 105 
        self.start_time = time.time()
        self.task_queue = Queue()
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, worker_id, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] W{worker_id}: {msg}", flush=True)

    def get_test_universe(self):
        # Wir starten mit einem kleinen Test-Pool von Tradegate
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            isins = list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)))
            return isins[:20] # Nur 20 ISINs für den Test
        except: return []

    def worker_process(self, worker_id):
        # STAGGERED START (Eiserner Standard)
        time.sleep((worker_id * 5.0) + 0.005)
        self.log(worker_id, "Test-Worker aktiv.")

        with sync_playwright() as p:
            # Goldstandard Browser-Settings
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={'width': 1280, 'height': 800})
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    self.log(worker_id, f"Rufe auf: {isin}")
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="load", timeout=30000)
                    
                    # VISUELLE GEDULD (Warten bis Preis erscheint)
                    time.sleep(5) 
                    
                    # Wir machen einen Screenshot vom Preis-Bereich
                    selector = ".price-container"
                    if page.query_selector(selector):
                        img_path = f"test_snap_{worker_id}.png"
                        page.locator(selector).first.screenshot(path=img_path)
                        
                        # OCR - Nur Zahlen und Trenner
                        config = '--psm 6 -c tessedit_char_whitelist=0123456789,.'
                        text = pytesseract.image_to_string(Image.open(img_path), config=config).strip()
                        
                        self.log(worker_id, f"OCR-Rohdaten für {isin}: '{text}'")
                        
                        price_match = re.search(r'(\d+[\.,]\d+)', text)
                        if price_match:
                            price = float(price_match.group(1).replace(',', '.'))
                            self._save(isin, price, worker_id)
                            self.log(worker_id, f"ERFOLG: {isin} -> {price}")
                        else:
                            self.log(worker_id, f"CHECK: OCR fand keine Zahl in '{text}'")
                    else:
                        self.log(worker_id, f"BLOCKADE: Selector {selector} nicht gefunden.")
                except Exception as e:
                    self.log(worker_id, f"FEHLER: {str(e)[:50]}")
                finally:
                    self.task_queue.task_done()
                    time.sleep(2)

            browser.close()

    def _save(self, isin, price, worker_id):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'W{worker_id}_V145_TEST', 'Anchor_Event': 'TRUE'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        universe = self.get_test_universe()
        for t in universe: self.task_queue.put(t)
        self.log("INIT", f"Testlauf mit {len(universe)} ISINs gestartet.")
        with ThreadPoolExecutor(max_workers=2) as executor:
            for i in range(2): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
