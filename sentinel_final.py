import os
import time
import random
import re
import pandas as pd
from datetime import datetime
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
try:
    from playwright.sync_api import sync_playwright
    import pytesseract
    from PIL import Image
except ImportError:
    print("Abhängigkeiten fehlen. Bitte Workflow-Installation prüfen.")

class AureumSentinel:
    def __init__(self):
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 110
        self.start_time = time.time()
        self.task_queue = Queue()
        # Pfad für Tesseract in GitHub Actions Ubuntu
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def get_universe(self):
        """ Discovery via Tradegate (Eiserner Standard Vorgabe) """
        import requests
        isins = set()
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
        except: pass
        return list(isins)[:40] # Test-Batch

    def worker_process(self, worker_id):
        # STAGGERED START: Mindestens 5ms + ID-Versatz
        initial_wait = (worker_id * 0.5) + 0.005 + random.uniform(0.005, 0.015)
        time.sleep(initial_wait)

        with sync_playwright() as p:
            # Browser-Tarnung: Echtes Chromium-Profil
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={'width': 1280, 'height': 720})
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    # Navigation mit menschlichem Jitter
                    time.sleep(random.uniform(0.2, 0.5))
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="networkidle", timeout=15000)
                    
                    # Screenshot vom Preis-Container
                    # Wir zielen auf das visuelle Element, das die Firewall nicht verstecken kann
                    selector = ".price-container"
                    if page.query_selector(selector):
                        img_path = f"snap_{worker_id}.png"
                        page.locator(selector).first.screenshot(path=img_path)
                        
                        # OPTISCHE AUSWERTUNG (OCR)
                        raw_text = pytesseract.image_to_string(Image.open(img_path))
                        # Extrahiere nur die Zahlen und das Komma/Punkt
                        price_match = re.search(r'(\d+[\.,]\d+)', raw_text)
                        
                        if price_match:
                            price_str = price_match.group(1).replace(',', '.')
                            price = float(price_str)
                            self._save_atomic(isin, price, worker_id)
                        else:
                            print(f"W{worker_id}: {isin} - OCR konnte Text nicht lesen.")
                except Exception as e:
                    print(f"W{worker_id}: Fehler bei {isin}")
                finally:
                    self.task_queue.task_done()
            browser.close()

    def _save_atomic(self, isin, price, worker_id):
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'W{worker_id}_V135_OPTICAL', 'Anchor_Event': 'TRUE'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        universe = self.get_universe()
        for isin in universe: self.task_queue.put(isin)
        print(f"--- AUREUM SENTINEL V135 (OPTICAL) ---")
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(5): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
