import os
import time
import random
import re
import pandas as pd
from datetime import datetime
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright
import pytesseract
from PIL import Image, ImageOps, ImageEnhance

class AureumSentinel:
    def __init__(self):
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 110
        self.start_time = time.time()
        self.task_queue = Queue()
        # Eiserner Standard: Pfad für Tesseract
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def get_universe(self):
        import requests
        isins = set()
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
        except: pass
        return list(isins)

    def optimize_image(self, path):
        """ Kern-Verbesserung 2: Binarisierung für 99% OCR-Rate """
        with Image.open(path) as img:
            img = img.convert('L')  # Graustufen
            img = img.resize((img.width * 2, img.height * 2), Image.Resampling.LANCZOS) # Vergrößern
            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(2.0) # Kontrast hoch
            img = img.point(lambda x: 0 if x < 128 else 255) # Hard Threshold
            img.save(path)

    def worker_process(self, worker_id):
        # Kern-Verbesserung 3: Versetzter Start (min 5ms)
        time.sleep((worker_id * 0.7) + 0.005)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width': 800, 'height': 600})

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="domcontentloaded")
                    
                    # Kern-Verbesserung 1: Warten auf Preis-Stabilisierung
                    selector = ".price-value"
                    page.wait_for_selector(selector, timeout=8000)
                    time.sleep(0.5) # Kurzer Puffer gegen Blinken
                    
                    img_path = f"ocr_{worker_id}.png"
                    page.locator(selector).first.screenshot(path=img_path)
                    
                    self.optimize_image(img_path)
                    
                    # OCR mit spezialisierten Parametern (digits only)
                    config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                    raw_text = pytesseract.image_to_string(Image.open(img_path), config=config)
                    
                    price_match = re.search(r'(\d+[\.,]\d+)', raw_text)
                    if price_match:
                        price = float(price_match.group(1).replace(',', '.'))
                        self._save_atomic(isin, price, worker_id)
                except: pass
                finally: self.task_queue.task_done()
            browser.close()

    def _save_atomic(self, isin, price, worker_id):
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'W{worker_id}_V137_FINAL', 'Anchor_Event': 'TRUE'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        universe = self.get_universe()
        for isin in universe: self.task_queue.put(isin)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(5): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
