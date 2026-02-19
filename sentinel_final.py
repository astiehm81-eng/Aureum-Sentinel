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

    def get_universe(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            return list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)))[:20]
        except: return []

    def worker_process(self, worker_id):
        # STAGGERED START (Eiserner Standard)
        time.sleep((worker_id * 5.0) + 0.005)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={'width': 1280, 'height': 800})
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    # 1. Startseite laden
                    page.goto("https://www.ls-x.de/de/suche", wait_until="networkidle")
                    
                    # 2. Menschliches Tippen simulieren
                    search_selector = "input[name='q']"
                    page.click(search_selector)
                    # Tippen mit Verzögerung zwischen den Tasten (Jitter)
                    for char in isin:
                        page.keyboard.type(char, delay=random.randint(50, 150))
                        time.sleep(random.uniform(0.01, 0.05))
                    
                    page.keyboard.press("Enter")
                    
                    # 3. Warten bis Seite geladen und Preis sichtbar
                    page.wait_for_selector(".price-value", timeout=15000)
                    time.sleep(random.uniform(2.0, 4.0)) # Stabilisierungszeit
                    
                    # 4. Optische Analyse (OCR)
                    img_path = f"snap_{worker_id}.png"
                    page.locator(".price-container").first.screenshot(path=img_path)
                    
                    config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                    text = pytesseract.image_to_string(Image.open(img_path), config=config)
                    
                    price_match = re.search(r'(\d+[\.,]\d+)', text)
                    if price_match:
                        price = float(price_match.group(1).replace(',', '.'))
                        self._save(isin, price, worker_id)
                        self.log(worker_id, f"ERFOLG: {isin} extrahiert: {price}")
                    else:
                        self.log(worker_id, f"OCR-FEHLER: Konnte Zahl in Bild nicht lesen für {isin}")
                        
                except Exception as e:
                    self.log(worker_id, f"TIMEOUT/FEHLER bei {isin}")
                
                time.sleep(random.uniform(3, 6)) # Pause wie ein Mensch
            
            browser.close()

    def _save(self, isin, price, worker_id):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'W{worker_id}_V147_CLICK'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        universe = self.get_universe()
        for t in universe: self.task_queue.put(t)
        with ThreadPoolExecutor(max_workers=2) as executor:
            for i in range(2): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
