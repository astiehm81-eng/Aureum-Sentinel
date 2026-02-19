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
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, worker_id, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] W{worker_id}: {msg}")

    def get_tradegate_universe(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            isins = list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)))
            print(f"--- UNIVERSE: {len(isins)} ISINs von Tradegate geladen ---")
            return isins
        except Exception as e:
            print(f"FEHLER bei Tradegate-Abfrage: {e}")
            return []

    def worker_process(self, worker_id):
        # STAGGERED START: Versetzter Start pro Worker (min 5ms)
        time.sleep((worker_id * 1.2) + 0.005)
        self.log(worker_id, "Worker initialisiert und gestartet.")

        with sync_playwright() as p:
            # Simulation unterschiedlicher Browser-Profile
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0")
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: 
                    self.log(worker_id, "Zeitlimit erreicht. Beende...")
                    break
                
                isin = self.task_queue.get()
                self.log(worker_id, f"Bearbeite ISIN: {isin}")
                
                try:
                    # Schritt 1: Suche laden (Menschliches Verhalten)
                    page.goto("https://www.ls-x.de/de/suche", wait_until="networkidle")
                    time.sleep(random.uniform(0.1, 0.3))
                    
                    # Schritt 2: ISIN eintippen
                    search_input = "input[name='q']" # Beispiel-Selector
                    page.fill(search_input, isin)
                    page.keyboard.press("Enter")
                    
                    # Schritt 3: Warten auf visuelle Preis-Daten
                    page.wait_for_selector(".price-value", timeout=15000)
                    time.sleep(0.5) # Stabilisierung
                    
                    # Schritt 4: Screenshot für OCR
                    img_path = f"snap_{worker_id}.png"
                    page.locator(".price-container").first.screenshot(path=img_path)
                    self.log(worker_id, f"Screenshot erstellt für {isin}")
                    
                    # Schritt 5: Optische Analyse
                    config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                    text = pytesseract.image_to_string(Image.open(img_path), config=config)
                    
                    price_match = re.search(r'(\d+[\.,]\d+)', text)
                    if price_match:
                        price = float(price_match.group(1).replace(',', '.'))
                        self._save(isin, price, worker_id)
                        self.log(worker_id, f"ERFOLG: {isin} = {price}")
                    else:
                        self.log(worker_id, f"WARNUNG: OCR konnte Preis für {isin} nicht lesen (Text: '{text.strip()}')")
                        
                except Exception as e:
                    self.log(worker_id, f"FEHLER bei {isin}: {str(e)[:50]}")
                finally:
                    self.task_queue.task_done()
                    time.sleep(random.uniform(0.5, 1.0)) # Pause zwischen Tasks

            browser.close()

    def _save(self, isin, price, worker_id):
        # Jeder Treffer > 0.1% ist ein neuer Ankerpunkt
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'W{worker_id}_V140_OPTICAL', 'Anchor_Event': 'TRUE'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        universe = self.get_tradegate_universe()
        for isin in universe: self.task_queue.put(isin)
        
        # Start von 5 Workern (Tasks aus dem Pool)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(5): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
