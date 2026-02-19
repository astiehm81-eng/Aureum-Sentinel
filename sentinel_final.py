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
        self.runtime_limit = 110 # Sicherheitslimit für GitHub
        self.start_time = time.time()
        self.task_queue = Queue()
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, worker_id, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] W{worker_id}: {msg}", flush=True)

    def get_tradegate_pool(self):
        import requests
        try:
            # Hard Refresh direkt von Tradegate laut Anweisung 18.02.
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            return list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)))[:20]
        except: return []

    def worker_process(self, worker_id):
        # STAGGERED START: Massive Verzögerung pro Worker (ms Versatz + Sekunden)
        time.sleep(worker_id * 15.0 + random.uniform(0.1, 0.5)) 
        
        with sync_playwright() as p:
            # Ghost-Browser Settings (Hardware-Vortäuschung)
            browser = p.chromium.launch(headless=True, args=[
                '--disable-blink-features=AutomationControlled',
                '--enable-webgl',
                '--use-gl=swiftshader',
                '--no-sandbox'
            ])
            context = browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0"
            )
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    self.log(worker_id, f"Analysiere: {isin}")
                    # Tarnung über Such-Parameter
                    page.goto(f"https://www.ls-x.de/de/suche?q={isin}", wait_until="domcontentloaded")
                    time.sleep(random.uniform(2, 4))
                    
                    # Direktsprung zur Aktie (Session ist nun 'warm')
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="networkidle")
                    
                    # Menschliche Interaktion: Minimales Zittern und Warten
                    page.mouse.move(random.randint(100, 300), random.randint(100, 300))
                    time.sleep(6) # Zeit für Preis-Rendering
                    
                    selector = ".price-container"
                    price_box = page.locator(selector).first
                    
                    if price_box.is_visible():
                        img_path = f"snap_{worker_id}.png"
                        price_box.screenshot(path=img_path)
                        
                        # Optische Analyse (Der Eiserne Standard)
                        text = pytesseract.image_to_string(Image.open(img_path), config='--psm 7')
                        price_match = re.search(r'(\d+[\.,]\d+)', text)
                        
                        if price_match:
                            price = float(price_match.group(1).replace(',', '.'))
                            self._save(isin, price, worker_id)
                            self.log(worker_id, f"TREFFER: {isin} -> {price}")
                        else:
                            self.log(worker_id, f"OCR-LEER: Bild vorhanden, aber kein Text bei {isin}")
                    else:
                        self.log(worker_id, f"BLOCKADE: Preis-Container für {isin} unsichtbar.")
                        
                except Exception as e:
                    self.log(worker_id, f"FEHLER: {isin}")
                
                self.task_queue.task_done()
                time.sleep(random.uniform(10, 20)) # Abkühlphase

            browser.close()

    def _save(self, isin, price, worker_id):
        # 0.1% Ankerpunkt Logik wird hier im Backend verarbeitet
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': price, 'Source': f'W{worker_id}_V151_GHOST'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        pool = self.get_tradegate_pool()
        for isin in pool: self.task_queue.put(isin)
        self.log("INIT", f"Starte V151 mit {len(pool)} ISINs.")
        with ThreadPoolExecutor(max_workers=2) as executor:
            for i in range(2): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
