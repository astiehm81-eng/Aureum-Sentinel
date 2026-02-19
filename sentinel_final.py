import os, time, random, re, sys, argparse, pandas as pd
from datetime import datetime
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright
import pytesseract
from PIL import Image

class AureumSentinel:
    def __init__(self, segment, total_segments):
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 110 # GitHub Action Limit Sicherheit
        self.start_time = time.time()
        self.task_queue = Queue()
        self.segment = segment
        self.total_segments = total_segments
        pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

    def log(self, worker_id, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] W{worker_id}: {msg}", flush=True)

    def get_tradegate_universe(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=10)
            isins = sorted(list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))))
            
            # Segmentierung der 10.000+ ISINs
            avg = len(isins) // self.total_segments
            start = self.segment * avg
            end = start + avg if self.segment < self.total_segments - 1 else len(isins)
            return isins[start:end]
        except: return []

    def worker_process(self, worker_id):
        # VERBESSERUNG: Massive Verzögerung beim Start (Human Jitter)
        # Jeder Worker wartet einen individuellen Versatz
        time.sleep((worker_id * 5.0) + random.uniform(0.1, 2.0))
        self.log(worker_id, f"Worker gestartet für Segment {self.segment}")

        with sync_playwright() as p:
            # Wir nutzen einen echten Browser-Kontext
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0")
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                
                isin = self.task_queue.get()
                
                try:
                    # Navigation zur Seite
                    page.goto(f"https://www.ls-x.de/de/aktie/{isin}", wait_until="domcontentloaded", timeout=30000)
                    
                    # Eiserner Standard: Warten auf optische Stabilisierung
                    time.sleep(random.uniform(1.5, 3.0)) 
                    
                    selector = ".price-container"
                    if page.query_selector(selector):
                        img_path = f"snap_{self.segment}_{worker_id}.png"
                        page.locator(selector).first.screenshot(path=img_path)
                        
                        # OCR Analyse
                        config = '--psm 7 -c tessedit_char_whitelist=0123456789,.'
                        text = pytesseract.image_to_string(Image.open(img_path), config=config)
                        
                        price_match = re.search(r'(\d+[\.,]\d+)', text)
                        if price_match:
                            price = float(price_match.group(1).replace(',', '.'))
                            self._save(isin, price, worker_id)
                            self.log(worker_id, f"ERFOLG: {isin} -> {price}")
                        else:
                            self.log(worker_id, f"OCR-FEHLER: {isin}")
                    else:
                        self.log(worker_id, f"BLOCKADE: Element nicht gefunden bei {isin}")
                        
                except Exception as e:
                    self.log(worker_id, f"TIMEOUT/ERROR bei {isin}")
                
                # WICHTIG: Pause zwischen den Abfragen (Kein Dauerfeuer!)
                time.sleep(random.uniform(2.0, 5.0))
                self.task_queue.task_done()
            
            browser.close()

    def _save(self, isin, price, worker_id):
        # Speicherung nach dem Eisernen Standard (V42 Ankerpunkt)
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'SEG{self.segment}_W{worker_id}', 'Anchor_Event': 'TRUE'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        tasks = self.get_tradegate_universe()
        for t in tasks: self.task_queue.put(t)
        self.log("INIT", f"Segment {self.segment} geladen mit {len(tasks)} ISINs.")
        
        # 3 Worker pro Segment (für Stabilität und IP-Schutz)
        with ThreadPoolExecutor(max_workers=3) as executor:
            for i in range(3): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--segment", type=int, default=0)
    parser.add_argument("--total", type=int, default=1)
    args = parser.parse_args()
    
    AureumSentinel(args.segment, args.total).run()
