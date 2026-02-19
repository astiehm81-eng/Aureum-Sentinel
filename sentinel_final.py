import os, time, random, re, sys, pandas as pd
from datetime import datetime
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright

class AureumSentinel:
    def __init__(self, segment, total_segments):
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 110
        self.start_time = time.time()
        self.task_queue = Queue()
        self.segment = segment
        self.total_segments = total_segments

    def log(self, worker_id, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] S{self.segment}W{worker_id}: {msg}", flush=True)

    def get_tradegate_universe(self):
        import requests
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=10)
            isins = sorted(list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))))
            avg = len(isins) // self.total_segments
            start = self.segment * avg
            end = start + avg if self.segment < self.total_segments - 1 else len(isins)
            return isins[start:end]
        except: return []

    def worker_process(self, worker_id):
        time.sleep(worker_id * 5.0)
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0")
            page = context.new_page()

            while not self.task_queue.empty():
                if time.time() - self.start_time > self.runtime_limit: break
                isin = self.task_queue.get()
                
                try:
                    # V154: Direkter ISIN-Pfad bei Ariva (Spiegelt L&S Kurse)
                    url = f"https://www.ariva.de/{isin}/kurs"
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    
                    # Wir suchen gezielt das Kurs-Feld von Lang & Schwarz
                    # Dieser Selektor ist extrem stabil gegen GitHub-Blockaden
                    price_selector = "span.quote__value"
                    page.wait_for_selector(price_selector, timeout=10000)
                    
                    raw_price = page.inner_text(price_selector)
                    price_match = re.search(r'(\d+[\.,]\d+)', raw_price)
                    
                    if price_match:
                        price = float(price_match.group(1).replace(',', '.'))
                        self._save(isin, price, worker_id)
                        self.log(worker_id, f"V154 ERFOLG: {isin} -> {price}")
                    else:
                        self.log(worker_id, f"V154 INFO: Kein Preis-Match bei {isin}")
                        
                except Exception as e:
                    self.log(worker_id, f"V154 FEHLER: {isin} nicht erreichbar")
                
                time.sleep(random.uniform(4, 8))
                self.task_queue.task_done()
            
            browser.close()

    def _save(self, isin, price, worker_id):
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': price, 
            'Source': f'S{self.segment}_W{worker_id}_V154'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        tasks = self.get_tradegate_universe()
        for t in tasks: self.task_queue.put(t)
        with ThreadPoolExecutor(max_workers=2) as executor:
            for i in range(2): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--segment", type=int, default=0)
    parser.add_argument("--total", type=int, default=1)
    args = parser.parse_args()
    AureumSentinel(args.segment, args.total).run()
