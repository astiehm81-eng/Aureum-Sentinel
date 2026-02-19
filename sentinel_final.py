import pandas as pd
import requests
import time
import os
import random
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

class AureumSentinel:
    def __init__(self):
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 120 
        self.start_time = time.time()
        self.task_queue = Queue()
        # Eiserner Standard Identitäten
        self.agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0',
            'Mozilla/5.0 (X11; Linux x86_64) Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/537.36'
        ]

    def get_expanded_universe(self):
        """ Discovery über Tradegate-Sektoren """
        isins = set()
        urls = ["https://www.tradegate.de/index.php", "https://www.tradegate.de/ausfuehrungen.php?index=DAX"]
        for url in urls:
            try:
                res = requests.get(url, timeout=5)
                isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
            except: pass
        return list(isins)

    def worker_process(self, worker_id):
        session = requests.Session()
        # Jede ISIN bekommt 50-100ms Jitter
        while not self.task_queue.empty():
            if time.time() - self.start_time > self.runtime_limit: break
            isin = self.task_queue.get()
            time.sleep(random.uniform(0.05, 0.10)) # Konservativer Jitter
            
            try:
                session.headers.update({'User-Agent': random.choice(self.agents), 'Referer': 'https://www.ls-x.de/'})
                res = session.get(f"https://www.ls-x.de/de/aktie/{isin}", timeout=7)
                match = re.search(r'price-value">([\d,.]+)', res.text)
                if match:
                    price = float(match.group(1).replace('.', '').replace(',', '.'))
                    self._save_data(isin, price, worker_id)
                else: print(f"W{worker_id}: {isin} - Preis-Tag fehlt.")
            except: pass
            finally: self.task_queue.task_done()

    def _save_data(self, isin, price, worker_id):
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'WORKER_{worker_id}_V130', 'Anchor_Event': 'TRUE'
        }]).to_csv(self.csv_path, mode='a', header=False, index=False)

    def run(self):
        universe = self.get_expanded_universe()
        for isin in universe: self.task_queue.put(isin)
        print(f"STARTE EISERNER STANDARD V130 | {len(universe)} ISINs | 50-100ms Jitter")
        with ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(10): executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run()
