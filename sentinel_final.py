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
        self.runtime_limit = 110
        self.start_time = time.time()
        self.task_queue = Queue()
        self.success_count = 0
        # Eiserner Standard Header
        self.agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/537.36'
        ]

    def log(self, message):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def get_universe(self):
        """ Discovery mit Fehler-Logging """
        isins = set()
        try:
            res = requests.get("https://www.tradegate.de/index.php", timeout=5)
            found = re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)
            isins.update(found)
            self.log(f"Discovery: {len(found)} ISINs auf Tradegate gefunden.")
        except Exception as e:
            self.log(f"FEHLER bei Discovery: {e}")
        return list(isins)

    def worker_process(self, worker_id):
        # Verbesserung 1: Staggered Start (min 5ms)
        initial_wait = (worker_id * 0.5) + 0.005
        time.sleep(initial_wait)
        
        session = requests.Session()
        
        while not self.task_queue.empty():
            if time.time() - self.start_time > self.runtime_limit: break
            isin = self.task_queue.get()
            
            # Verbesserung 2: Individueller Jitter (150-450ms)
            time.sleep(random.uniform(0.15, 0.45))
            
            try:
                # Verbesserung 3: Direkter JSON-Pfad (Umgeht visuelle Firewall)
                # Wir simulieren den Request, den das Frontend für die Kurs-Box nutzt
                url = f"https://www.ls-x.de/_rpc/json/.lstk.getQuote?isin={isin}"
                headers = {
                    'User-Agent': random.choice(self.agents),
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': f'https://www.ls-x.de/de/aktie/{isin}'
                }
                
                res = session.get(url, timeout=7, headers=headers)
                
                if res.status_code == 200:
                    data = res.json()
                    # Pfad im JSON: price oder last_price
                    price = data.get('price') or data.get('last')
                    
                    if price:
                        self._save_atomic(isin, float(price), worker_id)
                        self.success_count += 1
                    else:
                        self.log(f"W{worker_id}: {isin} - JSON enthält keinen Preis (Block?)")
                else:
                    self.log(f"W{worker_id}: {isin} - HTTP {res.status_code}")
                    
            except Exception as e:
                self.log(f"W{worker_id}: Fehler bei {isin} -> {e}")
            finally:
                self.task_queue.task_done()

    def _save_atomic(self, isin, price, worker_id):
        # Jeder Preis über 0.1% ist ein Ankerpunkt
        df = pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin, 'Price': round(price, 4),
            'Source': f'W{worker_id}_V138', 'Anchor_Event': 'TRUE'
        }])
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        universe = self.get_universe()
        for isin in universe: self.task_queue.put(isin)
        
        self.log(f"STARTE V138 LITE | {len(universe)} ISINs | 5 Threads")
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(5): executor.submit(self.worker_process, i)
        
        self.log(f"LAUF BEENDET. Treffer: {self.success_count}")

if __name__ == "__main__":
    AureumSentinel().run()
