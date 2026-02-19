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
        self.session = requests.Session()
        # Eiserner Standard Header
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0',
            'Accept': 'text/html,application/xhtml+xml',
            'Referer': 'https://www.ls-x.de/'
        })

    def get_massive_universe(self):
        """ Discovery über alle Tradegate-Sektoren für maximale Last """
        isins = set()
        targets = [
            "https://www.tradegate.de/index.php",
            "https://www.tradegate.de/ausfuehrungen.php?index=DAX",
            "https://www.tradegate.de/ausfuehrungen.php?index=US_TEC",
            "https://www.tradegate.de/ausfuehrungen.php?index=MDAX",
            "https://www.tradegate.de/ausfuehrungen.php?index=SDAX"
        ]
        for url in targets:
            try:
                res = self.session.get(url, timeout=5)
                isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
            except: continue
        return list(isins)

    def worker_process(self, worker_id):
        """ 10 Worker im 1-10ms Jitter Test-Modus """
        while not self.task_queue.empty():
            if time.time() - self.start_time > self.runtime_limit:
                break
                
            isin = self.task_queue.get()
            
            # EXTREM-JITTER TEST: 1ms bis 10ms
            # Das entspricht quasi einem synchronen Angriff der Worker
            time.sleep(random.uniform(0.001, 0.010))
            
            try:
                url = f"https://www.ls-x.de/de/aktie/{isin}"
                res = self.session.get(url, timeout=5)
                
                if res.status_code == 200:
                    # Suche nach Preis im HTML
                    match = re.search(r'price-value">([\d,.]+)', res.text)
                    if match:
                        price = float(match.group(1).replace('.', '').replace(',', '.'))
                        self._save_atomic(isin, price, worker_id)
                    else:
                        # Logge leere Treffer als potenziellen Bot-Block
                        print(f"Worker_{worker_id}: ISIN {isin} - Preis-Tag fehlt (Block-Gefahr?)")
                elif res.status_code == 403:
                    print(f"Worker_{worker_id}: 403 Forbidden! Jitter zu niedrig.")
            except:
                pass
            finally:
                self.task_queue.task_done()

    def _save_atomic(self, isin, price, worker_id):
        """ Sofortiges Schreiben des Ankerpunkts """
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin,
            'Price': round(price, 4),
            'Source': f'W{worker_id}_J10ms',
            'Anchor_Event': 'TRUE'
        }]).to_csv(self.csv_path, mode='a', header=False, index=False)

    def run_nitro_test(self):
        universe = self.get_massive_universe()
        for isin in universe:
            self.task_queue.put(isin)
            
        print(f"Starte Nitro-Test: {len(universe)} ISINs | 10 Worker | 1-10ms Jitter")

        with ThreadPoolExecutor(max_workers=10) as executor:
            for i in range(10):
                executor.submit(self.worker_process, i)

if __name__ == "__main__":
    AureumSentinel().run_nitro_test()
