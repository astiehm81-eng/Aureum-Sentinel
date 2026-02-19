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
        self.runtime_limit = 110  # Sicherheitslimit für GitHub Actions (120s max)
        self.start_time = time.time()
        self.task_queue = Queue()
        
        # Eiserner Standard: Authentische User-Agents zur Browser-Simulation
        self.agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0'
        ]

    def get_expanded_universe(self):
        """ 
        Discovery-Agent: Sammelt ISINs von Tradegate. 
        Erweiterbar auf 10.000 ISINs durch zusätzliche Sektor-URLs.
        """
        isins = set()
        targets = [
            "https://www.tradegate.de/index.php",
            "https://www.tradegate.de/ausfuehrungen.php?index=DAX",
            "https://www.tradegate.de/ausfuehrungen.php?index=MDAX",
            "https://www.tradegate.de/ausfuehrungen.php?index=SDAX",
            "https://www.tradegate.de/ausfuehrungen.php?index=EURO_STOXX_50"
        ]
        for url in targets:
            try:
                res = requests.get(url, timeout=5)
                # Extrahiert ISIN-Muster (2 Buchstaben + 10 Zeichen)
                isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
            except:
                continue
        return list(isins)

    def worker_process(self, worker_id):
        """ 
        Kern-Logik: Staggered Start & Deep-Masking.
        Verhindert die Erkennung durch L&S Firewall.
        """
        # STAGGERED START: Auch Worker 0 wartet mind. 5ms + Micro-Jitter
        initial_wait = (worker_id * 0.5) + 0.005 + random.uniform(0.005, 0.015)
        time.sleep(initial_wait)
        
        while not self.task_queue.empty():
            if time.time() - self.start_time > self.runtime_limit:
                break
                
            isin = self.task_queue.get()
            
            # Individueller Jitter zwischen den Assets (Eiserner Standard)
            time.sleep(random.uniform(0.15, 0.45))
            
            try:
                # Deep-Masking: Neue Session pro Anfrage simuliert Tab-Wechsel
                with requests.Session() as s:
                    s.headers.update({
                        'User-Agent': random.choice(self.agents),
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
                        'Referer': 'https://www.ls-x.de/de/suche',
                        'Connection': 'close'
                    })
                    
                    # Hard Refresh direkt von der Quelle
                    res = s.get(f"https://www.ls-x.de/de/aktie/{isin}", timeout=10)
                    
                    if res.status_code == 200:
                        # Regex-Suche (Web-Tag oder JSON-Backfill)
                        match = re.search(r'price-value">([\d,.]+)', res.text) or \
                                re.search(r'last&quot;:([\d.]+)', res.text)
                        
                        if match:
                            price_str = match.group(1).replace('.', '').replace(',', '.')
                            price = float(price_str)
                            self._save_atomic(isin, price, worker_id)
                        else:
                            print(f"W{worker_id}: {isin} - Preis versteckt (Firewall-Block).")
            except Exception as e:
                pass # Fehlerhafte Anfragen werden übersprungen
            finally:
                self.task_queue.task_done()

    def _save_atomic(self, isin, price, worker_id):
        """ 
        Schreibt jeden Preis-Event sofort in die CSV.
        Filter für statistisches Rauschen ist deaktiviert.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = [{
            'Timestamp': timestamp,
            'ISIN': isin,
            'Price': round(price, 4),
            'Source': f'W{worker_id}_V133_IRON',
            'Anchor_Event': 'TRUE' # Jeder Treffer ist ein potenzieller neuer Ankerpunkt
        }]
        
        # Sofortiges Speichern (Atomic Write)
        df = pd.DataFrame(data)
        df.to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

    def run(self):
        universe = self.get_expanded_universe()
        for isin in universe:
            self.task_queue.put(isin)
            
        print(f"--- AUREUM SENTINEL V133 GESTARTET ---")
        print(f"Universum: {len(universe)} ISINs | Modus: Staggered Iron (5 Threads)")
        
        # Nutzung von 5 Threads für optimale Lastverteilung ohne Firewall-Trigger
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(5):
                executor.submit(self.worker_process, i)
        
        print(f"--- LAUF BEENDET ---")

if __name__ == "__main__":
    # Initialisierung des Sentinel
    sentinel = AureumSentinel()
    sentinel.run()
