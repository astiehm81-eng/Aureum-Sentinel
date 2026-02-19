import pandas as pd
import requests
import time
import os
import random
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class AureumSentinel:
    def __init__(self):
        self.anchors = {}
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 90 
        self.start_time = time.time()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0',
            'Accept': 'text/html,application/xhtml+xml',
            'Referer': 'https://www.ls-x.de/'
        })
        self._clean_legacy_data()

    def _clean_legacy_data(self):
        if os.path.exists(self.csv_path):
            try:
                df = pd.read_csv(self.csv_path)
                df[df['Source'] != 'YAHOO_BACKFILL'].to_csv(self.csv_path, index=False)
            except: pass

    def get_market_universe(self):
        """ Discovery über Tradegate-Sektoren """
        isins = set()
        for url in ["https://www.tradegate.de/index.php", "https://www.tradegate.de/ausfuehrungen.php?index=DAX"]:
            try:
                res = self.session.get(url, timeout=5)
                isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
            except: continue
        return list(isins)

    def fetch_worker_task(self, isin):
        """ Ein einzelner Worker (Tab) mit Micro-Jitter """
        if time.time() - self.start_time > self.runtime_limit:
            return

        # Der entscheidende Micro-Jitter (Millisekunden-Bereich)
        time.sleep(random.uniform(0.2, 0.8))
        
        url = f"https://www.ls-x.de/de/aktie/{isin}"
        try:
            res = self.session.get(url, timeout=5)
            if res.status_code == 200:
                match = re.search(r'price-value">([\d,.]+)', res.text)
                if match:
                    price = float(match.group(1).replace('.', '').replace(',', '.'))
                    self._process_price(isin, price)
        except: pass

    def _process_price(self, isin, price):
        """ Prüft 0,1% Regel und schreibt sofort """
        if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
            self.anchors[isin] = price
            pd.DataFrame([{
                'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'ISIN': isin,
                'Price': round(price, 4),
                'Source': 'L&S_MULTI_WORKER',
                'Anchor_Event': 'TRUE'
            }]).to_csv(self.csv_path, mode='a', header=False, index=False)

    def run_monitoring(self):
        universe = self.get_market_universe()
        random.shuffle(universe)
        
        # Start-Eintrag
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'V127_MULTI_TAB_MODE',
            'Price': len(universe),
            'Source': 'SYSTEM',
            'Anchor_Event': 'INIT'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        # 3 Parallele Worker simulieren die Tabs
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(self.fetch_worker_task, universe)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
