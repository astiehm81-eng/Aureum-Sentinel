import pandas as pd
import requests
import time
import os
import random
import re
from datetime import datetime

class AureumSentinel:
    def __init__(self):
        self.anchors = {}
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 900
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cache-Control': 'no-cache'
        })
        
    def get_tradegate_isins(self):
        """ Holt ISINs von Tradegate als Basis für die Suche """
        urls = [
            "https://www.tradegate.de/index.php",
            "https://www.tradegate.de/kurslisten.php?die=aktien"
        ]
        isins = set()
        for url in urls:
            try:
                res = self.session.get(url, timeout=10)
                found = re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)
                isins.update(found)
            except: continue
        return list(isins)

    def fetch_ls_price(self, isin):
        """ Einlesen wie im stabilen Stand: Hard Refresh L&S """
        url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        try:
            # Nur ein ganz kurzer Jitter für die IP-Stabilität
            time.sleep(0.05) 
            response = self.session.get(url, timeout=3)
            if response.status_code == 200:
                price = response.json().get('last', {}).get('price')
                return float(price) if price else None
        except: return None

    def run_monitoring(self):
        start_time = time.time()
        # Einmaliges Discovery am Anfang
        universe = self.get_tradegate_isins()
        
        # System-Start-Eintrag
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'SENTINEL_START',
            'Price': len(universe),
            'Source': 'L&S_MINUTE_CYCLE',
            'Anchor_Event': 'INIT'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        while time.time() - start_time < self.runtime_limit:
            cycle_start = time.time()
            
            for isin in universe:
                price = self.fetch_ls_price(isin)
                if price:
                    # 0,1% Anker-Logik (Eiserner Standard)
                    if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                        self.anchors[isin] = price
                        pd.DataFrame([{
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'ISIN': isin,
                            'Price': round(price, 4),
                            'Source': 'L&S_LIVE',
                            'Anchor_Event': 'TRUE'
                        }]).to_csv(self.csv_path, mode='a', header=False, index=False)
            
            # Warten bis zum nächsten vollen Minuten-Takt
            elapsed = time.time() - cycle_start
            wait_time = max(1, 60 - elapsed)
            print(f"[{datetime.now()}] Zyklus beendet. Warte {int(wait_time)}s bis zum nächsten Scan.")
            time.sleep(wait_time)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
