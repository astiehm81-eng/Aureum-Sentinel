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
        # TEST-MODUS: Nur 120 Sekunden (2 Minuten) Laufzeit
        self.runtime_limit = 120 
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cache-Control': 'no-cache'
        })
        
    def get_tradegate_isins(self):
        """ Discovery via Tradegate (Eiserner Standard für Breitband) """
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
        """ Einlesen: Hard Refresh L&S (V42 Strategie) """
        url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        try:
            # Schnelles Einlesen ohne künstliche Verzögerung im Test
            response = self.session.get(url, timeout=3)
            if response.status_code == 200:
                price = response.json().get('last', {}).get('price')
                return float(price) if price else None
        except: return None

    def run_monitoring(self):
        start_time = time.time()
        # Einmaliges Discovery
        universe = self.get_tradegate_isins()
        
        # Start-Log
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'SENTINEL_FAST_TEST',
            'Price': len(universe),
            'Source': 'L&S_2MIN_MODE',
            'Anchor_Event': 'START'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        while time.time() - start_time < self.runtime_limit:
            cycle_start = time.time()
            
            for isin in universe:
                price = self.fetch_ls_price(isin)
                if price:
                    # 0,1% Anker-Check
                    if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                        self.anchors[isin] = price
                        pd.DataFrame([{
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'ISIN': isin,
                            'Price': round(price, 4),
                            'Source': 'L&S_LIVE',
                            'Anchor_Event': 'TRUE'
                        }]).to_csv(self.csv_path, mode='a', header=False, index=False)
            
            # Im 2-Minuten-Test takten wir etwas schneller (30s) für mehr Daten
            elapsed = time.time() - cycle_start
            wait_time = max(1, 30 - elapsed) 
            time.sleep(wait_time)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
