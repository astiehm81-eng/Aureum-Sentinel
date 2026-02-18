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
        # SCHARF GESCHALTET: 15 Minuten Laufzeit für stabile Erfassung
        self.runtime_limit = 900 
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cache-Control': 'no-cache'
        })
        
    def get_tradegate_discovery(self):
        """ Discovery: Tradegate liefert das globale Universum (Asien/USA/Europa) """
        urls = [
            "https://www.tradegate.de/index.php", 
            "https://www.tradegate.de/kurslisten.php?die=aktien"
        ]
        isins = set()
        for url in urls:
            try:
                res = self.session.get(url, timeout=10)
                # Extraktion aller ISINs direkt aus dem Tradegate-Orderbook-Stream
                found = re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)
                isins.update(found)
            except: continue
        return list(isins)

    def fetch_ls_live_price(self, isin):
        """ Execution: Hard-Refresh von L&S (Kein Yahoo, kein Tradegate-Preis) """
        url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        try:
            # Hard Refresh erzwingen (Cache löschen)
            response = self.session.get(url, timeout=3, headers={'Pragma': 'no-cache'})
            if response.status_code == 200:
                price = response.json().get('last', {}).get('price')
                return float(price) if price else None
        except: return None

    def run_monitoring(self):
        start_time = time.time()
        # 1. Globale ISINs von Tradegate finden
        universe = self.get_tradegate_discovery()
        
        # 2. CSV Header prüfen/erstellen (Rein für Live-Daten)
        if not os.path.exists(self.csv_path):
            pd.DataFrame(columns=['Timestamp', 'ISIN', 'Price', 'Source', 'Anchor_Event']).to_csv(self.csv_path, index=False)

        # Start-Eintrag für diesen Lauf
        print(f"[{datetime.now()}] Sentinel V116 scharf geschaltet. Scanne {len(universe)} Assets.")

        while time.time() - start_time < self.runtime_limit:
            cycle_start = time.time()
            found_anchors = 0
            
            for isin in universe:
                price = self.fetch_ls_live_price(isin)
                if price:
                    # Eiserner Standard: Jede Bewegung > 0,1% ist ein neuer Ankerpunkt
                    if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                        self.anchors[isin] = price
                        
                        # Sofortiges Schreiben (Atomic Write)
                        new_data = pd.DataFrame([{
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'ISIN': isin,
                            'Price': round(price, 4),
                            'Source': 'L&S_LIVE_REFRESH',
                            'Anchor_Event': 'TRUE'
                        }])
                        new_data.to_csv(self.csv_path, mode='a', header=False, index=False)
                        found_anchors += 1
            
            # Taktung: Einlesen alle 60 Sekunden (Eiserner Takt)
            elapsed = time.time() - cycle_start
            wait_time = max(1, 60 - elapsed)
            time.sleep(wait_time)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
