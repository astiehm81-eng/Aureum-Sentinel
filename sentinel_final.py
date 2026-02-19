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
        # Simulation eines echten Browsers (Eiserner Standard)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Connection': 'keep-alive'
        })
        
    def get_massive_universe(self):
        """ Discovery mit 'Page-Reload' Simulation """
        print(f"[{datetime.now()}] Starte Tradegate-Discovery (Simulated Browser)...")
        all_isins = set()
        
        # 1. Erst die Hauptseite laden (Setzt Cookies/Session)
        try:
            self.session.get("https://www.tradegate.de/index.php", timeout=10)
        except: pass

        # 2. Zufällige Buchstaben-Listen abgreifen (Simulation von Klicks)
        alphabet = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        selected_chars = random.sample(alphabet, 6) # 6 Sektoren pro Minute
        
        for char in selected_chars:
            url = f"https://www.tradegate.de/kurslisten.php?die=aktien&buchstabe={char}"
            # Referer setzen, um "natürlichen" Klickpfad vorzutäuschen
            self.session.headers.update({'Referer': 'https://www.tradegate.de/kurslisten.php?die=aktien'})
            
            try:
                res = self.session.get(url, timeout=10)
                found = re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)
                all_isins.update(found)
                # "Menschliche" Pause zwischen den Klicks
                time.sleep(random.uniform(1.5, 3.0))
            except: continue
            
        print(f"[{datetime.now()}] {len(all_isins)} ISINs über Tradegate-Discovery gefunden.")
        return list(all_isins)

    def fetch_ls_price(self, isin):
        """ Hard-Refresh bei L&S (V42 Einlese-Standard) """
        url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        try:
            # Schneller Abruf, aber mit Jitter
            time.sleep(random.uniform(0.05, 0.1))
            response = self.session.get(url, timeout=3, headers={'Referer': 'https://www.traderepublic.com/'})
            if response.status_code == 200:
                return float(response.json().get('last', {}).get('price'))
        except: return None

    def run_monitoring(self):
        start_time = time.time()
        # Initialer Herzschlag
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'SENTINEL_REFRESH_V116',
            'Price': 0,
            'Source': 'TRADEGATE_DISCOVERY',
            'Anchor_Event': 'ALIVE'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        while time.time() - start_time < self.runtime_limit:
            cycle_start = time.time()
            # Jede Minute frische Discovery (neue Buchstaben)
            universe = self.get_massive_universe()
            
            for isin in universe:
                price = self.fetch_ls_price(isin)
                if price:
                    if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                        self.anchors[isin] = price
                        pd.DataFrame([{
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'ISIN': isin,
                            'Price': round(price, 4),
                            'Source': 'L&S_LIVE',
                            'Anchor_Event': 'TRUE'
                        }]).to_csv(self.csv_path, mode='a', header=False, index=False)
            
            # Warten bis zum nächsten 60s-Zyklus
            wait = max(1, 60 - (time.time() - cycle_start))
            time.sleep(wait)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
