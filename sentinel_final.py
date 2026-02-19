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
        # Hochwertige Browser-Simulation
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        self._clean_legacy_data()

    def _clean_legacy_data(self):
        """ Entfernt YAHOO_BACKFILL automatisch beim Start """
        if os.path.exists(self.csv_path):
            try:
                df = pd.read_csv(self.csv_path)
                df[df['Source'] != 'YAHOO_BACKFILL'].to_csv(self.csv_path, index=False)
            except: pass

    def get_input_from_tradegate(self):
        """ Holt die 'Suchbegriffe' (ISINs) von Tradegate """
        urls = ["https://www.tradegate.de/index.php", "https://www.tradegate.de/kurslisten.php?die=aktien"]
        isins = set()
        for url in urls:
            try:
                res = self.session.get(url, timeout=10)
                isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
            except: continue
        return list(isins)

    def fetch_via_search_simulation(self, isin):
        """ 
        Simuliert: ISIN in Suche eingeben -> Enter -> Detailseite lesen 
        """
        # Schritt 1: Simulation der Suchanfrage (Referer setzen)
        search_url = f"https://www.ls-x.de/de/aktie/{isin}" 
        api_url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        
        try:
            # Menschliche Pause vor der "Suche"
            time.sleep(random.uniform(1.2, 3.5))
            
            # Wir "besuchen" die Detailseite (simuliert)
            self.session.get(search_url, timeout=5, headers={'Referer': 'https://www.ls-x.de/'})
            
            # Jetzt ziehen wir den Preis (wie der Browser im Hintergrund)
            response = self.session.get(api_url, timeout=3, headers={'Referer': search_url})
            
            if response.status_code == 200:
                return float(response.json().get('last', {}).get('price'))
        except:
            return None

    def run_monitoring(self):
        start_time = time.time()
        # Wir holen uns die Ziele
        universe = self.get_input_from_tradegate()
        
        # Start-Log ohne Yahoo-Müll
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'SEARCH_MODE_ACTIVE',
            'Price': len(universe),
            'Source': 'L&S_SEARCH_SIM',
            'Anchor_Event': 'START'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        while time.time() - start_time < self.runtime_limit:
            cycle_start = time.time()
            # Mische das Universum, damit die Abfrage-Reihenfolge nicht maschinell wirkt
            random.shuffle(universe)
            
            for isin in universe[:50]: # Wir konzentrieren uns auf 50 Assets pro Minute für maximale Qualität
                price = self.fetch_via_search_simulation(isin)
                if price:
                    # 0,1% Anker-Regel
                    if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                        self.anchors[isin] = price
                        pd.DataFrame([{
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'ISIN': isin,
                            'Price': round(price, 4),
                            'Source': 'L&S_SEARCH_LIVE',
                            'Anchor_Event': 'TRUE'
                        }]).to_csv(self.csv_path, mode='a', header=False, index=False)
            
            # Taktung einhalten
            wait = max(5, 60 - (time.time() - cycle_start))
            time.sleep(wait)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
