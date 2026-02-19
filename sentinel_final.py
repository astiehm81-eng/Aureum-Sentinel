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
        self.runtime_limit = 60  # Test-Modus: 1 Minute
        self.session = requests.Session()
        # Eiserner Standard: Browser-Identität für Hard-Refresh
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        self._purge_to_iron_standard()

    def _purge_to_iron_standard(self):
        """ Bereinigt die CSV radikal von Yahoo und fehlerhaften Testdaten """
        if os.path.exists(self.csv_path):
            try:
                df = pd.read_csv(self.csv_path)
                # Behalte nur echte Live-Daten, entferne Yahoo und die 90€-Mock-Werte
                df = df[(df['Source'] != 'YAHOO_BACKFILL') & (df['Price'] > 1.0)]
                df.to_csv(self.csv_path, index=False)
            except: pass

    def get_iron_universe(self):
        """ Discovery direkt vom Tradegate Orderbook (Live-Sektoren) """
        isins = set()
        # Wir nehmen die aktivsten Märkte für den Test
        urls = [
            "https://www.tradegate.de/index.php",
            "https://www.tradegate.de/ausfuehrungen.php?index=DAX",
            "https://www.tradegate.de/ausfuehrungen.php?index=US_TEC"
        ]
        for url in urls:
            try:
                res = self.session.get(url, timeout=5)
                isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
            except: continue
        return list(isins)

    def fetch_iron_price(self, isin):
        """ 
        Der Kern des Eisernen Standards: Hard-Refresh von L&S.
        Keine Simulation, kein Buffer.
        """
        api_url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        try:
            # Menschlicher Jitter für Bot-Schutz
            time.sleep(random.uniform(0.3, 0.7))
            # Hard-Refresh durch Cache-Control Header in der Session bereits gesetzt
            res = self.session.get(api_url, timeout=3)
            if res.status_code == 200:
                price_data = res.json().get('last', {}).get('price')
                return float(price_data) if price_data else None
        except: return None

    def run_monitoring(self):
        start_time = time.time()
        universe = self.get_iron_universe()
        
        # Log: Start des Eisernen Standards V121
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'EISERNER_STANDARD_V121',
            'Price': len(universe),
            'Source': 'TR_LIVE_REFRESH',
            'Anchor_Event': 'START'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        random.shuffle(universe)

        for isin in universe:
            if time.time() - start_time > self.runtime_limit:
                break
            
            price = self.fetch_iron_price(isin)
            if price:
                # Regel: Bewegung > 0,1% = Neuer Ankerpunkt (Noise-Filter deaktiviert)
                if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                    self.anchors[isin] = price
                    
                    # Sofortiges Atomic-Writing in die CSV
                    pd.DataFrame([{
                        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'ISIN': isin,
                        'Price': round(price, 4),
                        'Source': 'L&S_LIVE',
                        'Anchor_Event': 'TRUE'
                    }]).to_csv(self.csv_path, mode='a', header=False, index=False)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
