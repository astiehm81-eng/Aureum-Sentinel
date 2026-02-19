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
        self.runtime_limit = 60 # Test-Fokus: 1 Minute
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        self._clean_legacy_data()

    def _clean_legacy_data(self):
        """ Entfernt Yahoo-Müll zuverlässig """
        if os.path.exists(self.csv_path):
            try:
                df = pd.read_csv(self.csv_path)
                df[df['Source'] != 'YAHOO_BACKFILL'].to_csv(self.csv_path, index=False)
            except: pass

    def get_massive_input(self):
        """ Findet mehr als nur 38 Assets durch Sektor-Scans """
        isins = set()
        # Wir scannen die Startseite UND die aktivsten US/Asien Sektoren
        targets = [
            "https://www.tradegate.de/index.php",
            "https://www.tradegate.de/ausfuehrungen.php?index=US_TEC",
            "https://www.tradegate.de/ausfuehrungen.php?index=DAX"
        ]
        for url in targets:
            try:
                res = self.session.get(url, timeout=5)
                found = re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)
                isins.update(found)
            except: continue
        return list(isins)

    def fetch_via_search(self, isin):
        """ Deine Such-Logik (L&S Simulation) """
        search_url = f"https://www.ls-x.de/de/aktie/{isin}"
        api_url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        try:
            # Menschliche Verzögerung
            time.sleep(random.uniform(0.3, 0.8))
            self.session.get(search_url, timeout=3, headers={'Referer': 'https://www.ls-x.de/'})
            res = self.session.get(api_url, timeout=3)
            return float(res.json().get('last', {}).get('price')) if res.status_code == 200 else None
        except: return None

    def run_monitoring(self):
        start_time = time.time()
        universe = self.get_massive_input()
        
        # Start-Eintrag
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'V119_DEEP_SCAN',
            'Price': len(universe),
            'Source': 'L&S_SEARCH',
            'Anchor_Event': 'START'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        # Shuffle für unvorhersehbares Muster
        random.shuffle(universe)

        for isin in universe:
            if time.time() - start_time > self.runtime_limit: break
            
            price = self.fetch_via_search(isin)
            if price:
                # 0,1% Regel ohne Noise-Filter
                if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                    self.anchors[isin] = price
                    pd.DataFrame([{
                        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'ISIN': isin,
                        'Price': round(price, 4),
                        'Source': 'L&S_LIVE',
                        'Anchor_Event': 'TRUE'
                    }]).to_csv(self.csv_path, mode='a', header=False, index=False)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
