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
        # TEST-MODUS: Nur 60 Sekunden Laufzeit
        self.runtime_limit = 60 
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        # Bereinigt Yahoo-Leichen sofort beim Start
        self._clean_legacy_data()

    def _clean_legacy_data(self):
        if os.path.exists(self.csv_path):
            try:
                df = pd.read_csv(self.csv_path)
                df[df['Source'] != 'YAHOO_BACKFILL'].to_csv(self.csv_path, index=False)
                print(f"[{datetime.now()}] CSV von Yahoo-Daten bereinigt.")
            except: pass

    def get_input_from_tradegate(self):
        """ Holt frische ISINs von der Tradegate-Startseite """
        try:
            res = self.session.get("https://www.tradegate.de/index.php", timeout=10)
            return list(set(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)))
        except: return []

    def fetch_via_search_simulation(self, isin):
        """ Simuliert die Suche auf L&S """
        search_url = f"https://www.ls-x.de/de/aktie/{isin}" 
        api_url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        try:
            # Kurze menschliche Pause (0.5 - 1.5s für den Test)
            time.sleep(random.uniform(0.5, 1.5))
            self.session.get(search_url, timeout=5, headers={'Referer': 'https://www.ls-x.de/'})
            response = self.session.get(api_url, timeout=3)
            if response.status_code == 200:
                return float(response.json().get('last', {}).get('price'))
        except: return None

    def run_monitoring(self):
        start_time = time.time()
        universe = self.get_input_from_tradegate()
        
        # Sofortiger Start-Eintrag
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'TEST_RUN_V118',
            'Price': len(universe),
            'Source': 'L&S_SEARCH',
            'Anchor_Event': 'START'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        print(f"[{datetime.now()}] Testlauf gestartet (60s)...")

        for isin in universe:
            # Zeitcheck: Wenn 60s um sind, abbrechen
            if time.time() - start_time > self.runtime_limit:
                print(f"[{datetime.now()}] Zeitlimit erreicht. Beende Test.")
                break
                
            price = self.fetch_via_search_simulation(isin)
            if price:
                # Eiserner Standard: 0,1% Regel
                if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                    self.anchors[isin] = price
                    
                    # SOFORTIGES SCHREIBEN: Jede Änderung wird direkt in die Datei gespült
                    new_row = pd.DataFrame([{
                        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'ISIN': isin,
                        'Price': round(price, 4),
                        'Source': 'L&S_LIVE',
                        'Anchor_Event': 'TRUE'
                    }])
                    new_row.to_csv(self.csv_path, mode='a', header=False, index=False)
                    print(f"Anker gesetzt: {isin} @ {price}")

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
