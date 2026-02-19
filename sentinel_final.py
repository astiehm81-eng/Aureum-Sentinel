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
        # Verlängerte Testzeit auf 120s für tiefere Suche
        self.runtime_limit = 120 
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        self._clean_legacy_data()

    def _clean_legacy_data(self):
        """ Entfernt Yahoo-Backfills beim Start """
        if os.path.exists(self.csv_path):
            try:
                df = pd.read_csv(self.csv_path)
                df[df['Source'] != 'YAHOO_BACKFILL'].to_csv(self.csv_path, index=False)
            except: pass

    def get_massive_universe(self):
        """ Discovery über Tradegate-Sektoren (DAX, Tech, US) """
        all_isins = set()
        # Wir nehmen mehrere Listen, um die 38er-Grenze zu sprengen
        targets = [
            "https://www.tradegate.de/index.php",
            "https://www.tradegate.de/ausfuehrungen.php?index=DAX",
            "https://www.tradegate.de/ausfuehrungen.php?index=US_TEC"
        ]
        for url in targets:
            try:
                res = self.session.get(url, timeout=10)
                all_isins.update(re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text))
            except: continue
        return list(all_isins)

    def fetch_via_ls_search(self, isin):
        """ 
        STABILE LOGIK: Suche über L&S Webseite, nicht über API.
        Simuliert ISIN-Eingabe und Auslesen der Zielseite.
        """
        search_url = f"https://www.ls-x.de/de/aktie/{isin}"
        try:
            # Menschliche Pause vor der "Suche"
            time.sleep(random.uniform(1.5, 3.0))
            
            # Hard Refresh der Webseite
            response = self.session.get(search_url, timeout=10, headers={'Referer': 'https://www.ls-x.de/'})
            
            if response.status_code == 200:
                # Regex Suche nach dem Preis im HTML-Body (Eiserner Standard Extraktion)
                # Wir suchen nach dem Muster: "price":175.45 oder ähnlichen Daten-Tags im HTML
                price_match = re.search(r'last":([\d.]+)', response.text)
                if not price_match:
                    # Alternativer Matcher für HTML-Darstellung
                    price_match = re.search(r'price-value">([\d,.]+)', response.text)
                
                if price_match:
                    price_str = price_match.group(1).replace(',', '.')
                    return float(price_str)
        except:
            return None

    def run_monitoring(self):
        start_time = time.time()
        universe = self.get_massive_universe()
        
        # Start-Log
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'V124_IRON_SEARCH',
            'Price': len(universe),
            'Source': 'LS_WEB_SEARCH',
            'Anchor_Event': 'START'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        random.shuffle(universe)

        for isin in universe:
            if time.time() - start_time > self.runtime_limit:
                break
                
            price = self.fetch_via_ls_search(isin)
            if price and price > 0.1:
                if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                    self.anchors[isin] = price
                    
                    # Atomic Writing
                    pd.DataFrame([{
                        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'ISIN': isin,
                        'Price': round(price, 4),
                        'Source': 'L&S_WEB_LIVE',
                        'Anchor_Event': 'TRUE'
                    }]).to_csv(self.csv_path, mode='a', header=False, index=False)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
