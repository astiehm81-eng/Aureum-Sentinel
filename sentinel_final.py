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
        
    def scrape_active_isins(self):
        """ 
        Scraping-Strategie: Findet ISINs auf L&S/TR Übersichtsseiten.
        Sucht nach Mustern für asiatische und US-Aktien.
        """
        discovered = []
        # Wir scrapen die Hauptübersichten für 'Aktien' und 'Neuheiten'
        urls = [
            "https://www.ls-tc.de/de/aktien",
            "https://www.ls-tc.de/de/kryptowaehrungen"
        ]
        for url in urls:
            try:
                res = self.session.get(url, timeout=5)
                # Extraktion von ISIN-Mustern via Regex (Eiserner Standard Weg)
                isins = re.findall(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', res.text)
                discovered.extend(isins)
            except:
                pass
        return list(set(discovered)) # Duplikate entfernen

    def fetch_ls_hard_refresh(self, isin):
        """ Einlesen wie im stabilen Stand: Direkt & Ungefiltert """
        url = f"https://ls-api.traderepublic.com/v1/quotes/{isin}"
        try:
            time.sleep(random.uniform(0.1, 0.3)) # Anti-Bot Jitter
            response = self.session.get(url, timeout=3)
            if response.status_code == 200:
                data = response.json()
                # Einlesen des Last-Preises ohne Rauschfilter
                return float(data.get('last', {}).get('price'))
        except:
            return None

    def run_monitoring(self):
        start_time = time.time()
        
        # 1. Scraping-Lauf zur Initialisierung (Breitband-Discovery)
        print(f"[{datetime.now()}] Starte L&S Scraping-Discovery...", flush=True)
        universe = self.scrape_active_isins()
        
        # Fallback auf Kern-Assets (Asien/USA/Crypto)
        universe += ["BTC-EUR", "ETH-EUR", "US0378331005", "JP3435000009"]
        universe = list(set(universe))

        # Heartbeat-Startzeile
        pd.DataFrame([{
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': 'SYSTEM_INIT',
            'Price': len(universe),
            'Source': 'SCRAPE_DISCOVERY_V116',
            'Anchor_Event': 'START'
        }]).to_csv(self.csv_path, mode='a', header=not os.path.exists(self.csv_path), index=False)

        while time.time() - start_time < self.runtime_limit:
            for isin in universe:
                price = self.fetch_ls_hard_refresh(isin)
                if price:
                    # 0,1% ANKER-STRATEGIE (Kein Buffer, direkter Vergleich)
                    if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                        self.anchors[isin] = price
                        pd.DataFrame([{
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'ISIN': isin,
                            'Price': round(price, 4),
                            'Source': 'L&S_HARD_REFRESH',
                            'Anchor_Event': 'TRUE'
                        }]).to_csv(self.csv_path, mode='a', header=False, index=False)
            
            time.sleep(60)

if __name__ == "__main__":
    AureumSentinel().run_monitoring()
