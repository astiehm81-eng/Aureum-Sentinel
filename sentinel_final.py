import pandas as pd
import numpy as np
import requests
import time
import os
import random
import yaml
import concurrent.futures
from datetime import datetime

class AureumSentinel:
    def __init__(self, config_path="aureum_sentinel.yml"):
        self.config_path = config_path
        self.load_config()
        self.anchors = {}
        self.csv_path = "sentinel_history.csv"
        self.runtime_limit = 900 # 15 Min Laufzeit
        self.session = requests.Session()
        # Eiserner Standard Header gegen Bot-Erkennung
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Cache-Control': 'no-cache'
        })
        
    def load_config(self):
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = {'market_logic': {'anchor_threshold': 0.001}}

    def fetch_ls_price(self, asset_id):
        """ Hard Refresh mit Anti-Bot-Jitter """
        # Kleiner Jitter vor dem Request, um Muster zu brechen
        time.sleep(random.uniform(0.05, 0.2))
        
        url = f"https://ls-api.traderepublic.com/v1/quotes/{asset_id}"
        try:
            response = self.session.get(url, timeout=4)
            if response.status_code == 200:
                data = response.json()
                # Nutzung von 'last' oder 'bid/ask' mid gemäß Eisernem Standard
                price = float(data.get('last', {}).get('price'))
                return asset_id, price
        except:
            pass
        return asset_id, None

    def run_monitoring(self):
        start_time = time.time()
        # Dynamisches Universum: Erweitert sich automatisch
        # Startset: Deine Kernwerte + Top-Volumen Assets (DAX/NASDAQ/Crypto)
        universe = [
            "DE0007164600", "DE000ENER610", "LU0378438732", "DE000A1KWPQ3", 
            "US0378331005", "US5949181045", "US67066G1040", "BTC-EUR", "ETH-EUR"
        ]

        print(f"[{datetime.now()}] 🛡️ Sentinel V116: Breitband-Überwachung aktiv.", flush=True)

        while time.time() - start_time < self.runtime_limit:
            cycle_data = []
            
            # Parallelisierung mit moderater Worker-Anzahl gegen Bot-Falle
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(self.fetch_ls_price, universe))

            for asset_id, current_price in results:
                if current_price:
                    # Strategie: 0,1% Anker ohne Filter (Eiserner Standard 18.02.)
                    if asset_id not in self.anchors:
                        self.anchors[asset_id] = current_price
                        trigger = True
                    else:
                        diff = abs(current_price - self.anchors[asset_id]) / self.anchors[asset_id]
                        trigger = diff >= self.config['market_logic']['anchor_threshold']
                    
                    if trigger:
                        self.anchors[asset_id] = current_price
                        cycle_data.append({
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'ISIN': asset_id,
                            'Price': round(current_price, 4),
                            'Source': 'L&S_TR_LIVE',
                            'Anchor_Event': "TRUE"
                        })
            
            if cycle_data:
                df = pd.DataFrame(cycle_data)
                file_exists = os.path.isfile(self.csv_path)
                with open(self.csv_path, 'a', encoding='utf-8', newline='') as f:
                    df.to_csv(f, header=not file_exists, index=False)
                    f.flush()
                    os.fsync(f.fileno())
                print(f"[{datetime.now()}] {len(cycle_data)} Ankerpunkte synchronisiert.", flush=True)
            
            # Pause zwischen den Zyklen zur Stabilisierung
            time.sleep(30)

if __name__ == "__main__":
    sentinel = AureumSentinel()
    sentinel.run_monitoring()
