import pandas as pd
import numpy as np
import requests
import time
import random
import os
import concurrent.futures
import yaml
from datetime import datetime

# --- AUREUM SENTINEL V116 - ANGEPASST AN DEINE STRUKTUR ---

class AureumSentinelV116:
    def __init__(self, config_path="aureum_sentinel.yml"):
        # Falls die .yml noch nicht existiert, nutzen wir Standardwerte
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                self.config = yaml.safe_load(f)
        else:
            # Fallback-Konfiguration gemäß "Eisernem Standard"
            self.config = {
                'storage': {'csv_output': 'sentinel_history.csv'},
                'market_logic': {'anchor_threshold': 0.001},
                'system_core': {'heartbeat_interval_s': 60, 'delete_buffer_on_start': False},
                'discovery_engine': {'parallel_workers': 10}
            }
        
        self.anchors = {}
        self.session = requests.Session()
        self.csv_path = self.config['storage']['csv_output']

    def fetch_data(self, isin):
        """ Hard-Refresh von Tradegate (Simuliert für Struktur) """
        time.sleep(random.uniform(0.1, 0.3))
        price = random.uniform(90, 110) 
        return isin, price

    def run_cycle(self):
        # Wir nutzen die ISINs aus deinem Breitband-Fokus
        universe = ["DE0007164600", "DE0007236101", "LU0378438732", "DE000A1KWPQ3"]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['discovery_engine']['parallel_workers']) as executor:
            raw_data = list(executor.map(self.fetch_data, universe))
            
        results = []
        for isin, price in raw_data:
            # 0.1% Anker-Logik
            if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                self.anchors[isin] = price
                results.append({
                    'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'ISIN': isin,
                    'Price': round(price, 4),
                    'Anchor_Event': "TRUE"
                })

        if results:
            df = pd.DataFrame(results)
            file_exists = os.path.isfile(self.csv_path)
            with open(self.csv_path, 'a', encoding='utf-8', newline='') as f:
                df.to_csv(f, header=not file_exists, index=False)
                f.flush()
                os.fsync(f.fileno())
            print(f"Update in {self.csv_path} geschrieben.")

if __name__ == "__main__":
    sentinel = AureumSentinelV116()
    # Im GitHub-Workflow lassen wir es meist nur einen Zyklus laufen (oder Zeitbegrenzt)
    sentinel.run_cycle()
