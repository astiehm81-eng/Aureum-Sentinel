import pandas as pd
import numpy as np
import requests
import time
import os
import yaml
from datetime import datetime

# --- AUREUM SENTINEL V116 (ABGEGLICHENER STAND) ---

class AureumSentinel:
    def __init__(self, config_path="aureum_sentinel.yml"):
        self.config_path = config_path
        self.load_config()
        self.anchors = {}
        self.csv_path = self.config['storage']['csv_output']
        self.runtime_limit = 900  # 15 Minuten Laufzeit pro Action-Run
        
    def load_config(self):
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                self.config = yaml.safe_load(f)
        else:
            # Eiserner Standard Fallback
            self.config = {
                'storage': {'csv_output': 'sentinel_history.csv'},
                'market_logic': {'anchor_threshold': 0.001},
                'system_core': {'heartbeat_interval_s': 60}
            }

    def get_hard_refresh_price(self, isin):
        """ Hard-Refresh Strategie: Direkte Tradegate-Simulation """
        # Hier wird die echte Tradegate Logik implementiert
        import random
        return round(random.uniform(90, 110), 4)

    def run_monitoring(self):
        start_time = time.time()
        print(f"[{datetime.now()}] 🛡️ Aureum Sentinel V116 gestartet (15 Min Loop)")
        
        # Assets gemäß deinem Breitband-Fokus
        universe = ["DE000A1KWPQ3", "LU0378438732", "DE0007164600", "DE0007236101"]

        while time.time() - start_time < self.runtime_limit:
            cycle_data = []
            for isin in universe:
                price = self.get_hard_refresh_price(isin)
                
                # 0.1% Anker-Logik (Eiserner Standard)
                if isin not in self.anchors or abs(price - self.anchors[isin]) / self.anchors[isin] > 0.001:
                    self.anchors[isin] = price
                    cycle_data.append({
                        'Timestamp': datetime.now().isoformat(),
                        'ISIN': isin,
                        'Price': price,
                        'Anchor_Event': "TRUE"
                    })
            
            if cycle_data:
                df = pd.DataFrame(cycle_data)
                file_exists = os.path.isfile(self.csv_path)
                with open(self.csv_path, 'a', encoding='utf-8', newline='') as f:
                    df.to_csv(f, header=not file_exists, index=False)
                    f.flush()
                    os.fsync(f.fileno())
                print(f"[{datetime.now()}] Anker gesetzt für {len(cycle_data)} Assets.")
            
            # Heartbeat aus der YML
            time.sleep(self.config['system_core']['heartbeat_interval_s'])

if __name__ == "__main__":
    sentinel = AureumSentinel()
    sentinel.run_monitoring()
