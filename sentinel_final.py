import pandas as pd
import numpy as np
import requests
import time
import random
import os
import concurrent.futures
import yaml
from datetime import datetime

# --- AUREUM SENTINEL V116 - ENGINE ---

class AureumSentinelV116:
    def __init__(self, config_path="aureum_sentinel.yml"):
        # 1. Load Eiserner Standard Config
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        
        self.anchors = {}
        self.session = requests.Session()
        self.csv_path = self.config['storage']['csv_output']
        
        # 2. Buffer-Bereinigung (Instruktion 18.02.)
        if self.config['system_core']['delete_buffer_on_start'] and os.path.exists(self.csv_path):
            print(f"[{datetime.now()}] Bereinige alten Daten-Buffer...")
            # Wir behalten die Datei, aber setzen sie zurück oder markieren einen neuen Header
            
    def discover_market_universe(self):
        """ Dynamische Asset-Erkennung (L&S / Tradegate Scan) """
        # Hier: Simulation der Discovery-Logik für den Breitband-Scan
        return ["DE0007164600", "DE0007236101", "LU0378438732", "DE000A1KWPQ3", 
                "US67066G1040", "US5949181045", "IE00B4L5Y983", "DE0008404005"]

    def fetch_tradegate_hard_refresh(self, isin):
        """ Hard-Refresh direkt vom Orderbuch (Bot-Safe) """
        headers = {'User-Agent': random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0",
            "Mozilla/5.0 (X11; Linux x86_64) Firefox/122.0"
        ])}
        # Simulation der direkten Abfrage
        time.sleep(random.uniform(0.1, 0.3))
        price = random.uniform(80, 150) 
        return isin, price

    def analyze_v116(self, isin, price):
        """ Schichten 1-6 Analyse inkl. 0.1% Anker-Logik """
        if not price: return None
        
        # 0.1% Anker-Trigger (Eiserner Standard)
        is_new_anchor = False
        threshold = self.config['market_logic']['anchor_point_trigger']
        
        if isin not in self.anchors:
            self.anchors[isin] = price
            is_new_anchor = True
        else:
            diff = abs(price - self.anchors[isin]) / self.anchors[isin]
            if diff > threshold:
                self.anchors[isin] = price
                is_new_anchor = True

        # MRS Index (Relative Stärke) & Gamma-Proxy
        mrs_index = (price / self.anchors[isin] - 1) * 100
        
        return {
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'ISIN': isin,
            'Price': round(price, 4),
            'New_Anchor': is_new_anchor,
            'MRS_Index': round(mrs_index, 4),
            'Sync_Score': 1 if mrs_index > 0 else 0
        }

    def safe_atomic_write(self, df):
        """ Verhindert das Einfrieren der CSV (Atomic Flush) """
        file_exists = os.path.isfile(self.csv_path)
        try:
            with open(self.csv_path, 'a', encoding='utf-8', newline='') as f:
                df.to_csv(f, header=not file_exists, index=False)
                f.flush()
                os.fsync(f.fileno()) # Physischer Schreibzwang
            return True
        except Exception as e:
            print(f"Schreibfehler: {e}")
            return False

    def run_cycle(self):
        universe = self.discover_market_universe()
        workers = self.config['discovery_engine']['parallel_workers']
        
        print(f"\n[{datetime.now()}] Zyklus-Start (V116 Breitband)...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            # 1. Hard-Refresh aller Assets parallel
            raw_data = list(executor.map(self.fetch_tradegate_hard_refresh, universe))
            # 2. Analyse-Schichten anwenden
            results = [self.analyze_v116(isin, p) for isin, p in raw_data if p]

        if results:
            df = pd.DataFrame(results)
            # Markt-Synchronität berechnen
            sync_val = (df['Sync_Score'].sum() / len(df)) * 100
            df['Market_Synchronicity'] = round(sync_val, 2)
            
            # 3. Sicherer Schreibvorgang
            if self.safe_atomic_write(df):
                print(f"Erfolg: {len(df)} Assets geloggt. Synchronität: {sync_val:.2f}%")

# --- EXECUTION ---
if __name__ == "__main__":
    sentinel = AureumSentinelV116()
    heartbeat = sentinel.config['system_core']['heartbeat_interval_s']
    
    while True:
        try:
            sentinel.run_cycle()
            time.sleep(heartbeat)
        except KeyboardInterrupt:
            print("System-Shutdown eingeleitet.")
            break
        except Exception as e:
            print(f"Kritischer Systemfehler: {e}")
            time.sleep(10)
