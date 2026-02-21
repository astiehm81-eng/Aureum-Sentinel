import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import time
import random
from datetime import datetime

# --- KONFIGURATION ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage/sentinel_audit.log"
MAX_WORKERS_YAHOO = 40 
STOOQ_LOCK = threading.Lock() 
ANCHOR_THRESHOLD = 0.0005 

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {
            "total_processed": 0,
            "new_isins": 0,
            "gaps_filled": 0,
            "total_anchors": 0,
            "start_time": time.time(),
            "active_pool_size": 0
        }

    def get_storage_info(self):
        """Berechnet die aktuelle Größe der Datenbasis auf der Platte"""
        total_size = 0
        file_count = 0
        for root, dirs, files in os.walk(HERITAGE_ROOT):
            for f in files:
                if f.endswith(".parquet"):
                    total_size += os.path.getsize(os.path.join(root, f))
                    file_count += 1
        return total_size / (1024 * 1024), file_count # MB, Anzahl

    def log_status(self, pool_len):
        """Das große Dashboard-Update für Marktabdeckung"""
        size_mb, files = self.get_storage_info()
        elapsed = (time.time() - self.stats['start_time']) / 60
        coverage = (files / pool_len) * 100 if pool_len > 0 else 0
        
        print(f"\n" + "="*60)
        print(f" AUREUM SENTINEL STATUS | {datetime.now().strftime('%H:%M:%S')}")
        print(f" {'>'*2} Marktabdeckung:   {coverage:.2f}% ({files} von {pool_len} Assets)")
        print(f" {'>'*2} Datenbasis-Größe: {size_mb:.2f} MB")
        print(f" {'>'*2} Neue ISINs (10k): {self.stats['new_isins']} (Pool: {pool_len})")
        print(f" {'>'*2} Speed:            {self.stats['total_processed']/elapsed:.1f} Ast/Min")
        print(f" {'>'*2} Geheilte Lücken:  {self.stats['gaps_filled']}")
        print("="*60 + "\n")

    def apply_iron_standard(self, df):
        if df.empty: return df, 0
        df = df.sort_values('Date').drop_duplicates(subset=['Date'])
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        
        anchors = [df.iloc[0].to_dict()]
        for i in range(1, len(df)):
            if abs((df.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                anchors.append(df.iloc[i].to_dict())
        return pd.DataFrame(anchors), len(anchors)

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}, {"symbol": "SIE.DE"}]

    def discover_logic(self, ticker):
        """Erweitert den Pool Richtung 10.000 Assets"""
        if len(self.pool) < 10000 and random.random() > 0.90:
            base = ticker.split('.')[0]
            for s in ['.DE', '.F', '.AS', '.L', '.PA', '.MI']:
                new_s = f"{base}{s}"
                if not any(a['symbol'] == new_s for a in self.pool):
                    self.pool.append({"symbol": new_s, "source": "discovery"})
                    with inspector.stats_lock: inspector.stats["new_isins"] += 1

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # Check Platte
            path_exists = any(ticker in f for r, d, files in os.walk(HERITAGE_ROOT) for f in files)
            
            # Stooq-Hürde (Serialisiert)
            hist = pd.DataFrame()
            if not path_exists: # Nur bei neuen Assets oder großen Lücken
                with STOOQ_LOCK:
                    time.sleep(random.uniform(1.2, 2.0))
                    r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=10)
                    if r.status_code == 200: 
                        hist = pd.read_csv(io.StringIO(r.text), parse_dates=['Date'])
            
            # Yahoo Update
            stock = yf.Ticker(ticker)
            recent = stock.history(period="5d", interval="5m").reset_index()
            recent.rename(columns={'Datetime': 'Date'}, inplace=True, errors='ignore')

            # Heilung & Speichern
            combined = pd.concat([hist, recent], ignore_index=True)
            clean_df, anchor_count = inspector.apply_iron_standard(combined)
            
            # Speicher-Logik (Klassisch nach Jahren)
            for year, group in clean_df.groupby(clean_df['Date'].dt.year):
                p = f"{HERITAGE_ROOT}{(int(year)//10)*10}s/heritage_{int(year)}.parquet"
                os.makedirs(os.path.dirname(p), exist_ok=True)
                if os.path.exists(p):
                    old = pd.read_parquet(p)
                    group = pd.concat([old, group]).drop_duplicates(subset=['Date']).sort_values('Date')
                group.to_parquet(p, index=False, compression='snappy')

            with inspector.stats_lock:
                inspector.stats["total_processed"] += 1
                inspector.stats["total_anchors"] += anchor_count
            
            self.discover_logic(ticker)
            return "OK"
        except: return "ERR"

    def run(self):
        print(f"INITIALISIERUNG: Pool enthält {len(self.pool)} Assets. Starte Scan...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_YAHOO) as executor:
            for i in range(0, len(self.pool), 400):
                batch = self.pool[i:i+400]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                for f in concurrent.futures.as_completed(futures):
                    pass # Ergebnisse werden über stats_lock gesammelt
                
                # Nach jedem Batch: Dashboard & Speichern
                inspector.log_status(len(self.pool))
                with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)

if __name__ == "__main__":
    AureumSentinel().run()
