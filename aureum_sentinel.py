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

# --- KONFIGURATION EISERNER STANDARD ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage/sentinel_audit.log"
MAX_WORKERS_YAHOO = 40 
STOOQ_LOCK = threading.Lock() 
STORAGE_LOCK = threading.Lock() # Schützt die A-Z Partitionen
ANCHOR_THRESHOLD = 0.0005 

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.stats = {"processed": 0, "disk_hits": 0, "stooq_calls": 0, "start": time.time()}

    def log(self, level, ticker, message):
        """Erweitertes Logging: Schreibt in die Konsole UND in das Audit-Log"""
        ts = datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] [{level:7}] {ticker:10} | {message}"
        print(line, flush=True)
        with self.log_lock:
            os.makedirs(os.path.dirname(AUDIT_FILE), exist_ok=True)
            with open(AUDIT_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")

    def get_partition_path(self, ticker, year):
        """A-Z Partitionierung: heritage/2020s/A_2024.parquet"""
        first_char = ticker[0].upper() if ticker[0].isalpha() else "_"
        decade = f"{(int(year)//10)*10}s"
        folder = os.path.join(HERITAGE_ROOT, decade)
        return os.path.join(folder, f"{first_char}_{int(year)}.parquet")

    def apply_iron_anchor(self, df):
        if df.empty: return df
        df = df.sort_values('Date').drop_duplicates(subset=['Date'])
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        anchors = [df.iloc[0].to_dict()]
        for i in range(1, len(df)):
            if abs((df.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                anchors.append(df.iloc[i].to_dict())
        return pd.DataFrame(anchors)

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}]

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. INITIALER CHECK: Existiert das Asset in der A-Z Struktur?
            # Wir prüfen das aktuelle Jahr als Indikator
            current_year = datetime.now().year
            check_path = inspector.get_partition_path(ticker, current_year)
            
            has_local = False
            if os.path.exists(check_path):
                with STORAGE_LOCK:
                    temp_df = pd.read_parquet(check_path)
                    if ticker in temp_df.values: # Vereinfachter Check
                        has_local = True
            
            # 2. DATENBESCHAFFUNG (Heirat)
            hist = pd.DataFrame()
            if not has_local:
                inspector.log("FETCH", ticker, "Keine lokalen Daten - Stooq-Heilung eingeleitet")
                with STOOQ_LOCK:
                    time.sleep(random.uniform(1.2, 2.0))
                    r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=12)
                    if r.status_code == 200 and len(r.content) > 300:
                        hist = pd.read_csv(io.StringIO(r.text), parse_dates=['Date'])
                        with inspector.stats_lock: inspector.stats["stooq_calls"] += 1
            else:
                with inspector.stats_lock: inspector.stats["disk_hits"] += 1

            # Immer Yahoo-Refresh für die letzte Woche
            stock = yf.Ticker(ticker)
            recent = stock.history(period="7d", interval="5m").reset_index()
            recent.rename(columns={'Datetime': 'Date'}, inplace=True, errors='ignore')

            # 3. HEILUNG & REFRESH
            combined = pd.concat([hist, recent], ignore_index=True)
            clean_df = inspector.apply_iron_anchor(combined)
            
            # 4. PARTITIONIERTES SPEICHERN (A-Z)
            with STORAGE_LOCK:
                for year, group in clean_df.groupby(clean_df['Date'].dt.year):
                    path = inspector.get_partition_path(ticker, year)
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    
                    if os.path.exists(path):
                        old = pd.read_parquet(path)
                        # Filter für spezifisches Asset in der A-Z Datei (falls gemischt)
                        group = pd.concat([old, group]).drop_duplicates(subset=['Date', 'Close']).sort_values('Date')
                    
                    # Atomares Schreiben
                    tmp_path = path + ".tmp"
                    group.to_parquet(tmp_path, index=False, compression='snappy')
                    os.replace(tmp_path, path)

            inspector.log("DONE", ticker, f"Basis stabil. {len(clean_df)} Anker gesetzt.")
            with inspector.stats_lock: inspector.stats["processed"] += 1
            return "OK"

        except Exception as e:
            inspector.log("ERROR", ticker, f"Fehlgeschlagen: {str(e)[:50]}")
            return "ERR"

    def run(self):
        inspector.log("SYSTEM", "START", f"V252 aktiv. Pool: {len(self.pool)} Assets.")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_YAHOO) as executor:
            # Batch-Verarbeitung für kontinuierliches Logging
            for i in range(0, len(self.pool), 200):
                batch = self.pool[i:i+200]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                
                for f in concurrent.futures.as_completed(futures):
                    pass # Worker loggen selbstständig

                # Dashboard-Update alle 200 Assets
                elapsed = (time.time() - inspector.stats['start']) / 60
                print(f"\n--- DASHBOARD | Progress: {inspector.stats['processed']} | Disk-Hits: {inspector.stats['disk_hits']} | Stooq: {inspector.stats['stooq_calls']} | Speed: {inspector.stats['processed']/elapsed:.1f} Ast/Min ---\n")
                
                with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)

if __name__ == "__main__":
    AureumSentinel().run()
