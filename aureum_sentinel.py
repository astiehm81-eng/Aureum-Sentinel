import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import time
from datetime import datetime

# --- KONFIGURATION EISERNER STANDARD ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
MAX_WORKERS = 50 
STOOQ_LOCK = threading.Lock()
FILE_LOCKS = {}
FILE_LOCKS_LOCK = threading.Lock()
ANCHOR_THRESHOLD = 0.0005 

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {
            "processed": 0, "errors": 0, "new_isin": 0,
            "start": time.time(), "market_coverage": 0
        }

    def log_audit(self, message):
        """Schreibt in die heritage_audit.txt und Konsole"""
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] {message}"
        print(line, flush=True)
        with self.stats_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def get_lock(self, path):
        with FILE_LOCKS_LOCK:
            if path not in FILE_LOCKS: FILE_LOCKS[path] = threading.Lock()
            return FILE_LOCKS[path]

    def clean_timestamp(self, df):
        if df.empty: return df
        if 'Date' not in df.columns and 'Datetime' in df.columns:
            df = df.rename(columns={'Datetime': 'Date'})
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        return df

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()
        self.init_structure()

    def init_structure(self):
        """Erzeugt die neue Ordner-Hierarchie physisch"""
        for decade in ["1990s", "2000s", "2010s", "2020s"]:
            os.makedirs(os.path.join(HERITAGE_ROOT, decade), exist_ok=True)

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
            # Bereinigung: Nur Haupt-Ticker behalten (kein BMW.AS wenn BMW.DE existiert)
            unique_pool = {}
            for entry in self.pool:
                base = entry['symbol'].split('.')[0]
                if base not in unique_pool or '.DE' in entry['symbol']:
                    unique_pool[base] = entry
            self.pool = list(unique_pool.values())
        else:
            self.pool = [{"symbol": "SAP.DE"}]

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. Daten-Heirat (Stooq Historie + Yahoo 7d)
            with STOOQ_LOCK:
                time.sleep(0.3)
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=7)
                hist = pd.read_csv(io.StringIO(r.text)) if r.status_code == 200 else pd.DataFrame()
            
            recent = yf.Ticker(ticker).history(period="7d", interval="5m").reset_index()
            
            # 2. Eiserner Standard (Anker-Filterung)
            df = pd.concat([inspector.clean_timestamp(hist), inspector.clean_timestamp(recent)])
            if df.empty: return
            
            df = df.sort_values('Date').drop_duplicates(subset=['Date'])
            anchors = [df.iloc[0].to_dict()]
            for i in range(1, len(df)):
                if abs((df.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                    anchors.append(df.iloc[i].to_dict())
            
            # 3. Partitioniertes Speichern (Year-Files)
            final_df = pd.DataFrame(anchors)
            for year, group in final_df.groupby(final_df['Date'].dt.year):
                decade = f"{(int(year)//10)*10}s"
                path = os.path.join(HERITAGE_ROOT, decade, f"{int(year)}.parquet")
                
                with inspector.get_lock(path):
                    existing = pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame()
                    # Altes Asset-Fragment überschreiben für Vollständigkeit
                    if not existing.empty and 'Ticker' in existing.columns:
                        existing = existing[existing['Ticker'] != ticker]
                    
                    group['Ticker'] = ticker
                    updated = pd.concat([existing, group], ignore_index=True)
                    updated.to_parquet(path, index=False, compression='snappy')

            with inspector.stats_lock: inspector.stats["processed"] += 1
        except:
            with inspector.stats_lock: inspector.stats["errors"] += 1

    def run(self):
        inspector.log_audit(f"START V260 | Pool-Größe: {len(self.pool)} ISINs")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), 300):
                batch = self.pool[i:i+300]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                concurrent.futures.wait(futures)
                
                # DASHBOARD-LOGGING
                elapsed = (time.time() - inspector.stats['start']) / 60
                coverage = self.calculate_coverage()
                msg = (f"STATS | Assets: {inspector.stats['processed']} | "
                       f"Abdeckung: {coverage} Unikate | Speed: {inspector.stats['processed']/elapsed:.1f} Ast/Min")
                inspector.log_audit(msg)

    def calculate_coverage(self):
        """Zählt die eindeutigen Ticker über alle neuen Jahres-Dateien"""
        tickers = set()
        for root, dirs, files in os.walk(HERITAGE_ROOT):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        df = pd.read_parquet(os.path.join(root, f), columns=['Ticker'])
                        tickers.update(df['Ticker'].unique())
                    except: pass
        return len(tickers)

if __name__ == "__main__":
    AureumSentinel().run()
