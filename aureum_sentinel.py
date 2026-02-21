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
AUDIT_FILE = "heritage_audit.txt"
MAX_WORKERS = 50 
STOOQ_LOCK = threading.Lock()
FILE_LOCKS = {}
FILE_LOCKS_LOCK = threading.Lock()
ANCHOR_THRESHOLD = 0.0005 # 0,05% Anker nach Anforderung

class AureumInspector:
    def __init__(self, initial_pool_size):
        self.stats_lock = threading.Lock()
        self.stats = {
            "processed": 0, "errors": 0, "skips": 0,
            "start": time.time(), "initial_pool": initial_pool_size
        }

    def log_audit(self, level, ticker, message):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] [{level:7}] {ticker:10} | {message}"
        print(line, flush=True)
        with self.stats_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def get_lock(self, path):
        with FILE_LOCKS_LOCK:
            if path not in FILE_LOCKS: FILE_LOCKS[path] = threading.Lock()
            return FILE_LOCKS[path]

    def clean_df(self, df):
        if df is None or df.empty: return pd.DataFrame()
        if 'Date' not in df.columns and 'Datetime' in df.columns:
            df = df.rename(columns={'Datetime': 'Date'})
        if 'Date' not in df.columns: return pd.DataFrame()
        # Radikale Normalisierung für Zeitreihen-Vervollständigung
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        return df

inspector = None

class AureumSentinel:
    def __init__(self):
        self.init_structure()
        self.load_and_clean_pool()

    def init_structure(self):
        """Erzwingt die Ordnerstruktur heritage/1990s...2020s"""
        for d in ["1990s", "2000s", "2010s", "2020s"]:
            os.makedirs(os.path.join(HERITAGE_ROOT, d), exist_ok=True)

    def load_and_clean_pool(self):
        """Master-ISIN Logik: Reduziert Pool auf Primär-Assets (bevorzugt .DE)"""
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: raw_pool = json.load(f)
            clean_map = {}
            for entry in raw_pool:
                base = entry['symbol'].split('.')[0]
                # Priorisiere deutsche Börse oder US-Hauptmarkt
                if base not in clean_map or '.DE' in entry['symbol']:
                    clean_map[base] = entry
            self.pool = list(clean_map.values())
        else:
            self.pool = [{"symbol": "SAP.DE"}]
        
        global inspector
        inspector = AureumInspector(len(self.pool))

    def fetch_stooq(self, ticker):
        """Robustes Nitro-Fetching"""
        with STOOQ_LOCK:
            time.sleep(random.uniform(0.2, 0.45))
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=7)
                if r.status_code == 200 and len(r.content) > 100:
                    return pd.read_csv(io.StringIO(r.text))
            except: pass
        return pd.DataFrame()

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. Daten holen (Historie + Aktuell)
            hist = self.fetch_stooq(ticker)
            recent = yf.Ticker(ticker).history(period="7d", interval="5m").reset_index()
            
            # 2. Heirat & Normalisierung
            df = pd.concat([inspector.clean_df(hist), inspector.clean_df(recent)])
            if df.empty:
                with inspector.stats_lock: inspector.stats["skips"] += 1
                return
            
            # Eiserner Standard Anker-Filter
            df = df.sort_values('Date').drop_duplicates(subset=['Date'])
            anchors = [df.iloc[0].to_dict()]
            for i in range(1, len(df)):
                if abs((df.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                    anchors.append(df.iloc[i].to_dict())
            
            # 3. Oberaufseher: Partitionierung & Integration
            final_df = pd.DataFrame(anchors)
            for year, group in final_df.groupby(final_df['Date'].dt.year):
                decade = f"{(int(year)//10)*10}s"
                file_path = os.path.join(HERITAGE_ROOT, decade, f"{int(year)}.parquet")
                
                with inspector.get_lock(file_path):
                    db = pd.read_parquet(file_path) if os.path.exists(file_path) else pd.DataFrame()
                    # Zeitreihen-Vervollständigung: Altes Fragment des Tickers entfernen
                    if not db.empty and 'Ticker' in db.columns:
                        db = db[db['Ticker'] != ticker]
                    
                    group['Ticker'] = ticker
                    pd.concat([db, group], ignore_index=True).to_parquet(file_path, index=False, compression='snappy')

            with inspector.stats_lock: inspector.stats["processed"] += 1
        except Exception as e:
            inspector.log_audit("ERROR", ticker, f"Sync fehlgeschlagen: {str(e)[:40]}")
            with inspector.stats_lock: inspector.stats["errors"] += 1

    def run(self):
        inspector.log_audit("SYSTEM", "START", f"V262 Master-Sync | Pool: {len(self.pool)}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Batches für regelmäßiges Dashboard-Logging
            for i in range(0, len(self.pool), 200):
                batch = self.pool[i:i+200]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                concurrent.futures.wait(futures)
                
                # Marktabdeckung physisch messen
                actual_coverage = self.get_physical_coverage()
                elapsed = (time.time() - inspector.stats['start']) / 60
                
                dash = (f"ABDECKUNG: {actual_coverage} Unikate | "
                        f"SPEED: {inspector.stats['processed']/elapsed:.1f} Ast/Min | "
                        f"ERRS: {inspector.stats['errors']}")
                inspector.log_audit("STATS", "DASHBOARD", dash)

    def get_physical_coverage(self):
        """Scannt alle Jahres-Dateien nach einzigartigen Tickern"""
        unique_tickers = set()
        for root, _, files in os.walk(HERITAGE_ROOT):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        temp = pd.read_parquet(os.path.join(root, f), columns=['Ticker'])
                        unique_tickers.update(temp['Ticker'].unique())
                    except: pass
        return len(unique_tickers)

if __name__ == "__main__":
    AureumSentinel().run()
