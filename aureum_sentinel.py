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
AUDIT_FILE = "heritage_audit.txt"
MAX_WORKERS = 50 
STOOQ_LOCK = threading.Lock()
FILE_LOCKS = {}
FILE_LOCKS_LOCK = threading.Lock()
ANCHOR_THRESHOLD = 0.0005 

class AureumInspector:
    def __init__(self, initial_pool_size):
        self.stats_lock = threading.Lock()
        self.stats = {"processed": 0, "errors": 0, "start": time.time(), "initial_pool": initial_pool_size}

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
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        return df

inspector = None

class AureumSentinel:
    def __init__(self):
        self.init_structure()
        self.cleanup_old_files()
        self.load_and_smart_clean_pool()

    def init_structure(self):
        for d in ["1990s", "2000s", "2010s", "2020s"]:
            os.makedirs(os.path.join(HERITAGE_ROOT, d), exist_ok=True)

    def cleanup_old_files(self):
        """Bereinigt lose Parquet-Dateien im Hauptverzeichnis (ehem. flache Struktur)"""
        for f in os.listdir(HERITAGE_ROOT):
            if f.endswith(".parquet") and os.path.isfile(os.path.join(HERITAGE_ROOT, f)):
                try:
                    os.remove(os.path.join(HERITAGE_ROOT, f))
                except: pass

    def load_and_smart_clean_pool(self):
        """
        Smart-Ticker Logik: US-Ticker ohne Suffix, DE-Ticker mit .DE.
        Verhindert Fehlversuche wie ADBE.DE.
        """
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: raw_pool = json.load(f)
            clean_map = {}
            for entry in raw_pool:
                sym = entry['symbol']
                base = sym.split('.')[0]
                # US-Hauptmarkt bevorzugen (ohne Punkt)
                if '.' not in sym:
                    clean_map[base] = entry
                elif '.DE' in sym and base not in clean_map:
                    clean_map[base] = entry
                elif base not in clean_map:
                    clean_map[base] = entry
            self.pool = list(clean_map.values())
        else:
            self.pool = [{"symbol": "SAP.DE"}]
        
        global inspector
        inspector = AureumInspector(len(self.pool))

    def get_physical_coverage(self):
        """Misst die reale Anzahl eindeutiger Assets in der Datenbank"""
        tickers = set()
        for root, _, files in os.walk(HERITAGE_ROOT):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        temp = pd.read_parquet(os.path.join(root, f), columns=['Ticker'])
                        tickers.update(temp['Ticker'].unique())
                    except: pass
        return len(tickers)

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            with STOOQ_LOCK:
                time.sleep(random.uniform(0.2, 0.4))
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=7)
                hist = pd.read_csv(io.StringIO(r.text)) if r.status_code == 200 else pd.DataFrame()
            
            recent = yf.Ticker(ticker).history(period="7d", interval="5m").reset_index()
            df = pd.concat([inspector.clean_df(hist), inspector.clean_df(recent)])
            if df.empty: return

            df = df.sort_values('Date').drop_duplicates(subset=['Date'])
            anchors = [df.iloc[0].to_dict()]
            for i in range(1, len(df)):
                if abs((df.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                    anchors.append(df.iloc[i].to_dict())
            
            final_df = pd.DataFrame(anchors)
            for year, group in final_df.groupby(final_df['Date'].dt.year):
                decade = f"{(int(year)//10)*10}s"
                path = os.path.join(HERITAGE_ROOT, decade, f"{int(year)}.parquet")
                
                with inspector.get_lock(path):
                    db = pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame()
                    if not db.empty and 'Ticker' in db.columns:
                        db = db[db['Ticker'] != ticker] # Heilung/Replace
                    
                    group['Ticker'] = ticker
                    pd.concat([db, group], ignore_index=True).to_parquet(path, index=False, compression='snappy')

            with inspector.stats_lock: inspector.stats["processed"] += 1
        except:
            with inspector.stats_lock: inspector.stats["errors"] += 1

    def run(self):
        # STATUS-AUSGABE AM ANFANG (Wie gewünscht)
        coverage = self.get_physical_coverage()
        start_msg = f"INITIAL STATUS | Assets in DB: {coverage} | ISIN-Pool: {len(self.pool)} | Marktabdeckung: {coverage/100:.2f}% (Ziel 10k)"
        inspector.log_audit("SYSTEM", "START", start_msg)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), 200):
                batch = self.pool[i:i+200]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                concurrent.futures.wait(futures)
                
                current_cov = self.get_physical_coverage()
                elapsed = (time.time() - inspector.stats['start']) / 60
                dash = f"COVERAGE: {current_cov} | POOL: {len(self.pool)} | SPEED: {inspector.stats['processed']/elapsed:.1f} Ast/Min"
                inspector.log_audit("STATS", "DASH", dash)

if __name__ == "__main__":
    AureumSentinel().run()
