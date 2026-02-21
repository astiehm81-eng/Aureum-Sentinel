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
MAX_WORKERS = 100
STOOQ_LOCK = threading.Lock()
FILE_LOCKS = {}
FILE_LOCKS_LOCK = threading.Lock()
ANCHOR_THRESHOLD = 0.0005 

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {
            "processed": 0, "errors": 0, "start": time.time(),
            "found_in_db": 0, "pool_total": 0, "coverage_pct": 0.0
        }

    def log(self, level, ticker, message):
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

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.init_structure()
        self.load_pool()
        self.perform_pre_flight_inspection()

    def init_structure(self):
        for d in ["1960s","1970s","1980s","1990s", "2000s", "2010s", "2020s"]:
            os.makedirs(os.path.join(HERITAGE_ROOT, d), exist_ok=True)

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}]
        inspector.stats["pool_total"] = len(self.pool)

    def perform_pre_flight_inspection(self):
        """
        Scannt die Datenbank, um Dubletten zu vermeiden und 
        die echte Marktabdeckung zu ermitteln.
        """
        inspector.log("SYSTEM", "INSPECT", "Starte Datenbank-Scan zur Marktabdeckung...")
        found_tickers = set()
        
        # Durchsuche alle Jahre in allen Dekaden
        for root, _, files in os.walk(HERITAGE_ROOT):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        # Wir lesen nur die Ticker-Spalte für Speed
                        df = pd.read_parquet(os.path.join(root, f), columns=['Ticker'])
                        found_tickers.update(df['Ticker'].unique())
                    except: pass
        
        inspector.stats["found_in_db"] = len(found_tickers)
        if inspector.stats["pool_total"] > 0:
            inspector.stats["coverage_pct"] = (len(found_tickers) / inspector.stats["pool_total"]) * 100
        
        # STATUS-LOGGING AM ANFANG
        inspector.log("STATUS", "GLOBAL", f"Assets in DB: {inspector.stats['found_in_db']}")
        inspector.log("STATUS", "GLOBAL", f"ISIN-Pool:    {inspector.stats['pool_total']}")
        inspector.log("STATUS", "GLOBAL", f"Abdeckung:    {inspector.stats['coverage_pct']:.2f}%")

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. Daten-Heirat (Immer Voll-Sync für Zeitreihen-Vervollständigung)
            with STOOQ_LOCK:
                time.sleep(random.uniform(0.01, 0.02))
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=7)
                hist = pd.read_csv(io.StringIO(r.text)) if r.status_code == 200 else pd.DataFrame()
            
            recent = yf.Ticker(ticker).history(period="7d", interval="5m").reset_index()
            
            # Normalisierung & Eiserner Standard
            df_raw = pd.concat([self.clean_df(hist), self.clean_df(recent)])
            if df_raw.empty: return
            
            df_raw = df_raw.sort_values('Date').drop_duplicates(subset=['Date'])
            anchors = [df_raw.iloc[0].to_dict()]
            for i in range(1, len(df_raw)):
                if abs((df_raw.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                    anchors.append(df_raw.iloc[i].to_dict())
            
            # 2. In Jahres-Dateien integrieren
            final_df = pd.DataFrame(anchors)
            for year, group in final_df.groupby(final_df['Date'].dt.year):
                decade = f"{(int(year)//10)*10}s"
                path = os.path.join(HERITAGE_ROOT, decade, f"{int(year)}.parquet")
                
                with inspector.get_lock(path):
                    db = pd.read_parquet(path) if os.path.exists(path) else pd.DataFrame()
                    if not db.empty and 'Ticker' in db.columns:
                        db = db[db['Ticker'] != ticker] # Replace für Vollständigkeit
                    
                    group['Ticker'] = ticker
                    pd.concat([db, group], ignore_index=True).to_parquet(path, index=False, compression='snappy')

            with inspector.stats_lock: inspector.stats["processed"] += 1
            inspector.log("DONE", ticker, "Sync ok.")
        except:
            with inspector.stats_lock: inspector.stats["errors"] += 1

    def clean_df(self, df):
        if df is None or df.empty: return pd.DataFrame()
        if 'Date' not in df.columns and 'Datetime' in df.columns:
            df = df.rename(columns={'Datetime': 'Date'})
        if 'Date' not in df.columns: return pd.DataFrame()
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        return df

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), 200):
                batch = self.pool[i:i+200]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                concurrent.futures.wait(futures)
                
                # Periodisches Dashboard-Update im Log
                elapsed = (time.time() - inspector.stats['start']) / 60
                inspector.log("STATS", "DASH", f"Proc: {inspector.stats['processed']} | Speed: {inspector.stats['processed']/elapsed:.1f} Ast/Min")

if __name__ == "__main__":
    AureumSentinel().run()
