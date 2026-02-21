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
MAX_WORKERS = 60 
STOOQ_LOCK = threading.Lock()
FILE_LOCKS = {} 
FILE_LOCKS_LOCK = threading.Lock()
ANCHOR = 0.0005 

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {"processed": 0, "errors": 0, "start": time.time()}

    def log(self, level, ticker, message):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level:7}] {ticker:10} | {message}", flush=True)

    def get_lock(self, path):
        with FILE_LOCKS_LOCK:
            if path not in FILE_LOCKS: FILE_LOCKS[path] = threading.Lock()
            return FILE_LOCKS[path]

    def safe_normalize(self, df):
        """Wäscht Zeitstempel und entfernt Zeitzonen radikal"""
        if df is None or df.empty: return pd.DataFrame()
        if 'Date' not in df.columns and 'Datetime' in df.columns:
            df = df.rename(columns={'Datetime': 'Date'})
        if 'Date' not in df.columns: return pd.DataFrame()
        
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        return df

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()
        os.makedirs(HERITAGE_ROOT, exist_ok=True)

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}]

    def fetch_stooq_safe(self, ticker):
        """Robustes Parsen: Verhindert 'Missing Column Date' Fehler"""
        with STOOQ_LOCK:
            time.sleep(random.uniform(0.2, 0.4))
            try:
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=7)
                if r.status_code == 200 and len(r.content) > 100:
                    df = pd.read_csv(io.StringIO(r.text))
                    if 'Date' in df.columns: return df
            except: pass
        return pd.DataFrame()

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. DATEN-AKQUISE
            hist = self.fetch_stooq_safe(ticker)
            recent = yf.Ticker(ticker).history(period="7d", interval="5m").reset_index()
            
            # 2. WASCHEN & HEIRATEN
            hist = inspector.safe_normalize(hist)
            recent = inspector.safe_normalize(recent)
            
            full_series = pd.concat([hist, recent], ignore_index=True)
            if full_series.empty: 
                inspector.log("SKIP", ticker, "Keine Daten gefunden.")
                return

            # 3. VERTEILUNG AUF JAHRES-DATEIEN (Oberaufseher)
            full_series['Year'] = full_series['Date'].dt.year
            for year, group in full_series.groupby('Year'):
                decade_dir = os.path.join(HERITAGE_ROOT, f"{(int(year)//10)*10}s")
                os.makedirs(decade_dir, exist_ok=True)
                file_path = os.path.join(decade_dir, f"{int(year)}.parquet")

                with inspector.get_lock(file_path):
                    # Datei laden und säubern
                    if os.path.exists(file_path):
                        db = pd.read_parquet(file_path)
                        db = inspector.safe_normalize(db)
                        # Altes Asset-Fragment entfernen für sauberen Neu-Eintrag (Vervollständigung)
                        if 'Ticker' in db.columns:
                            db = db[db['Ticker'] != ticker]
                    else:
                        db = pd.DataFrame()

                    # Neuen Eiserner Standard Anker für dieses Asset berechnen
                    group = group.sort_values('Date').drop_duplicates(subset=['Date'])
                    anchors = [group.iloc[0].to_dict()]
                    for i in range(1, len(group)):
                        if abs((group.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR:
                            anchors.append(group.iloc[i].to_dict())
                    
                    asset_final = pd.DataFrame(anchors)
                    asset_final['Ticker'] = ticker
                    
                    # Zusammenführen mit dem Rest des Marktes in dieser Jahres-Datei
                    new_db = pd.concat([db, asset_final], ignore_index=True)
                    new_db.to_parquet(file_path, index=False, compression='snappy')

            inspector.log("DONE", ticker, "Sync & Year-Partition stabil.")
            with inspector.stats_lock: inspector.stats["processed"] += 1
        except Exception as e:
            inspector.log("FAIL", ticker, f"Fehler: {str(e)[:45]}")
            with inspector.stats_lock: inspector.stats["errors"] += 1

    def run(self):
        inspector.log("SYSTEM", "START", f"V259 Robust Mode | Pool: {len(self.pool)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), 400):
                batch = self.pool[i:i+400]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                concurrent.futures.wait(futures)
                
                elapsed = (time.time() - inspector.stats['start']) / 60
                print(f"\n[DASHBOARD] {inspector.stats['processed']} OK | {inspector.stats['errors']} FAIL | Speed: {inspector.stats['processed']/elapsed:.1f} Ast/Min\n")

if __name__ == "__main__":
    AureumSentinel().run()
