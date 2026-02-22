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

# --- KONFIGURATION (GOLDENER STANDARD V272) ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
BLACKLIST_FILE = "blacklist.json"
MAX_WORKERS = 200
STOOQ_LOCK = threading.Lock()
FILE_LOCKS = {}
FILE_LOCKS_LOCK = threading.Lock()
ANCHOR_THRESHOLD = 0.0005 

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.error_registry = {}
        self.stats = {
            "processed": 0, "errors": 0, "start": time.time(),
            "found_in_db": 0, "pool_total": 0
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
        self.blacklist = self.load_json(BLACKLIST_FILE, set)
        self.load_and_refine_pool()
        self.perform_pre_flight_inspection()

    def init_structure(self):
        for d in ["1960s","1970s","1980s","1990s", "2000s", "2010s", "2020s"]:
            os.makedirs(os.path.join(HERITAGE_ROOT, d), exist_ok=True)

    def load_json(self, path, type_func):
        if os.path.exists(path):
            try:
                with open(path, "r") as f: return type_func(json.load(f))
            except: return type_func()
        return type_func()

    def load_and_refine_pool(self):
        """Filtert Dubletten und loggt den Prozess"""
        raw_pool = self.load_json(POOL_FILE, list)
        inspector.stats["pool_total"] = len(raw_pool)
        
        refined = {}
        for entry in raw_pool:
            ticker = entry['symbol']
            if ticker in self.blacklist: continue
            
            base = ticker.split('.')[0]
            if base not in refined:
                refined[base] = entry
            else:
                current = refined[base]['symbol']
                if '.' not in ticker: refined[base] = entry
                elif '.DE' in ticker and '.' in current: refined[base] = entry
        
        self.pool = list(refined.values())
        inspector.log("SYSTEM", "POOL", f"Refined: {len(self.pool)} Primär-Assets geladen.")

    def perform_pre_flight_inspection(self):
        found_tickers = set()
        for root, _, files in os.walk(HERITAGE_ROOT):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        df = pd.read_parquet(os.path.join(root, f), columns=['Ticker'])
                        found_tickers.update(df['Ticker'].unique())
                    except: pass
        inspector.stats["found_in_db"] = len(found_tickers)
        inspector.log("STATUS", "GLOBAL", f"Marktabdeckung: {len(found_tickers)} Assets in DB | Pool-Abgleich gestartet.")

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # Phase 1: Stooq
            inspector.log("FETCH", ticker, "Anfrage bei Stooq gestartet...")
            with STOOQ_LOCK:
                time.sleep(random.uniform(0.001, 0.005))
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=10)
                hist = pd.read_csv(io.StringIO(r.text)) if r.status_code == 200 else pd.DataFrame()
            
            # Phase 2: Yahoo (Zurück zum stabilen Modus)
            inspector.log("FETCH", ticker, "Anfrage bei Yahoo (7d/5m) gestartet...")
            y_obj = yf.Ticker(ticker)
            recent = y_obj.history(period="7d", interval="5m").reset_index()
            
            # Phase 3: Daten-Merge
            df = pd.concat([self.clean_df(hist), self.clean_df(recent)])
            if df.empty: 
                inspector.log("WARN", ticker, "Keine Daten gefunden.")
                raise ValueError("Empty")

            df = df.sort_values('Date').drop_duplicates(subset=['Date'])
            last_price = df.iloc[-1]['Close']
            
            # Phase 4: Anker-Berechnung & Speicherung
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
                    if not db.empty: db = db[db['Ticker'] != ticker]
                    group['Ticker'] = ticker
                    pd.concat([db, group], ignore_index=True).to_parquet(path, index=False, compression='snappy')

            with inspector.stats_lock: inspector.stats["processed"] += 1
            inspector.log("DONE", ticker, f"Erfolgreich synchronisiert | Letzter Kurs: {last_price:.2f}")

        except Exception as e:
            inspector.log("ERROR", ticker, f"Fehlgeschlagen: {str(e)}")
            with inspector.stats_lock: inspector.stats["errors"] += 1
            # Blacklist Logik (optional hier einbauen)

    def clean_df(self, df):
        if df is None or df.empty: return pd.DataFrame()
        if 'Date' not in df.columns and 'Datetime' in df.columns:
            df = df.rename(columns={'Datetime': 'Date'})
        if 'Date' not in df.columns: return pd.DataFrame()
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        return df

    def run(self):
        inspector.log("SYSTEM", "START", f"V272 gestartet mit {MAX_WORKERS} Workern.")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), 200):
                batch = self.pool[i:i+200]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                concurrent.futures.wait(futures)
                
                elapsed = (time.time() - inspector.stats['start']) / 60
                speed = inspector.stats['processed']/elapsed
                inspector.log("STATS", "DASH", f"Batch beendet | Speed: {speed:.1f} Ast/Min | Errors: {inspector.stats['errors']}")

if __name__ == "__main__":
    AureumSentinel().run()
