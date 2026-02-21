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
AUDIT_FILE = "heritage/sentinel_audit.log"
MAX_WORKERS = 40 
# Stooq-Drosselung: Nur 8 gleichzeitige Anfragen (Sicherheits-Layer)
SKOOQ_SEMAPHORE = threading.Semaphore(8) 
ANCHOR_THRESHOLD = 0.0005 # 0,05% Anker-Regel
SKOOQ_URL = "https://stooq.com/q/d/l/?s={ticker}&i=d"

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.pool_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.stats = {"total": 0, "anchors": 0, "new_isins": 0, "merges": 0, "start": time.time()}

    def log(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] [{level:7}] {ticker:10} | {message}"
        print(line, flush=True)
        with self.log_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")

    def apply_iron_anchor(self, df):
        """0,05% Anker-Logik: Eliminiert Noise, behält Signale"""
        if df.empty: return df
        df = df.sort_values('Date').copy()
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        
        anchors = [df.iloc[0].to_dict()]
        for i in range(1, len(df)):
            if abs((df.iloc[i]['Close'] / anchors[-1]['Close']) - 1) >= ANCHOR_THRESHOLD:
                anchors.append(df.iloc[i].to_dict())
        
        res = pd.DataFrame(anchors)
        with self.stats_lock: self.stats["anchors"] += len(res)
        return res

    def save_heritage(self, df, ticker):
        """Datenheirat auf Dateiebene (Merge & Hard Refresh)"""
        for year, group in df.groupby(df['Date'].dt.year):
            path = f"{HERITAGE_ROOT}{(int(year)//10)*10}s/heritage_{int(year)}.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if os.path.exists(path):
                old = pd.read_parquet(path)
                group = pd.concat([old, group]).drop_duplicates(subset=['Date'])
            group.to_parquet(path, index=False, compression='snappy')
            with self.stats_lock: self.stats["merges"] += 1

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
            # 1. Skooq-Daten (Historie) mit Semaphore-Schutz
            hist = pd.DataFrame()
            with SKOOQ_SEMAPHORE:
                r = requests.get(SKOOQ_URL.format(ticker=ticker), timeout=8)
                if len(r.content) > 300:
                    hist = pd.read_csv(io.StringIO(r.text), parse_dates=['Date'])
            
            # 2. Yahoo-Daten (Hard Refresh 1 Woche)
            stock = yf.Ticker(ticker)
            recent = stock.history(period="7d", interval="5m").reset_index()
            
            if recent.empty and hist.empty: return "EMPTY"

            # 3. Heirat & Anker-Filter
            recent.rename(columns={'Datetime': 'Date'}, inplace=True, errors='ignore')
            recent['Date'] = pd.to_datetime(recent['Date'], utc=True).dt.tz_localize(None)
            
            if not hist.empty:
                hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
                cutoff = recent['Date'].min()
                hist = hist[hist['Date'] < cutoff]
                combined = pd.concat([hist, recent], ignore_index=True)
                inspector.log("SYNC", ticker, f"Heirat: {len(hist)}H + {len(recent)}Y")
            else:
                combined = recent
                inspector.log("YAHOO", ticker, "Nur Yahoo-Daten verfügbar")

            clean_df = inspector.apply_iron_anchor(combined)
            inspector.save_heritage(clean_df, ticker)
            
            # 4. Discovery (Wachstum auf 10k+ ISINs)
            if len(self.pool) < 15000:
                base = ticker.split('.')[0]
                for s in ['.DE', '.F', '.AS', '.L']:
                    cand = f"{base}{s}"
                    with inspector.pool_lock:
                        if not any(a['symbol'] == cand for a in self.pool):
                            self.pool.append({"symbol": cand, "last_sync": "1900-01-01"})
                            inspector.stats["new_isins"] += 1
            return "OK"
        except Exception as e:
            inspector.log("FAIL", ticker, f"Fehler: {e}")
            return "ERR"

    def run(self):
        inspector.log("SYSTEM", "START", f"Zyklus V238 gestartet (Workers: {MAX_WORKERS})")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), 400):
                batch = self.pool[i:i+400]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                for f in concurrent.futures.as_completed(futures):
                    inspector.stats["total"] += 1
                    if inspector.stats["total"] % 40 == 0:
                        elapsed = (time.time() - inspector.stats["start"]) / 60
                        print(f"\n[DASHBOARD] Abdeckung: {(inspector.stats['total']/len(self.pool))*100:.2f}% | ISINs: {len(self.pool)} | Speed: {inspector.stats['total']/elapsed:.1f} Ast/Min\n")
                
                with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)

if __name__ == "__main__":
    AureumSentinel().run()
