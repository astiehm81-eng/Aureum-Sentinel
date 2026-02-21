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
MAX_WORKERS = 50 
ANCHOR_THRESHOLD = 0.0005  # 0,05% Anker-Regel
SKOOQ_URL = "https://stooq.com/q/d/l/?s={ticker}&i=d"

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.pool_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.stats = {
            "received": 0, "anchors": 0, "new_isins": 0, 
            "merges": 0, "errors": 0, "total": 0, "start": time.time()
        }

    def log(self, level, ticker, message):
        """Zentrales Logging für Audit-Trail"""
        ts = datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] [{level:7}] {ticker:10} | {message}"
        print(line, flush=True)
        with self.log_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def apply_iron_anchor(self, df, ticker):
        """Reduziert Daten auf 0,05% Ankerpunkte"""
        if df.empty: return df
        df = df.sort_values('Date').copy()
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        
        initial_count = len(df)
        anchors = [df.iloc[0].to_dict()]
        for i in range(1, len(df)):
            last_p = anchors[-1]['Close']
            curr_p = df.iloc[i]['Close']
            if abs((curr_p / last_p) - 1) >= ANCHOR_THRESHOLD:
                anchors.append(df.iloc[i].to_dict())
        
        res = pd.DataFrame(anchors)
        with self.stats_lock: self.stats["anchors"] += len(res)
        return res

    def save_heritage(self, df, ticker):
        """Mergen & Speichern in Jahresscheiben"""
        try:
            for year, group in df.groupby(df['Date'].dt.year):
                path = f"{HERITAGE_ROOT}{(int(year)//10)*10}s/heritage_{int(year)}.parquet"
                os.makedirs(os.path.dirname(path), exist_ok=True)
                
                if os.path.exists(path):
                    old = pd.read_parquet(path)
                    group = pd.concat([old, group]).drop_duplicates(subset=['Date'])
                    status = "MERGE"
                else:
                    status = "NEW"
                
                group.to_parquet(path, index=False, compression='snappy')
                with self.stats_lock: self.stats["merges"] += 1
                self.log(status, ticker, f"Jahr {int(year)}: {len(group)} Zeilen gesichert.")
        except Exception as e:
            self.log("ERROR", ticker, f"Speicherfehler: {str(e)}")

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}]

    def discover(self, ticker):
        """Erweitert Pool für 99% Abdeckung"""
        base = ticker.split('.')[0]
        for s in ['.DE', '.F', '.AS', '.L', '']:
            cand = f"{base}{s}"
            with inspector.pool_lock:
                if not any(a['symbol'] == cand for a in self.pool):
                    self.pool.append({"symbol": cand, "last_sync": "1900-01-01"})
                    inspector.stats["new_isins"] += 1
                    inspector.log("DISCOV", cand, f"Neu entdeckt via {ticker}")

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # Skooq (Historie)
            r = requests.get(SKOOQ_URL.format(ticker=ticker), timeout=10)
            hist = pd.read_csv(io.StringIO(r.text), parse_dates=['Date']) if len(r.content) > 300 else pd.DataFrame()
            
            # Yahoo (1 Woche Hard Refresh)
            stock = yf.Ticker(ticker)
            recent = stock.history(period="7d", interval="5m").reset_index()
            
            if recent.empty and hist.empty: return "EMPTY"

            # Heirat
            if not recent.empty:
                recent.rename(columns={'Datetime': 'Date'}, inplace=True, errors='ignore')
                cutoff = recent['Date'].min().tz_localize(None)
                hist = hist[hist['Date'] < cutoff] if not hist.empty else hist
                combined = pd.concat([hist, recent], ignore_index=True)
                inspector.log("SYNC", ticker, f"Heirat: {len(hist)} Hist + {len(recent)} Yahoo")
            else:
                combined = hist

            # 0,05% Anker & Save
            clean_df = inspector.apply_iron_anchor(combined, ticker)
            inspector.save_heritage(clean_df, ticker)
            
            if len(self.pool) < 15000: self.discover(ticker)
            return "OK"
        except Exception as e:
            inspector.log("FAIL", ticker, f"Fehler: {str(e)}")
            return "ERR"

    def print_dashboard(self):
        s = inspector.stats
        elapsed = (time.time() - s["start"]) / 60
        cov = (s["total"] / len(self.pool)) * 100
        
        box = (f"\n{'='*75}\n"
               f" AUREUM V236 | POOL: {len(self.pool)} | NEU: {s['new_isins']} | COV: {cov:.2f}%\n"
               f" SPEED: {s['total']/elapsed:.1f} Ast/Min | MERGES: {s['merges']} | ANKER: {s['anchors']}\n"
               f"{'='*75}\n")
        print(box, flush=True)

    def run(self):
        inspector.log("SYSTEM", "START", f"Zyklus mit {len(self.pool)} Assets gestartet.")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Batches für Stabilität und Dashboard-Intervallen
            for i in range(0, len(self.pool), 400):
                batch = self.pool[i:i+400]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                for f in concurrent.futures.as_completed(futures):
                    inspector.stats["total"] += 1
                    if inspector.stats["total"] % 40 == 0: self.print_dashboard()
                
                with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)
        inspector.log("SYSTEM", "END", "Zyklus abgeschlossen.")

if __name__ == "__main__":
    AureumSentinel().run()
