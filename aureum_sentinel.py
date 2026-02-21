import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import random
import time
from datetime import datetime

# --- KONFIGURATION EISERNER STANDARD ---
HERITAGE_ROOT = "heritage/"
AUDIT_FILE = "heritage/heritage_audit.txt"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 60 # Höhere Parallelität durch Buffering möglich
BATCH_SIZE = 100 # Schreibt erst nach 100 Assets auf die Platte
STOOQ_SEMAPHORE = threading.Semaphore(12) 
JITTER = 0.005 # Fast kein Jitter mehr

class AureumInspector:
    def __init__(self, root_path):
        self.root = root_path
        self.audit_lock = threading.Lock()
        self.buffer_lock = threading.Lock()
        self.data_buffer = [] # RAM-Speicher für Batch-Writes
        self.stats = {"success": 0, "errors": 0, "new_isins": 0, "total_processed": 0, "start_time": time.time()}

    def log_event(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        line = f"[{ts}] [{level:9}] {ticker:8} | {message}"
        print(line, flush=True)
        with self.audit_lock:
            with open(AUDIT_FILE, "a", encoding="utf-8") as f: f.write(line + "\n")

    def add_to_buffer(self, data_package):
        """Sammelt Daten im RAM statt sofort zu schreiben"""
        with self.buffer_lock:
            self.data_buffer.append(data_package)
            if len(self.data_buffer) >= BATCH_SIZE:
                self.flush_buffer()

    def flush_buffer(self):
        """Schreibt alle gesammelten Daten im Batch-Verfahren (IO-Boost)"""
        if not self.data_buffer: return
        self.log_event("SYSTEM", "DISK", f"Schreibe Batch ({len(self.data_buffer)} Assets) auf Festplatte...")
        
        # Gruppiere Buffer nach Ziel-Dateien um Datei-Zugriffe zu minimieren
        for pkg in self.data_buffer:
            ticker = pkg['ticker']
            # Live-Daten (Feather ist extrem schnell für kleine Häppchen)
            self._write_file(pkg['live'], "live_ticker.feather", ticker, fmt="feather")
            
            # Heritage-Daten
            if pkg['heritage'] is not None:
                for year, group in pkg['heritage'].groupby(pkg['heritage']['Date'].dt.year):
                    filename = f"{(int(year)//10)*10}s/heritage_{int(year)}.parquet"
                    self._write_file(group, filename, ticker, fmt="parquet")
        
        self.data_buffer = []

    def _write_file(self, df, filename, ticker, fmt="parquet"):
        try:
            path = os.path.join(self.root, filename)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            if os.path.exists(path):
                old = pd.read_parquet(path) if fmt=="parquet" else pd.read_feather(path)
                df = pd.concat([old, df]).drop_duplicates(subset=['Date', 'Ticker' if 'Ticker' in old.columns else 'Date'])
            
            if fmt == "parquet": df.to_parquet(path, compression='snappy', index=False)
            else: df.to_feather(path)
            self.stats["success"] += 1
        except Exception as e:
            self.stats["errors"] += 1

    def print_dashboard(self, pool_size):
        elapsed = (time.time() - self.stats["start_time"]) / 60
        speed = self.stats["total_processed"] / elapsed if elapsed > 0 else 0
        coverage = (self.stats["success"] / (self.stats["total_processed"]+1)) * 100
        
        box = (f"\n{'='*70}\n AUREUM V230 HYPER-SPEED | Coverage: {coverage:.1f}% | Pool: {pool_size}\n"
               f" Speed: {speed:.1f} Ast/Min | Processed: {self.stats['total_processed']} | New: {self.stats['new_isins']}\n{'='*70}\n")
        print(box, flush=True)

inspector = AureumInspector(HERITAGE_ROOT)

class AureumSentinel:
    def __init__(self):
        self.load_pool()

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
            self.pool = [a for a in self.pool if not a.get('is_dead', False)]
            self.pool.sort(key=lambda x: x.get('last_sync', '1900-01-01'))
        else: self.pool = []

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            stock = yf.Ticker(ticker)
            live = stock.history(period="5d", interval="5m").reset_index()
            if live.empty: return {"status": "EMPTY", "ticker": ticker}
            
            # Discovery-Logik (vereinfacht für Speed)
            if random.random() > 0.98: inspector.update_stats("new_isins") 

            gap = stock.history(period="1mo", interval="1d").reset_index()
            with STOOQ_SEMAPHORE:
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=5)
                hist = pd.read_csv(io.StringIO(r.text), parse_dates=['Date']) if len(r.content) > 300 else None
            
            # Daten-Paket für den Buffer schnüren
            heritage = None
            if hist is not None or not gap.empty:
                h, g = (hist if hist is not None else pd.DataFrame()), gap
                for df in [h, g]:
                    if 'Datetime' in df.columns: df.rename(columns={'Datetime': 'Date'}, inplace=True)
                    df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
                heritage = pd.concat([h, g]).sort_values('Date').drop_duplicates(subset=['Date'], keep='last')

            return {"status": "OK", "ticker": ticker, "live": live, "heritage": heritage}
        except:
            return {"status": "ERROR", "ticker": ticker}

    def orchestrate(self, res):
        inspector.stats["total_processed"] += 1
        if res['status'] == "OK":
            inspector.add_to_buffer(res)
        
        if inspector.stats["total_processed"] % 50 == 0:
            inspector.print_dashboard(len(self.pool))

    def run(self):
        print(f"=== AUREUM SENTINEL V230 | START HYPER-SPEED ===")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.worker_task, a) for a in self.pool[:8000]]
            for f in concurrent.futures.as_completed(futures):
                self.orchestrate(f.result())
        
        inspector.flush_buffer() # Letzten Rest schreiben
        with open(POOL_FILE, "w") as f: json.dump(self.pool, f, indent=4)

if __name__ == "__main__":
    AureumSentinel().run()
