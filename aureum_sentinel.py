import pandas as pd
import yfinance as yf
import os
import json
import time
import multiprocessing
import shutil
import random
import sys
from datetime import datetime, timedelta
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (EISERNER STANDARD) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"
REPORT_FILE = "coverage_report.txt"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      
MAX_RUNTIME_SECONDS = 1200 # 20 Minuten Laufzeit pro Workflow-Start

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        self.start_time = datetime.now()
        self._sanitize_environment()
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self.is_initial_start = True
        self.known_assets = set()
        self._sync_and_audit()

    def _sanitize_environment(self):
        log("CLEANUP", "System-Bereinigung wird durchgeführt...")
        old_dir = "heritage_vault"
        if os.path.exists(old_dir):
            if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
            for item in os.listdir(old_dir):
                try: shutil.move(os.path.join(old_dir, item), os.path.join(HERITAGE_DIR, item))
                except: pass
            shutil.rmtree(old_dir)
        
        # Entferne alle Leichen aus V120/V129
        trash = ["live_buffer.parquet", "system.lock", "current_buffer.parquet", "dead_assets.json", "current_buffer.json"]
        for f in trash:
            if os.path.exists(f): os.remove(f)

    def _sync_and_audit(self):
        if os.path.exists(HERITAGE_DIR):
            for f in os.listdir(HERITAGE_DIR):
                if f.endswith(".parquet"):
                    try:
                        df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
                        self.known_assets.update(df['Ticker'].unique())
                    except: pass
        
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass
        log("HERITAGE", f"📊 Inventur: {len(self.known_assets)} Assets im Archiv.")

    def fetch_data(self, symbol):
        """Holt Deep History für neue Assets ODER nur den aktuellen Preis."""
        try:
            t = yf.Ticker(symbol)
            # Falls Asset neu: Gesamte Historie saugen
            if symbol not in self.known_assets or self.is_initial_start:
                hist = t.history(period="max", interval="1d").reset_index()
                if not hist.empty:
                    hist = hist.rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
                    hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
                    return symbol, t.fast_info.get('last_price'), hist[['Date', 'Price']]
            
            # Sonst: Nur aktueller Live-Tick
            return symbol, t.fast_info.get('last_price'), pd.DataFrame()
        except Exception as e:
            return symbol, None, pd.DataFrame()

    def atomic_write(self, df, path):
        tmp = path + ".tmp"
        try:
            df = df.dropna(subset=['Price'])
            df = df[df['Price'] > 0]
            if os.path.exists(path):
                old = pd.read_parquet(path)
                df = pd.concat([old, df]).drop_duplicates(subset=['Date', 'Ticker']).sort_values('Date')
            df.to_parquet(tmp, compression='zstd', index=False)
            os.replace(tmp, path)
        except:
            if os.path.exists(tmp): os.remove(tmp)

    def run_cycle(self):
        if not os.path.exists(POOL_FILE):
            log("ERROR", "Kein ISIN-Pool gefunden!")
            return

        with open(POOL_FILE, "r") as f: pool = json.load(f)
        log("STATUS", f"Puls-Check: {len(pool)} Assets aktiv.")
        
        ticker_batch, heritage_updates = [], []
        now = datetime.now().replace(microsecond=0)

        with ThreadPoolExecutor(max_workers=50) as exe:
            futures = [exe.submit(self.fetch_data, a['symbol']) for a in pool]
            for f in as_completed(futures):
                sym, price, hist = f.result()
                if price:
                    log("TICK", f"💓 {sym}: {price}")
                    ticker_batch.append({"Date": now, "Ticker": sym, "Price": price})
                    
                    if not hist.empty:
                        heritage_updates.append(hist.assign(Ticker=sym))
                        self.known_assets.add(sym)
                    
                    last = self.anchors.get(sym)
                    if self.is_initial_start or last is None or abs(price - last) / last >= ANCHOR_THRESHOLD:
                        self.anchors[sym] = price
                        heritage_updates.append(pd.DataFrame([{"Date": now, "Ticker": sym, "Price": price}]))

        if ticker_batch:
            pd.DataFrame(ticker_batch).to_feather(TICKER_FILE)
            if heritage_updates:
                df_all = pd.concat(heritage_updates)
                df_all['Decade'] = (pd.to_datetime(df_all['Date']).dt.year // 10) * 10
                for decade, chunk in df_all.groupby('Decade'):
                    path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                    self.atomic_write(chunk.drop(columns=['Decade']), path)
            
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            coverage = (len(self.known_assets) / len(pool)) * 100
            log("PROGRESS", f"✅ Zyklus beendet. Abdeckung: {len(self.known_assets)}/{len(pool)} ({coverage:.1f}%)")
            self.is_initial_start = False

if __name__ == "__main__":
    # Finder Start
    key = os.getenv("GEMINI_API_KEY")
    def finder(k):
        client = genai.Client(api_key=k)
        while True:
            try:
                r = client.models.generate_content(model="gemini-2.0-flash", contents="Nenne 50 Nasdaq Ticker. NUR JSON: ['AAPL', ...]")
                new_s = json.loads(r.text.strip().replace("```json", "").replace("```", ""))
                with open(POOL_FILE, "r") as f: p = json.load(f)
                exist = {x['symbol'] for x in p}
                added = [s.upper() for s in new_s if s.upper() not in exist]
                if added:
                    for s in added: p.append({"symbol": s, "added_at": datetime.now().isoformat()})
                    with open(POOL_FILE, "w") as f: json.dump(p, f, indent=4)
                    log("FINDER", f"✨ +{len(added)} neue Assets.")
                time.sleep(600)
            except: time.sleep(60)

    p_finder = multiprocessing.Process(target=finder, args=(key,))
    p_finder.start()

    try:
        sentinel = AureumSentinel()
        # Kontrollierte Laufzeit, um GitHub Timeouts zu verhindern
        while (datetime.now() - sentinel.start_time).seconds < MAX_RUNTIME_SECONDS:
            sentinel.run_cycle()
            time.sleep(PULSE_INTERVAL)
        log("SYSTEM", "Reguläres Laufzeitende erreicht. Speichere und schließe.")
    finally:
        p_finder.terminate()
