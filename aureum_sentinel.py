import pandas as pd
import yfinance as yf
import os
import json
import time
import multiprocessing
import shutil
import sys
import threading
from datetime import datetime
from google import genai
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (EISERNER STANDARD) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      
MAX_WORKERS = 25 # Skaliert auf maximale Stabilität vs. Speed
file_lock = threading.Lock()

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)
    sys.stdout.flush()

class AureumSentinel:
    def __init__(self):
        self._deep_cleanup()
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self.is_initial_start = True
        self.known_assets = set()
        self._sync_and_audit()

    def _deep_cleanup(self):
        # Löscht nur echte Altlasten, keine wertvollen Daten
        for f in ["current_buffer.json", "current_buffer.parquet", "system.lock"]:
            if os.path.exists(f): 
                try: os.remove(f)
                except: pass

    def _sync_and_audit(self):
        if os.path.exists(HERITAGE_DIR):
            for f in os.listdir(HERITAGE_DIR):
                if f.endswith(".parquet"):
                    try:
                        # Nur Header lesen für Speed
                        df = pd.read_parquet(os.path.join(HERITAGE_DIR, f), columns=['Ticker'])
                        self.known_assets.update(df['Ticker'].unique())
                    except Exception:
                        log("REPAIR", f"🔥 {f} korrupt! Lösche...")
                        os.remove(os.path.join(HERITAGE_DIR, f))
        
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass
        log("HERITAGE", f"📊 Inventur: {len(self.known_assets)} Assets.")

    def get_price_robust(self, ticker_obj):
        try:
            p = ticker_obj.fast_info.get('last_price')
            if p: return p
        except: pass
        # Fallback auf history für maximale Zuverlässigkeit
        try:
            p = ticker_obj.history(period="1d", interval="1m")['Close'].iloc[-1]
            if p: return p
        except: pass
        return None

    def process_asset(self, symbol):
        try:
            log("FETCH", f"📡 {symbol}...")
            t = yf.Ticker(symbol)
            price = self.get_price_robust(t)
            
            if not price:
                log("WARN", f"❌ {symbol}: Kein Preis.")
                return None

            log("TICK", f"💓 {symbol}: {price}")
            now = datetime.now().replace(microsecond=0)
            heritage_updates = []

            # Deep Scan Logik
            if symbol not in self.known_assets or self.is_initial_start:
                log("DEEP", f"🔍 {symbol} Historie...")
                hist = t.history(period="max", interval="1d").reset_index()
                if not hist.empty:
                    hist = hist.rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
                    hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
                    hist['Ticker'] = symbol
                    heritage_updates.append(hist[['Date', 'Ticker', 'Price']])

            # Anker Logik
            last = self.anchors.get(symbol)
            if self.is_initial_start or last is None or abs(price - last) / last >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = price
                heritage_updates.append(pd.DataFrame([{"Date": now, "Ticker": symbol, "Price": price}]))
                log("ANCHOR", f"⚓ {symbol} Anker.")

            # Geschütztes Speichern
            if heritage_updates:
                df_to_save = pd.concat(heritage_updates)
                with file_lock:
                    df_to_save['Decade'] = (pd.to_datetime(df_to_save['Date']).dt.year // 10) * 10
                    for decade, chunk in df_to_save.groupby('Decade'):
                        path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                        if os.path.exists(path):
                            old = pd.read_parquet(path)
                            chunk = pd.concat([old, chunk]).drop_duplicates(subset=['Date', 'Ticker'])
                        chunk.drop(columns=['Decade']).to_parquet(path, compression='zstd', index=False)
                log("SAVE", f"💾 {symbol} archiviert.")
                
            return {"Date": now, "Ticker": symbol, "Price": price}
        except Exception as e:
            log("ERROR", f"❌ {symbol}: {str(e)}")
            return None

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"🚀 Puls-Start ({MAX_WORKERS} Workers) für {len(pool)} Assets.")
        ticker_results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = [exe.submit(self.process_asset, a['symbol']) for a in pool]
            for f in futures:
                res = f.result()
                if res: ticker_results.append(res)

        if ticker_results:
            with file_lock:
                pd.DataFrame(ticker_results).to_feather(TICKER_FILE)
                with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            log("PROGRESS", "✅ Zyklus beendet.")
            self.is_initial_start = False

def finder_loop(api_key):
    if not api_key: return
    client = genai.Client(api_key=api_key)
    while True:
        try:
            log("FINDER", "🔎 Markt-Scan...")
            r = client.models.generate_content(model="gemini-2.0-flash", contents="Nenne 15 Nasdaq Ticker. NUR JSON: ['TSLA', ...]")
            new_list = json.loads(r.text.strip().replace("```json", "").replace("```", ""))
            with open(POOL_FILE, "r") as f: pool = json.load(f)
            existing = {a['symbol'] for a in pool}
            added = [s.upper() for s in new_list if s.upper() not in existing]
            if added:
                for s in added: pool.append({"symbol": s, "added_at": datetime.now().isoformat()})
                with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
                log("FINDER", f"✨ Pool: {len(pool)}.")
            time.sleep(3600)
        except:
            time.sleep(900)

if __name__ == "__main__":
    key = os.getenv("GEMINI_API_KEY")
    p = multiprocessing.Process(target=finder_loop, args=(key,))
    p.start()
    try:
        sentinel = AureumSentinel()
        while True:
            sentinel.run_cycle()
            time.sleep(PULSE_INTERVAL)
    finally:
        p.terminate()
