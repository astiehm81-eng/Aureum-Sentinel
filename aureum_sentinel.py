import pandas as pd
import yfinance as yf
import os
import json
import time
import multiprocessing
import shutil
import sys
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

def log(tag, msg):
    """Erzwingt Live-Logs in GitHub Actions."""
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)
    sys.stdout.flush()

class AureumSentinel:
    def __init__(self):
        self._sanitize_environment()
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self.is_initial_start = True
        self.known_assets = set()
        self._sync_and_audit()

    def _sanitize_environment(self):
        log("CLEANUP", "🧹 Bereinigung läuft...")
        old_dir = "heritage_vault"
        if os.path.exists(old_dir):
            if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
            for item in os.listdir(old_dir):
                try: shutil.move(os.path.join(old_dir, item), os.path.join(HERITAGE_DIR, item))
                except: pass
            shutil.rmtree(old_dir)
        # Lösche Altlasten aus V120/V129
        for f in ["live_buffer.parquet", "system.lock", "current_buffer.parquet"]:
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
        log("HERITAGE", f"📊 Inventur: {len(self.known_assets)} Assets mit Historie.")

    def fetch_and_save_immediate(self, symbol):
        """Holt Daten und verarbeitet sie SOFORT einzeln (kein Batch-Warten)."""
        try:
            t = yf.Ticker(symbol)
            # Timeout-Schutz: Nutze fast_info für Geschwindigkeit
            price = t.fast_info.get('last_price')
            if not price: return None
            
            log("TICK", f"💓 {symbol}: {price}")
            
            now = datetime.now().replace(microsecond=0)
            heritage_data = []

            # 1. Deep History Check
            if symbol not in self.known_assets or self.is_initial_start:
                hist = t.history(period="max", interval="1d").reset_index()
                if not hist.empty:
                    hist = hist.rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
                    hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
                    hist['Ticker'] = symbol
                    heritage_data.append(hist[['Date', 'Ticker', 'Price']])
            
            # 2. Anker-Logik
            last = self.anchors.get(symbol)
            if self.is_initial_start or last is None or abs(price - last) / last >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = price
                live_entry = pd.DataFrame([{"Date": now, "Ticker": symbol, "Price": price}])
                heritage_data.append(live_entry)

            # 3. Sofortiges Schreiben der Heritage-Updates für dieses Asset
            if heritage_data:
                df_up = pd.concat(heritage_data)
                df_up['Decade'] = (pd.to_datetime(df_up['Date']).dt.year // 10) * 10
                for decade, chunk in df_up.groupby('Decade'):
                    path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                    # Atomares Update pro Datei
                    if os.path.exists(path):
                        old = pd.read_parquet(path)
                        chunk = pd.concat([old, chunk]).drop_duplicates(subset=['Date', 'Ticker'])
                    chunk.drop(columns=['Decade']).to_parquet(path, compression='zstd', index=False)
                
            return {"Date": now, "Ticker": symbol, "Price": price}
        except:
            return None

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"📡 Puls-Check startet für {len(pool)} Assets.")
        ticker_results = []

        # Jedes Asset wird einzeln abgearbeitet, damit wir sofort Logs sehen
        with ThreadPoolExecutor(max_workers=20) as exe:
            futures = [exe.submit(self.fetch_and_save_immediate, a['symbol']) for a in pool]
            for f in futures:
                res = f.result()
                if res: ticker_results.append(res)

        if ticker_results:
            pd.DataFrame(ticker_results).to_feather(TICKER_FILE)
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            
            cov = (len(self.known_assets) / len(pool)) * 100
            log("PROGRESS", f"✅ Zyklus beendet. Abdeckung: {cov:.1f}%")
            self.is_initial_start = False

def finder_loop(api_key):
    if not api_key: return
    client = genai.Client(api_key=api_key)
    while True:
        try:
            log("FINDER", "🔎 Gemini-Scan läuft...")
            # Reduziere Komplexität für Free-Tier Quota
            r = client.models.generate_content(model="gemini-2.0-flash", contents="Nenne 20 Nasdaq Ticker. NUR JSON: ['TSLA', ...]")
            new_list = json.loads(r.text.strip().replace("```json", "").replace("```", ""))
            
            with open(POOL_FILE, "r") as f: pool = json.load(f)
            existing = {a['symbol'] for a in pool}
            added = [s.upper() for s in new_list if s.upper() not in existing]
            
            if added:
                for s in added: pool.append({"symbol": s, "added_at": datetime.now().isoformat()})
                with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
                log("FINDER", f"✨ +{len(added)} Assets.")
            
            time.sleep(3600) # 1 Stunde Pause (Quota-Schutz!)
        except Exception as e:
            log("FINDER", "💤 Quota-Pause (10 Min)...")
            time.sleep(600)

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
