import pandas as pd
import yfinance as yf
import os
import json
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (EISERNER STANDARD V149) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"
AUDIT_FILE = "heritage_audit.txt"

ANCHOR_THRESHOLD = 0.0005  # 0,05%
LOOKBACK_MINUTES = 60      # Sicherheitspuffer für lückenlose Daten
MAX_WORKERS = 25 
file_lock = threading.Lock()

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self.known_assets = set()
        self._sync_and_audit()

    def _sync_and_audit(self):
        """Self-Healing & Inventory."""
        if os.path.exists(HERITAGE_DIR):
            for f in os.listdir(HERITAGE_DIR):
                if f.endswith(".parquet"):
                    path = os.path.join(HERITAGE_DIR, f)
                    try:
                        df = pd.read_parquet(path, columns=['Ticker'])
                        self.known_assets.update(df['Ticker'].unique())
                    except: os.remove(path)
        
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: self.anchors = {}

    def process_asset(self, symbol):
        try:
            t = yf.Ticker(symbol)
            # Hole die letzte Stunde im 1-Minuten-Takt
            df_recent = t.history(period="1d", interval="1m").tail(LOOKBACK_MINUTES).reset_index()
            if df_recent.empty: return None

            df_recent = df_recent.rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
            df_recent['Date'] = pd.to_datetime(df_recent['Date'], utc=True).dt.tz_localize(None)
            df_recent['Ticker'] = symbol
            
            updates = []
            current_price = df_recent['Price'].iloc[-1]

            # 1. Deep Scan bei Erstaufnahme (Tageswerte seit Max)
            if symbol not in self.known_assets:
                log("DEEP", f"🔍 {symbol}...")
                hist = t.history(period="max", interval="1d").reset_index()
                if not hist.empty:
                    hist = hist.rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
                    hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
                    hist['Ticker'] = symbol
                    updates.append(hist[['Date', 'Ticker', 'Price']])
                    self.known_assets.add(symbol)

            # 2. Anker-Logik & Gap-Filling
            last_anchor = self.anchors.get(symbol)
            if last_anchor is None or abs(current_price - last_anchor) / last_anchor >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = current_price
                # Wir speichern die gesamte letzte Stunde für maximale Datenqualität
                updates.append(df_recent[['Date', 'Ticker', 'Price']])
                log("TICK", f"⚓ {symbol}: {current_price} (+{len(df_recent)} Min-Ticks)")

            if updates:
                self.safe_save(pd.concat(updates))
            
            return {"Date": df_recent['Date'].iloc[-1], "Ticker": symbol, "Price": current_price}
        except:
            return None

    def safe_save(self, df):
        """Speichert Daten nach Jahrzehnten sortiert."""
        with file_lock:
            df['Decade'] = (pd.to_datetime(df['Date']).dt.year // 10) * 10
            for decade, chunk in df.groupby('Decade'):
                path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                if os.path.exists(path):
                    try:
                        old = pd.read_parquet(path)
                        chunk = pd.concat([old, chunk]).drop_duplicates(subset=['Date', 'Ticker'])
                    except: pass
                chunk.drop(columns=['Decade']).to_parquet(path, index=False, compression='zstd')

    def generate_audit(self):
        """Erstellt einen Statusbericht für das Repo."""
        with open(AUDIT_FILE, "w") as f:
            f.write(f"AUREUM SENTINEL AUDIT - {datetime.now()}\n")
            f.write(f"Assets im System: {len(self.known_assets)}\n")
            for file in sorted(os.listdir(HERITAGE_DIR)):
                if file.endswith(".parquet"):
                    size = os.path.getsize(os.path.join(HERITAGE_DIR, file)) / (1024*1024)
                    f.write(f"- {file}: {size:.2f} MB\n")

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"🚀 Gap-Filler Puls (60m) für {len(pool)} Assets.")
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = [exe.submit(self.process_asset, a['symbol']) for a in pool]
            for f in futures:
                res = f.result()
                if res: results.append(res)
        
        if results:
            pd.DataFrame(results).to_feather(TICKER_FILE)
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            self.generate_audit()
            log("PROGRESS", "✅ Daten konsolidiert & bereit für Sync.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
