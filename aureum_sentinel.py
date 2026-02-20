import pandas as pd
import yfinance as yf
import os
import json
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (EISERNER STANDARD) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"
AUDIT_FILE = "heritage_audit.txt"

ANCHOR_THRESHOLD = 0.0005  # 0,05% Anker
MAX_WORKERS = 25 
file_lock = threading.Lock()

# Liste der zu bereinigenden Altdaten (Leichen)
LEGACY_FILES = [
    "current_buffer.json", "sentinel_data.txt", "ticker_mapping.json", 
    "missing_assets.json", "dead_assets.json", "coverage_report.txt"
]

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self.known_assets = set()
        self.repair_log = []
        self._cleanup_legacy()
        self._sync_and_audit()

    def _cleanup_legacy(self):
        """Entfernt alte Dateien, die nicht mehr benötigt werden."""
        for f in LEGACY_FILES:
            if os.path.exists(f):
                try:
                    os.remove(f)
                    log("CLEANUP", f"🗑️ Alte Datei gelöscht: {f}")
                except: pass

    def _sync_and_audit(self):
        """Self-Healing: Scannt Heritage auf Defekte und bereinigt Korruption."""
        if os.path.exists(HERITAGE_DIR):
            for f in os.listdir(HERITAGE_DIR):
                if f.endswith(".parquet"):
                    path = os.path.join(HERITAGE_DIR, f)
                    try:
                        df = pd.read_parquet(path, columns=['Ticker'])
                        self.known_assets.update(df['Ticker'].unique())
                    except Exception as e:
                        log("REPAIR", f"🔥 Datei {f} korrupt! Entferne... ({e})")
                        self.repair_log.append(f"Auto-Repair: {f} gelöscht am {datetime.now()}")
                        os.remove(path)
        
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except:
                self.anchors = {}

    def get_price_robust(self, ticker_obj):
        try:
            p = ticker_obj.fast_info.get('last_price')
            if p: return p
        except: pass
        try:
            p = ticker_obj.history(period="1d", interval="1m")['Close'].iloc[-1]
            if p: return p
        except: pass
        return None

    def process_asset(self, symbol):
        try:
            t = yf.Ticker(symbol)
            price = self.get_price_robust(t)
            if not price: return None

            now = datetime.now().replace(microsecond=0)
            heritage_updates = []

            # Deep Scan bei fehlenden Daten
            if symbol not in self.known_assets:
                log("DEEP", f"🔍 {symbol} Erstaufnahme...")
                hist = t.history(period="max", interval="1d").reset_index()
                if not hist.empty:
                    hist = hist.rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
                    hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
                    hist['Ticker'] = symbol
                    heritage_updates.append(hist[['Date', 'Ticker', 'Price']])

            # Anker-Logik
            last = self.anchors.get(symbol)
            if last is None or abs(price - last) / last >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = price
                heritage_updates.append(pd.DataFrame([{"Date": now, "Ticker": symbol, "Price": price}]))
                log("TICK", f"⚓ {symbol}: {price}")

            if heritage_updates:
                self.safe_save(pd.concat(heritage_updates))
                
            return {"Date": now, "Ticker": symbol, "Price": price}
        except Exception as e:
            log("ERROR", f"❌ {symbol}: {str(e)}")
            return None

    def safe_save(self, df_up):
        with file_lock:
            df_up['Decade'] = (pd.to_datetime(df_up['Date']).dt.year // 10) * 10
            for decade, chunk in df_up.groupby('Decade'):
                path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                try:
                    if os.path.exists(path):
                        old = pd.read_parquet(path)
                        chunk = pd.concat([old, chunk]).drop_duplicates(subset=['Date', 'Ticker'])
                    chunk.drop(columns=['Decade']).to_parquet(path, compression='zstd', index=False)
                except Exception as e:
                    if os.path.exists(path): os.remove(path)

    def generate_audit_report(self):
        with open(AUDIT_FILE, "w") as f:
            f.write(f"AUREUM SENTINEL AUDIT REPORT - {datetime.now()}\n")
            f.write("="*40 + "\n")
            f.write(f"Gesamt-Assets im Archiv: {len(self.known_assets)}\n")
            if self.repair_log:
                f.write("\nREPARATUR-LOG:\n" + "\n".join(self.repair_log) + "\n")
            f.write("\nDATEI-STATUS (HERITAGE):\n")
            for file in sorted(os.listdir(HERITAGE_DIR)):
                if file.endswith(".parquet"):
                    size = os.path.getsize(os.path.join(HERITAGE_DIR, file)) / (1024 * 1024)
                    f.write(f"- {file}: {size:.2f} MB\n")

    def run_pulse(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"🚀 Puls-Start für {len(pool)} Assets.")
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = [exe.submit(self.process_asset, a['symbol']) for a in pool]
            for fut in futures:
                res = fut.result()
                if res: results.append(res)

        if results:
            with file_lock:
                pd.DataFrame(results).to_feather(TICKER_FILE)
                with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            self.generate_audit_report()
            log("PROGRESS", "✅ Puls abgeschlossen.")

if __name__ == "__main__":
    AureumSentinel().run_pulse()
