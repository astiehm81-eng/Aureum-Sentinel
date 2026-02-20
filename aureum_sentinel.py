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

# --- KONFIGURATION ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      

def log(tag, msg):
    """Erzwingt sofortigen Flush für GitHub Actions Sichtbarkeit."""
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
        log("CLEANUP", "🧹 Bereinigungs-Modus aktiv...")
        if os.path.exists("heritage_vault"):
            for item in os.listdir("heritage_vault"):
                shutil.move(os.path.join("heritage_vault", item), os.path.join(HERITAGE_DIR, item))
            shutil.rmtree("heritage_vault")

    def _sync_and_audit(self):
        if os.path.exists(HERITAGE_DIR):
            for f in os.listdir(HERITAGE_DIR):
                if f.endswith(".parquet"):
                    try:
                        df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
                        self.known_assets.update(df['Ticker'].unique())
                    except: pass
        if os.path.exists(ANCHOR_FILE):
            with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
        log("HERITAGE", f"📊 Bestand: {len(self.known_assets)} Assets im Archiv.")

    def process_asset(self, symbol):
        """Einzelschritt pro Asset mit detailliertem Live-Logging."""
        try:
            log("FETCH", f"📡 Rufe Daten ab für: {symbol}...")
            t = yf.Ticker(symbol)
            price = t.fast_info.get('last_price')
            
            if not price:
                log("WARN", f"⚠️ Kein Preis für {symbol} empfangen.")
                return None

            log("TICK", f"💓 {symbol}: {price} (Anker-Check läuft...)")
            
            now = datetime.now().replace(microsecond=0)
            heritage_data = []

            # 1. Historie-Erfassung (Deep Scan)
            if symbol not in self.known_assets or self.is_initial_start:
                log("DEEP", f"🔍 Lade Max-Historie für {symbol}...")
                hist = t.history(period="max", interval="1d").reset_index()
                if not hist.empty:
                    hist = hist.rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
                    hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
                    hist['Ticker'] = symbol
                    heritage_data.append(hist[['Date', 'Ticker', 'Price']])
                    log("DEEP", f"✅ Historie für {symbol} geladen ({len(hist)} Zeilen).")

            # 2. Anker-Logik (Immer loggen, ob gespeichert wird)
            last = self.anchors.get(symbol)
            if self.is_initial_start or last is None or abs(price - last) / last >= ANCHOR_THRESHOLD:
                diff = 0 if last is None else abs(price - last) / last * 100
                log("ANCHOR", f"⚓ Neuer Anker für {symbol} bei {price} (Abweichung: {diff:.3f}%).")
                self.anchors[symbol] = price
                heritage_data.append(pd.DataFrame([{"Date": now, "Ticker": symbol, "Price": price}]))
            else:
                log("SKIP", f"▫️ {symbol} stabil ({price}), kein neuer Anker nötig.")

            # 3. Sofortiges physisches Speichern
            if heritage_data:
                df_up = pd.concat(heritage_data)
                df_up['Decade'] = (pd.to_datetime(df_up['Date']).dt.year // 10) * 10
                for decade, chunk in df_up.groupby('Decade'):
                    path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                    if os.path.exists(path):
                        old = pd.read_parquet(path)
                        chunk = pd.concat([old, chunk]).drop_duplicates(subset=['Date', 'Ticker'])
                    chunk.drop(columns=['Decade']).to_parquet(path, compression='zstd', index=False)
                log("SAVE", f"💾 {symbol} erfolgreich in Heritage archiviert.")
                
            return {"Date": now, "Ticker": symbol, "Price": price}
        except Exception as e:
            log("ERROR", f"❌ Fehler bei {symbol}: {str(e)}")
            return None

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"🚀 Puls-Start: Verarbeite {len(pool)} Assets parallel...")
        ticker_results = []

        # workers auf 10 reduziert für stabilere Log-Reihenfolge
        with ThreadPoolExecutor(max_workers=10) as exe:
            futures = [exe.submit(self.process_asset, a['symbol']) for a in pool]
            for f in futures:
                res = f.result()
                if res: ticker_results.append(res)

        if ticker_results:
            pd.DataFrame(ticker_results).to_feather(TICKER_FILE)
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            log("PROGRESS", f"🏁 Zyklus beendet. {len(ticker_results)} Assets verarbeitet.")
            self.is_initial_start = False

def finder_loop(api_key):
    if not api_key: return
    client = genai.Client(api_key=api_key)
    while True:
        try:
            log("FINDER", "🔎 Gemini-Suche nach neuen ISINs...")
            r = client.models.generate_content(model="gemini-2.0-flash", contents="Nenne 15 Nasdaq Ticker. NUR JSON: ['TSLA', ...]")
            new_list = json.loads(r.text.strip().replace("```json", "").replace("```", ""))
            with open(POOL_FILE, "r") as f: pool = json.load(f)
            existing = {a['symbol'] for a in pool}
            added = [s.upper() for s in new_list if s.upper() not in existing]
            if added:
                for s in added: pool.append({"symbol": s, "added_at": datetime.now().isoformat()})
                with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
                log("FINDER", f"✨ Pool erweitert: {len(pool)} Assets gesamt.")
            time.sleep(3600)
        except:
            log("FINDER", "💤 Quota-Limit erreicht. Warte 15 Min...")
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
