import pandas as pd
import yfinance as yf
import os
import json
import time
import multiprocessing
import shutil
import random
import sys
from datetime import datetime
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION (EISERNER STANDARD) ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      

def log(tag, msg):
    """Erzwingt sofortiges Schreiben ins Log, damit GitHub nichts verschluckt."""
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
        log("CLEANUP", "🧹 Räume alte Strukturen auf...")
        old_dir = "heritage_vault"
        if os.path.exists(old_dir):
            log("CLEANUP", f"📦 Migriere {old_dir} -> {HERITAGE_DIR}")
            if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
            for item in os.listdir(old_dir):
                shutil.move(os.path.join(old_dir, item), os.path.join(HERITAGE_DIR, item))
            shutil.rmtree(old_dir)
        
        # Lösche alles, was in deinen Screenshots als "Leiche" zu sehen war
        trash = ["live_buffer.parquet", "system.lock", "current_buffer.parquet", 
                 "current_buffer.json", "dead_assets.json", "ticker_mapping.json"]
        for f in trash:
            if os.path.exists(f): os.remove(f)

    def _sync_and_audit(self):
        # Bestandsaufnahme
        if os.path.exists(HERITAGE_DIR):
            for f in os.listdir(HERITAGE_DIR):
                if f.endswith(".parquet"):
                    try:
                        df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
                        self.known_assets.update(df['Ticker'].unique())
                    except: pass
        
        if os.path.exists(ANCHOR_FILE):
            with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
        
        log("HERITAGE", f"📊 Datenbasis: {len(self.known_assets)} Assets mit Historie gesichert.")

    def fetch_deep_data(self, symbol):
        """Holt Deep History oder Live-Tick."""
        try:
            t = yf.Ticker(symbol)
            # Wichtig: Deep Scan nur wenn wirklich neu
            if symbol not in self.known_assets or self.is_initial_start:
                hist = t.history(period="max", interval="1d").reset_index()
                if not hist.empty:
                    hist = hist.rename(columns={'Date':'Date','Datetime':'Date','Close':'Price'})
                    hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
                    return symbol, t.fast_info.get('last_price'), hist[['Date', 'Price']]
            
            return symbol, t.fast_info.get('last_price'), pd.DataFrame()
        except:
            return symbol, None, pd.DataFrame()

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("STATUS", f"📡 Puls-Check: Starte Abfrage für {len(pool)} Assets...")
        ticker_batch, heritage_updates = [], []
        now = datetime.now().replace(microsecond=0)

        # Höhere Parallelität, um "Blockieren" zu vermeiden
        with ThreadPoolExecutor(max_workers=50) as exe:
            futures = [exe.submit(self.fetch_deep_data, a['symbol']) for a in pool]
            for f in as_completed(futures):
                sym, price, hist = f.result()
                if price:
                    # Direkter Herzschlag-Log
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
                    # Hier speichern wir mit ZSTD Kompression
                    chunk.drop(columns=['Decade']).to_parquet(path, compression='zstd', index=False)
            
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            coverage = (len(self.known_assets) / len(pool)) * 100
            log("PROGRESS", f"✅ Zyklus beendet. Abdeckung: {len(self.known_assets)}/{len(pool)} ({coverage:.1f}%)")
            self.is_initial_start = False

def finder_loop(api_key):
    """Eigener Prozess für Gemini, um den Hauptpuls nicht zu bremsen."""
    if not api_key: 
        log("FINDER", "⚠️ Kein API-Key gefunden. Finder deaktiviert.")
        return
    client = genai.Client(api_key=api_key)
    while True:
        try:
            log("FINDER", "🔎 Gemini scannt Markt nach neuen Assets...")
            r = client.models.generate_content(model="gemini-2.0-flash", contents="Nenne 50 Nasdaq ISINs/Ticker. NUR JSON: ['TSLA', ...]")
            new_list = json.loads(r.text.strip().replace("```json", "").replace("```", ""))
            
            with open(POOL_FILE, "r") as f: pool = json.load(f)
            existing = {a['symbol'] for a in pool}
            added = [s.upper() for s in new_list if s.upper() not in existing]
            
            log("FINDER", f"📊 Ergebnis: {len(new_list)} gefunden, davon {len(added)} neu für den Pool.")
            
            if added:
                for s in added: pool.append({"symbol": s, "added_at": datetime.now().isoformat()})
                with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
                log("FINDER", f"✨ Pool erweitert auf {len(pool)} Assets.")
            
            time.sleep(600)
        except Exception as e:
            log("FINDER", f"❌ Fehler: {e}")
            time.sleep(60)

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
