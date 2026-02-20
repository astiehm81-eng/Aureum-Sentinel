import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import multiprocessing
from datetime import datetime
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
BUFFER_FILE = os.path.join(HERITAGE_DIR, "live_buffer.parquet")
ANCHOR_FILE = "anchors_memory.json"
REPORT_FILE = "coverage_report.txt"

# Strategie-Parameter
ANCHOR_THRESHOLD = 0.0005  # 0,05% Anker
PULSE_INTERVAL = 300       # Alle 5 Min
TOTAL_RUNTIME = 900        # 15 Min Laufzeit
MAX_WORKERS = 100          

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

# --- ENGINE 1: DER KI-FINDER (Expansion) ---
def run_finder_parallel(api_key):
    if not api_key:
        log("FINDER", "❌ ERROR: Kein API-Key im Subprozess.")
        return
    try:
        client = genai.Client(api_key=api_key)
        start_time = time.time()
        # Segmente für die Rotation
        segments = ["Global Mega-Caps", "Nasdaq 100", "DAX 40", "S&P 500 Tech", "Crypto Top 100"]
        
        while (time.time() - start_time) < (TOTAL_RUNTIME - 60):
            seg = segments[int(time.time() / 60) % len(segments)]
            log("FINDER", f"KI-Suche läuft: {seg}...")
            
            prompt = f"Gib mir 250 Yahoo Finance Tickersymbole für {seg}. NUR JSON-Array: [{{'symbol': 'AAPL'}}, ...]"
            response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            
            try:
                raw_text = response.text.strip().replace("```json", "").replace("```", "")
                new_data = json.loads(raw_text)
                
                if os.path.exists(POOL_FILE):
                    with open(POOL_FILE, "r") as f: pool = json.load(f)
                else: pool = []

                existing = {a['symbol'] for a in pool}
                added = 0
                for item in new_data:
                    sym = str(item.get('symbol', '')).upper()
                    if sym and sym not in existing:
                        pool.append({"symbol": sym, "added_at": datetime.now().isoformat()})
                        existing.add(sym)
                        added += 1
                
                if added > 0:
                    with open(POOL_FILE, "w") as f:
                        json.dump(pool, f, indent=4)
                    log("FINDER", f"🚀 Pool-Wachstum: +{added} Assets (Gesamt: {len(pool)})")
            except:
                log("FINDER", "⚠️ JSON-Parsing fehlgeschlagen.")
            
            time.sleep(60) # Schont das API-Limit
    except Exception as e:
        log("FINDER", f"❌ API-Fehler: {str(e)[:50]}")

# --- ENGINE 2: DER SENTINEL (Monitoring) ---
class AureumSentinel:
    def __init__(self):
        self.anchors = {}
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass

    def fetch_price(self, symbol):
        try:
            t = yf.Ticker(symbol)
            price = t.fast_info.get('last_price')
            if not price:
                df = t.history(period="1d")
                if not df.empty: price = df['Close'].iloc[-1]
            
            if price:
                price = round(float(price), 4)
                last = self.anchors.get(symbol)
                if last is None or abs(price - last) / last >= ANCHOR_THRESHOLD:
                    self.anchors[symbol] = price
                    return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": price}
        except: pass
        return None

    def run_cycle(self):
        log("SENTINEL", "Monitoring gestartet.")
        start_run = time.time()
        while (time.time() - start_run) < TOTAL_RUNTIME:
            loop_start = time.time()
            if os.path.exists(POOL_FILE):
                with open(POOL_FILE, "r") as f: pool = json.load(f)
            else: pool = []

            if pool:
                log("SENTINEL", f"💓 Puls-Check ({len(pool)} Assets)...")
                results = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
                    futures = [exe.submit(self.fetch_price, a['symbol']) for a in pool]
                    for f in as_completed(futures):
                        r = f.result()
                        if r: results.append(r)

                if results:
                    df_new = pd.DataFrame(results)
                    if os.path.exists(BUFFER_FILE):
                        try:
                            df_old = pd.read_parquet(BUFFER_FILE)
                            df_new = pd.concat([df_old, df_new]).drop_duplicates(subset=['Date', 'Ticker'])
                        except: pass
                    df_new.to_parquet(BUFFER_FILE, index=False)
                    with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
                    log("SENTINEL", f"💾 {len(results)} Anker-Events gesichert.")

            wait = max(10, PULSE_INTERVAL - (time.time() - loop_start))
            if (time.time() - start_run) + PULSE_INTERVAL > TOTAL_RUNTIME: break
            time.sleep(wait)

if __name__ == "__main__":
    # Key-Diagnose beim Start
    k1 = os.getenv("GEMINI_API_KEY", "").strip()
    k2 = os.getenv("GOOGLE_API_KEY", "").strip()
    final_key = k1 if len(k1) > 10 else k2

    log("SYSTEM", "=== DIAGNOSE V117 ===")
    log("SYSTEM", f"Key gefunden: {'✅' if len(final_key) > 10 else '❌'}")
    
    if len(final_key) < 10:
        log("SYSTEM", "❌ FATAL: Kein gültiger API-Key in den Secrets gefunden.")
        sys.exit(1)

    finder_proc = multiprocessing.Process(target=run_finder_parallel, args=(final_key,))
    finder_proc.start()
    
    try:
        AureumSentinel().run_cycle()
    finally:
        finder_proc.terminate()
        finder_proc.join()
        log("SYSTEM", "Zyklus sauber beendet.")
