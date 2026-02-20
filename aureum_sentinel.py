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

# Strategie-Parameter
ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      
TOTAL_RUNTIME = 900       
MAX_WORKERS = 100          

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

# --- ENGINE 1: DER KI-FINDER (Optimiert für Free-Tier) ---
def run_finder_parallel(api_key):
    try:
        client = genai.Client(api_key=api_key)
        start_time = time.time()
        segments = ["Global Mega-Caps", "Nasdaq 100", "DAX 40", "S&P 500 Tech", "Crypto Top 50"]
        
        while (time.time() - start_time) < (TOTAL_RUNTIME - 120):
            seg = segments[int(time.time() / 120) % len(segments)]
            log("FINDER", f"KI-Suche (Free-Tier): {seg}...")
            
            # Kleinere Menge (50 statt 250), um 'Resource Exhausted' zu vermeiden
            prompt = f"Gib mir 50 Yahoo Finance Tickersymbole für {seg}. Antwort NUR als JSON-Liste von Strings: ['AAPL', 'MSFT', ...]"
            
            try:
                response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
                raw_text = response.text.strip().replace("```json", "").replace("```", "")
                
                # Flexiblere Logik für verschiedene Antwortformate
                symbols_list = json.loads(raw_text)
                if isinstance(symbols_list, list):
                    if os.path.exists(POOL_FILE):
                        with open(POOL_FILE, "r") as f: pool = json.load(f)
                    else: pool = []

                    existing = {a['symbol'] for a in pool}
                    added = 0
                    for sym in symbols_list:
                        s = str(sym).upper() if isinstance(sym, str) else str(sym.get('symbol', '')).upper()
                        if s and s not in existing:
                            pool.append({"symbol": s, "added_at": datetime.now().isoformat()})
                            existing.add(s)
                            added += 1
                    
                    if added > 0:
                        with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
                        log("FINDER", f"🚀 Pool +{added} Assets (Gesamt: {len(pool)})")
                
                # Wichtig: Lange Pause im Free Tier
                time.sleep(120) 

            except Exception as e:
                if "429" in str(e):
                    log("FINDER", "⏳ Limit erreicht (429). Warte 180s...")
                    time.sleep(180)
                else:
                    log("FINDER", f"⚠️ Fehler: {str(e)[:50]}")
                    time.sleep(60)
    except Exception as e:
        log("FINDER", f"❌ API-Kritisch: {str(e)[:50]}")

# --- ENGINE 2: DER SENTINEL ---
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
        log("SENTINEL", "Monitoring aktiv.")
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
                    log("SENTINEL", f"💾 {len(results)} Anker gesichert.")
            
            elapsed = time.time() - loop_start
            wait = max(10, PULSE_INTERVAL - elapsed)
            if (time.time() - start_run) + PULSE_INTERVAL > TOTAL_RUNTIME: break
            time.sleep(wait)

if __name__ == "__main__":
    k = os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()
    log("SYSTEM", f"=== START V118 (Stabil) === Key: {'✅' if len(k)>10 else '❌'}")
    if len(k) < 10: sys.exit(1)

    finder_proc = multiprocessing.Process(target=run_finder_parallel, args=(k,))
    finder_proc.start()
    try:
        AureumSentinel().run_cycle()
    finally:
        finder_proc.terminate()
        finder_proc.join()
        log("SYSTEM", "Zyklus beendet.")
