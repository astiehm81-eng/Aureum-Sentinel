import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import multiprocessing
from datetime import datetime, timedelta
from google import genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage_vault"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.csv") # Schneller Ticker-Stream
BUFFER_FILE = os.path.join(HERITAGE_DIR, "live_buffer.parquet") # Heritage Anker-Vault
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005 
PULSE_INTERVAL = 300      
TOTAL_RUNTIME = 1100      
MAX_PARALLEL = 60

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

# --- HYBRID DATA LOGIC ---
def fetch_market_data(symbol):
    """Holt Yahoo-Daten für die letzte Woche und den aktuellen Preis."""
    try:
        t = yf.Ticker(symbol)
        # Aktueller Kurs für den Ticker
        price = t.fast_info.get('last_price')
        # Historie für die Verheiratung (letzte 7 Tage)
        hist = t.history(period="7d", interval="5m")
        return symbol, price, hist
    except:
        return symbol, None, None

# --- ENGINE 1: DER UNABHÄNGIGE TICKER & ANKER ---
class AureumSentinel:
    def __init__(self):
        self.anchors = {}
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass

    def process_pulse(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        log("SENTINEL", f"Puls-Check für {len(pool)} Assets...")
        ticker_data = []
        anchor_data = []
        now = datetime.now().replace(microsecond=0)

        with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as exe:
            futures = [exe.submit(fetch_market_data, a['symbol']) for a in pool]
            for f in as_completed(futures):
                sym, price, hist = f.result()
                if price:
                    # 1. TICKER-LOGIK (Schreibt jeden Preis)
                    ticker_data.append({"Timestamp": now, "Ticker": sym, "Price": price})
                    
                    # 2. ANKER-LOGIK (Heritage)
                    last_a = self.anchors.get(sym)
                    if last_a is None or abs(price - last_a) / last_a >= ANCHOR_THRESHOLD:
                        self.anchors[sym] = price
                        anchor_data.append({"Date": now, "Ticker": sym, "Price": price})

        # Speichern des Tickers (CSV für schnelle Lesbarkeit)
        if ticker_data:
            df_ticker = pd.DataFrame(ticker_data)
            df_ticker.to_csv(TICKER_FILE, mode='a', header=not os.path.exists(TICKER_FILE), index=False)
            log("TICKER", f"⚡ {len(ticker_data)} Ticker-Updates gestreamt.")

        # Speichern der Anker (Parquet für Heritage Vault)
        if anchor_data:
            df_anchor = pd.DataFrame(anchor_data)
            if os.path.exists(BUFFER_FILE):
                try:
                    df_old = pd.read_parquet(BUFFER_FILE)
                    df_anchor = pd.concat([df_old, df_anchor])
                except: pass
            df_anchor.to_parquet(BUFFER_FILE, index=False)
            with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)
            log("HERITAGE", f"🏛️ {len(anchor_data)} neue Anker-Punkte gesetzt.")

    def run(self):
        start = time.time()
        while (time.time() - start) < TOTAL_RUNTIME:
            cycle_start = time.time()
            self.process_pulse()
            wait = max(10, PULSE_INTERVAL - (time.time() - cycle_start))
            log("SYSTEM", f"Nächster Puls in {int(wait)}s.")
            time.sleep(wait)

# --- ENGINE 2: KI-EXPANSION (Hintergrund) ---
def run_expansion(key):
    client = genai.Client(api_key=key)
    while True:
        try:
            prompt = "Gib mir 50 DAX/Nasdaq Tickersymbole. Antwort NUR JSON-Liste: ['AAPL', ...]"
            resp = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            new_syms = json.loads(resp.text.strip().replace("```json", "").replace("```", ""))
            
            with open(POOL_FILE, "r") as f: pool = json.load(f)
            existing = {a['symbol'] for a in pool}
            added = 0
            for s in new_syms:
                if s.upper() not in existing:
                    pool.append({"symbol": s.upper(), "added_at": datetime.now().isoformat()})
                    added += 1
            if added > 0:
                with open(POOL_FILE, "w") as f: json.dump(pool, f, indent=4)
            time.sleep(180) # Respektiert Free-Tier Limits
        except:
            time.sleep(60)

if __name__ == "__main__":
    key = os.getenv("GEMINI_API_KEY")
    # Expansion-Prozess
    p_exp = multiprocessing.Process(target=run_expansion, args=(key,))
    p_exp.start()
    
    # Sentinel-Prozess
    try:
        AureumSentinel().run()
    finally:
        p_exp.terminate()
        log("SYSTEM", "Zyklus beendet.")
