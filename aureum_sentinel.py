import pandas as pd
import yfinance as yf
import os, json, time, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (V107.1 - EISERNER STANDARD) ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"

# 0,01% Sensitivität & Performance
ANCHOR_THRESHOLD = 0.0001 
MAX_WORKERS_TICKER = 100 
REFRESH_RATE = 60 

file_lock = threading.Lock()
stop_event = threading.Event()
anchors = {}

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f: f.write(f"[{timestamp}] {msg}\n")
    print(f"🛡️ {msg}", flush=True)

# --- ENGINE A: LIVE TICKER (YAHOO) ---
def ticker_engine(pool):
    update_status("Engine A (Yahoo Live) gestartet. Schwelle: 0,01%.")
    while not stop_event.is_set():
        loop_start = time.time()
        def process_tick(asset):
            symbol = asset['symbol']
            try:
                t = yf.Ticker(symbol)
                # Hard Refresh via fast_info
                curr_p = round(t.fast_info['last_price'], 4)
                last_p = anchors.get(symbol)
                
                # Anker-Logik
                if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                    anchors[symbol] = curr_p
                    print(f"⚡ {symbol}: {curr_p:.4f} (Anchor Triggered)")
                
                return {"Date": datetime.now(), "Ticker": symbol, "Price": curr_p}
            except: return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_TICKER) as executor:
            results = list(executor.map(process_tick, pool))
        
        valid = [r for r in results if r is not None]
        if valid:
            with file_lock:
                df = pd.DataFrame(valid)
                if os.path.exists(BUFFER_FILE):
                    df = pd.concat([pd.read_parquet(BUFFER_FILE), df])
                df.to_parquet(BUFFER_FILE, index=False)
        
        time.sleep(max(0, REFRESH_RATE - (time.time() - loop_start)))

# --- ENGINE B: HERITAGE (STOOQ) ---
def heritage_engine(pool):
    update_status("Engine B (Stooq Heritage) aktiv.")
    def fetch_stooq(asset):
        symbol = asset['symbol']
        path = f"{HERITAGE_DIR}/{symbol}_history.parquet"
        # Nur laden, wenn älter als 7 Tage
        if os.path.exists(path) and (time.time() - os.path.getmtime(path)) < 604800: return
        
        try:
            url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
            df_hist = pd.read_csv(url)
            if not df_hist.empty: df_hist.to_parquet(path)
        except: pass

    with ThreadPoolExecutor(max_workers=30) as executor:
        executor.map(fetch_stooq, pool)

if __name__ == "__main__":
    if not os.path.exists(POOL_FILE):
        update_status("POOL_FILE fehlt. Warte auf Gemini-Repair-Agent...")
        exit(1)
    
    with open(POOL_FILE, "r") as f: pool = json.load(f)
    
    # Threads starten
    t_thread = threading.Thread(target=ticker_engine, args=(pool,))
    h_thread = threading.Thread(target=heritage_engine, args=(pool,))
    t_thread.start()
    h_thread.start()
