import pandas as pd
import yfinance as yf
import os, json, time, sys, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (V106.8 HIGH-SENSITIVITY) ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
# NEU: Anker-Schwelle auf 0,01% gesetzt
ANCHOR_THRESHOLD = 0.0001 
MAX_WORKERS_TICKER = 100 
MAX_WORKERS_HERITAGE = 30
RUNTIME_LIMIT = 780 

file_lock = threading.Lock()
stop_event = threading.Event()
anchors = {}
healed_assets = set()

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f: f.write(f"[{timestamp}] {msg}\n")
    print(f"🌍 {msg}", flush=True)

# --- ENGINE A: SENSITIVE TICKER ---
def ticker_thread(pool):
    update_status(f"Ticker gestartet (Sensitivität: 0.01%).")
    while not stop_event.is_set():
        loop_start = time.time()
        
        def process_sensitive_tick(asset):
            symbol = asset['symbol']
            try:
                t = yf.Ticker(symbol)
                df = t.history(period="1d", interval="1m")
                if not df.empty:
                    curr_p = round(df['Close'].iloc[-1], 4)
                    last_p = anchors.get(symbol)
                    
                    # Log-Trigger bei 0,01% Abweichung
                    if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                        anchors[symbol] = curr_p
                        print(f"⚡ {symbol}: {curr_p:.4f} (Delta > 0.01%)", flush=True)
                    
                    return {"Date": datetime.now().replace(second=0, microsecond=0), 
                            "Ticker": symbol, "Price": curr_p}
            except: pass
            return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_TICKER) as executor:
            results = list(executor.map(process_sensitive_tick, pool))
        
        valid = [r for r in results if r is not None]
        if valid:
            with file_lock:
                df_b = pd.DataFrame(valid)
                if os.path.exists(BUFFER_FILE):
                    df_b = pd.concat([pd.read_parquet(BUFFER_FILE), df_b])
                df_b.to_parquet(BUFFER_FILE, index=False)
            update_status(f"Pulse: {len(valid)} Ticks lückenlos im Buffer.")

        time.sleep(max(0, 60 - (time.time() - loop_start)))

# --- ENGINE B: HERITAGE (BLEIBT PARALLEL) ---
def heritage_thread(pool):
    # Logik wie V106.7 (Max-Historie & Decade-Splitting)
    # ... (Code gekürzt, bleibt identisch zu V106.7)
    pass
