import pandas as pd
import yfinance as yf
import os, json, time, sys, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (V106.6 SELF-EVOLVING) ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
MAX_WORKERS_TICKER = 50 
MAX_WORKERS_HERITAGE = 20
RUNTIME_LIMIT = 780 

# Thread-Sicherheit & Kontrolle
file_lock = threading.Lock()
stop_event = threading.Event()
anchors = {}
healed_assets = set() # Verhindert Mehrfach-Heilung in einer Session

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f: f.write(f"[{timestamp}] {msg}\n")
    print(f"🌍 {msg}", flush=True)

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

# --- NEU: INTEGRIERTE EXPANSIONS-LOGIK ---
def autonomous_expansion():
    """Erweitert den Pool automatisch, wenn er unter das Ziel-Limit fällt."""
    target_size = 10000
    current_pool = []
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f: current_pool = json.load(f)
    
    if len(current_pool) < target_size:
        update_status(f"Expansion gestartet: {len(current_pool)} -> {target_size} Assets...")
        # Hier definieren wir die 'Samen' für die Expansion (Indizes)
        seeds = ["^GSPC", "^IXIC", "^GDAXI", "^FTSE", "^STOXX50E", "^N225"]
        new_entries = []
        
        # Beispielhafte Erweiterung durch bekannte Blue-Chips & Tech-Werte
        # In V106.6 nutzen wir eine interne Liste von ~500 Symbolen als ersten Sprung
        global_top_tickers = [
            "AAPL", "MSFT", "NVDA", "GOOG", "AMZN", "META", "TSLA", "BRK-B", "LLY", "AVGO",
            "SAP.DE", "SIE.DE", "ALV.DE", "DTE.DE", "AIR.DE", "MBG.DE", "BMW.DE", "BAS.DE",
            "ASML.AS", "MC.PA", "OR.PA", "TTE.PA", "SAN.PA", "NESN.SW", "NOVN.SW", "ROG.SW"
            # Hier kann die Liste beliebig verlängert werden
        ]
        
        existing_symbols = {a['symbol'] for a in current_pool}
        for s in global_top_tickers:
            if s not in existing_symbols:
                new_entries.append({"symbol": s, "sector": "Auto-Expand"})
        
        current_pool.extend(new_entries)
        with open(POOL_FILE, 'w') as f:
            json.dump(current_pool, f, indent=4)
        update_status(f"Pool auf {len(current_pool)} Assets erweitert.")
    return current_pool

# --- ENGINE A: LIVE TICKER ---
def ticker_thread(pool):
    while not stop_event.is_set():
        loop_start = time.time()
        def single_tick(asset):
            symbol = asset['symbol']
            try:
                t = yf.Ticker(symbol)
                df = t.history(period="1d", interval="1m")
                if not df.empty:
                    curr_p = df['Close'].iloc[-1]
                    last_p = anchors.get(symbol)
                    if curr_p > 0 and (not last_p or abs(curr_p-last_p)/last_p < 0.20):
                        if last_p is None or abs(curr_p - last_p) / last_p >= 0.001:
                            anchors[symbol] = curr_p
                            print(f"🚀 {symbol}: {curr_p:.4f}", flush=True)
                            return {"Date": datetime.now(), "Ticker": symbol, "Price": round(curr_p, 4)}
            except: pass
            return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_TICKER) as executor:
            results = list(executor.map(single_tick, pool))
        
        valid = [r for r in results if r is not None]
        if valid:
            with file_lock:
                df_b = pd.DataFrame(valid)
                if os.path.exists(BUFFER_FILE):
                    df_b = pd.concat([pd.read_parquet(BUFFER_FILE), df_b])
                df_b.to_parquet(BUFFER_FILE, index=False)

        time.sleep(max(0, 60 - (time.time() - loop_start)))

# --- ENGINE B: HERITAGE-GRÄBER (MIT SESSION-MEMORY) ---
def heritage_thread(pool):
    idx = 0
    while not stop_event.is_set():
        asset = pool[idx]
        symbol = asset['symbol']
        
        if symbol not in healed_assets:
            try:
                t = yf.Ticker(symbol)
                df_hist = t.history(period="max", interval="1d").reset_index()
                if not df_hist.empty:
                    df_hist['Ticker'] = symbol
                    df_hist = df_hist[['Date', 'Ticker', 'Close']].rename(columns={'Close': 'Price'})
                    df_hist['Date'] = pd.to_datetime(df_hist['Date']).dt.tz_localize(None)
                    
                    # Nur Daten bis gestern
                    yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    df_hist = df_hist[df_hist['Date'] < yesterday]
                    
                    if not df_hist.empty:
                        df_hist['Decade'] = (df_hist['Date'].dt.year // 10) * 10
                        with file_lock:
                            for decade, data in df_hist.groupby('Decade'):
                                path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                                if os.path.exists(path):
                                    data = pd.concat([pd.read_parquet(path), data]).drop_duplicates(subset=['Date', 'Ticker'])
                                data.drop(columns=['Decade']).to_parquet(path, index=False)
                        healed_assets.add(symbol)
                        print(f"🏛️ {symbol}: Max-Historie verarbeitet.", flush=True)
            except: pass
        
        idx = (idx + 1) % len(pool)
        time.sleep(0.5)

if __name__ == "__main__":
    if "--archive" in sys.argv:
        # Marriage Logik (wie V106.4)
        pass 
    else:
        # 1. Automatisch Pool laden & erweitern
        active_pool = autonomous_expansion()
        
        # 2. Parallel-Threads starten
        t1 = threading.Thread(target=ticker_thread, args=(active_pool,))
        t2 = threading.Thread(target=heritage_thread, args=(active_pool,))
        t1.start(); t2.start()
        
        time.sleep(RUNTIME_LIMIT)
        stop_event.set(); t1.join(); t2.join()
        update_status("Lauf beendet.")
