import pandas as pd
import yfinance as yf
import os, json, time, sys, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (V106.4 SECURE) ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
MAX_WORKERS_TICKER = 40
MAX_WORKERS_HERITAGE = 15
RUNTIME_LIMIT = 780 

# Thread-Sicherheit
file_lock = threading.Lock()
stop_event = threading.Event()
anchors = {}

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f: f.write(f"[{timestamp}] {msg}\n")
    print(f"📊 {msg}", flush=True)

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

# --- ENGINE A: LIVE TICKER (SCHREIBT NUR IN BUFFER) ---
def ticker_thread(pool):
    update_status("Ticker-Thread aktiv.")
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
            with file_lock: # Schutz beim Schreiben
                df_b = pd.DataFrame(valid)
                if os.path.exists(BUFFER_FILE):
                    df_b = pd.concat([pd.read_parquet(BUFFER_FILE), df_b])
                df_b.to_parquet(BUFFER_FILE, index=False)

        time.sleep(max(0, 60 - (time.time() - loop_start)))

# --- ENGINE B: HERITAGE-GRÄBER (SCHREIBT NUR HISTORIE < HEUTE) ---
def heritage_thread(pool):
    update_status("Heritage-Gräber aktiv.")
    idx = 0
    while not stop_event.is_set():
        symbol = pool[idx]['symbol']
        try:
            t = yf.Ticker(symbol)
            # Holt MAX, aber wir filtern alles von heute raus, um Ticker-Konflikte zu vermeiden
            df_hist = t.history(period="max", interval="1d").reset_index()
            if not df_hist.empty:
                df_hist['Ticker'] = symbol
                df_hist = df_hist[['Date', 'Ticker', 'Close']].rename(columns={'Close': 'Price'})
                df_hist['Date'] = pd.to_datetime(df_hist['Date']).dt.tz_localize(None)
                
                # Nur Daten bis gestern (Schutz der Live-Daten)
                yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                df_hist = df_hist[df_hist['Date'] < yesterday]
                
                if not df_hist.empty:
                    df_hist['Decade'] = (df_hist['Date'].dt.year // 10) * 10
                    with file_lock: # Schutz des Vaults
                        for decade, data in df_hist.groupby('Decade'):
                            path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
                            if os.path.exists(path):
                                data = pd.concat([pd.read_parquet(path), data]).drop_duplicates(subset=['Date', 'Ticker'])
                            data.drop(columns=['Decade']).to_parquet(path, index=False)
                    print(f"🏛️ {symbol} (Max-History) gesichert.", flush=True)
        except: pass
        idx = (idx + 1) % len(pool)
        time.sleep(2)

# --- DIE "SICHERE HOCHZEIT" (DAILY ARCHIVE) ---
def daily_marriage():
    """Verschiebt Buffer-Ticks sicher in den Vault (darf nur alleine laufen)."""
    update_status("💍 Starte sichere Verheiratung...")
    if not os.path.exists(BUFFER_FILE): return
    
    b_df = pd.read_parquet(BUFFER_FILE)
    b_df['Date'] = pd.to_datetime(b_df['Date']).dt.tz_localize(None)
    b_df['Decade'] = (b_df['Date'].dt.year // 10) * 10
    
    for decade, data in b_df.groupby('Decade'):
        path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
        if os.path.exists(path):
            combined = pd.concat([pd.read_parquet(path), data.drop(columns=['Decade'])])
        else:
            combined = data.drop(columns=['Decade'])
        
        combined.drop_duplicates(subset=['Date', 'Ticker']).to_parquet(path, index=False)
    
    os.remove(BUFFER_FILE)
    update_status("✅ Buffer erfolgreich verheiratet.")

if __name__ == "__main__":
    if "--archive" in sys.argv:
        daily_marriage()
    else:
        with open(POOL_FILE, 'r') as f: pool = json.load(f)
        t1 = threading.Thread(target=ticker_thread, args=(pool,))
        t2 = threading.Thread(target=heritage_thread, args=(pool,))
        t1.start(); t2.start()
        time.sleep(RUNTIME_LIMIT)
        stop_event.set(); t1.join(); t2.join()
