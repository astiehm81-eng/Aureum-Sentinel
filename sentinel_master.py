import pandas as pd
import yfinance as yf
import os, json, time, sys, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
ANCHOR_THRESHOLD = 0.001
MAX_WORKERS_TICKER = 15
MAX_WORKERS_HERITAGE = 15 # Parallel zum Ticker
RUNTIME_LIMIT = 780

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f: # Wir hängen an, um den Verlauf zu sehen
        f.write(f"[{timestamp}] {msg}\n")
    print(f"📡 {msg}", flush=True)

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

# --- ENGINE B: INDEPENDENT HERITAGE LOADER ---
def fill_heritage_gap(symbol):
    """Füllt die Historie für ein Asset unabhängig vom Ticker."""
    try:
        decade = datetime.now().year // 10 * 10
        path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
        
        last_date = "1900-01-01"
        if os.path.exists(path):
            h_df = pd.read_parquet(path)
            asset_data = h_df[h_df['Ticker'] == symbol]
            if not asset_data.empty:
                last_date = asset_data['Date'].max()

        # Hole historische Daten (Daily) ab dem letzten Stand
        t = yf.Ticker(symbol)
        df_hist = t.history(start=last_date, interval="1d").reset_index()
        
        if not df_hist.empty:
            df_hist['Ticker'] = symbol
            df_hist = df_hist[['Date', 'Ticker', 'Close']].rename(columns={'Close': 'Price'})
            df_hist['Date'] = pd.to_datetime(df_hist['Date']).dt.tz_localize(None)
            print(f"🏛️ Heritage-Update: {symbol} (+{len(df_hist)} Tage)", flush=True)
            return df_hist
    except: pass
    return None

# --- ENGINE A: LIVE TICKER ---
def process_live_tick(asset, anchors):
    symbol = asset['symbol']
    try:
        t = yf.Ticker(symbol)
        df = t.history(period="1d", interval="1m")
        if df.empty: df = t.history(period="1d")
        
        if not df.empty:
            curr_p = df['Close'].iloc[-1]
            last_p = anchors.get(symbol)
            if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                anchors[symbol] = curr_p
                print(f"🚀 Ticker: {symbol} @ {curr_p:.4f}", flush=True)
                return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": round(curr_p, 4)}
    except: pass
    return None

# --- DAILY MERGE: DIE VERHEIRATUNG ---
def daily_marriage():
    """Schiebt den Buffer in den Heritage-Vault und bereinigt alles."""
    update_status("💍 Starte tägliche Verheiratung & Bereinigung...")
    if not os.path.exists(BUFFER_FILE): return
    
    b_df = pd.read_parquet(BUFFER_FILE)
    b_df['Date'] = pd.to_datetime(b_df['Date']).dt.tz_localize(None)
    
    # Gruppiere Buffer nach Dekaden (falls über Jahreswechsel hinweg)
    b_df['Decade'] = (pd.to_datetime(b_df['Date']).dt.year // 10) * 10
    
    for decade, data in b_df.groupby('Decade'):
        path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
        if os.path.exists(path):
            h_df = pd.read_parquet(path)
            h_df['Date'] = pd.to_datetime(h_df['Date']).dt.tz_localize(None)
            combined = pd.concat([h_df, data.drop(columns=['Decade'])])
        else:
            combined = data.drop(columns=['Decade'])
            
        # Bereinigung: Dubletten raus, Nullwerte raus, Sortieren
        combined = combined.dropna().drop_duplicates(subset=['Date', 'Ticker'], keep='last')
        combined.sort_values(['Ticker', 'Date']).to_parquet(path, index=False)
        print(f"✅ Monolith {decade}s aktualisiert.", flush=True)

    os.remove(BUFFER_FILE)
    update_status("🧹 Buffer verheiratet und gelöscht.")

# --- MAIN LOOP ---
def run_dual_engine():
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    anchors = {}
    start_time = time.time()
    
    update_status(f"Sentinel V104 DUAL-ENGINE aktiv (Pool: {len(pool)})")

    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        
        # Starte beide Motoren gleichzeitig
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_TICKER + MAX_WORKERS_HERITAGE) as executor:
            # Motor A: Ticks sammeln
            tick_futures = [executor.submit(process_live_tick, a, anchors) for a in pool]
            # Motor B: Historie heilen (Stichprobe pro Durchlauf)
            hist_futures = [executor.submit(fill_heritage_gap, a['symbol']) for a in pool[:10]] 
            
            # Ergebnisse sammeln
            new_ticks = [f.result() for f in tick_futures if f.result() is not None]
            new_hists = [f.result() for f in hist_futures if f.result() is not None]

        # Ticker-Daten in Buffer schreiben
        if new_ticks:
            df_t = pd.DataFrame(new_ticks)
            if os.path.exists(BUFFER_FILE):
                df_t = pd.concat([pd.read_parquet(BUFFER_FILE), df_t])
            df_t.to_parquet(BUFFER_FILE, index=False)

        # Heritage-Daten direkt in Monolithen schreiben (sofortiges Füllen)
        if new_hists:
            for h_data in new_hists:
                year = h_data['Date'].iloc[0].year
                path = os.path.join(HERITAGE_DIR, f"heritage_{year // 10 * 10}s.parquet")
                if os.path.exists(path):
                    h_data = pd.concat([pd.read_parquet(path), h_data]).drop_duplicates(subset=['Date', 'Ticker'])
                h_data.to_parquet(path, index=False)

        time.sleep(max(0, 60 - (time.time() - loop_start)))

if __name__ == "__main__":
    if "--archive" in sys.argv:
        daily_marriage()
    else:
        run_dual_engine()
