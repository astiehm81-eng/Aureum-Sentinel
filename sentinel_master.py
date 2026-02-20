import pandas as pd
import yfinance as yf
import os, json, time, sys, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION EISERNER STANDARD V103.2 ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
ANCHOR_THRESHOLD = 0.001
MAX_WORKERS_LIVE = 15   # Für den schnellen Ticker
MAX_WORKERS_HEAL = 25   # Für das massive Historie-Füllen
RUNTIME_LIMIT = 780      # 13 Minuten

def update_status(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(STATUS_FILE, "w") as f:
        f.write(f"[{timestamp}] {msg}")
    print(f"STATUS: {msg}", flush=True)

def get_decade_path(year):
    decade = (int(year) // 10) * 10
    path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    return path

# --- LÜCKEN-HEILUNG (PARALLEL) ---
def heal_single_asset(symbol, last_date):
    """Holt fehlende historische Daten für ein Asset ab dem letzten Stand."""
    try:
        # Puffer von 1 Tag zur Sicherheit
        start_date = (last_date - timedelta(days=1)).strftime('%Y-%m-%d')
        ticker = yf.Ticker(symbol)
        df_gap = ticker.history(start=start_date, interval="1d")
        
        if not df_gap.empty:
            df_gap = df_gap.reset_index()
            # Datum vereinheitlichen (tz-naive)
            df_gap['Date'] = pd.to_datetime(df_gap['Date']).dt.tz_localize(None)
            df_gap['Ticker'] = symbol
            return df_gap[['Date', 'Ticker', 'Close']].rename(columns={'Close': 'Price'})
    except: pass
    return None

# --- DER MINUTEN-TICKER (LIVE-MODUS) ---
def process_live_tick(asset, anchors):
    symbol = asset['symbol']
    try:
        t = yf.Ticker(symbol)
        df = t.history(period="1d", interval="1m")
        if df.empty: return None
        
        curr_p = df['Close'].iloc[-1]
        last_p = anchors.get(symbol)
        
        if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
            anchors[symbol] = curr_p
            return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": round(curr_p, 4)}
    except: return None

# --- ARCHIVIERUNG, VERHEIRATUNG & HEALING ---
def archive_and_heal():
    update_status("🔍 Starte Archiv-Merge & Deep Healing...")
    
    # 1. Dekaden-Monolithen scannen
    files = glob.glob(os.path.join(HERITAGE_DIR, "heritage_*s.parquet"))
    if not files:
        update_status("Keine Heritage-Daten gefunden. Initialisierung nötig.")
        # Hier könnte ein Initial-Download für den Pool stehen
        return

    # Buffer laden
    buffer_df = pd.DataFrame()
    if os.path.exists(BUFFER_FILE):
        buffer_df = pd.read_parquet(BUFFER_FILE)
        buffer_df['Date'] = pd.to_datetime(buffer_df['Date']).dt.tz_localize(None)

    for path in files:
        h_df = pd.read_parquet(path)
        h_df['Date'] = pd.to_datetime(h_df['Date']).dt.tz_localize(None)
        all_symbols = h_df['Ticker'].unique()
        
        update_status(f"Heile {len(all_symbols)} Assets in {os.path.basename(path)}...")
        
        # Parallel Healing der Historie
        tasks = []
        for symbol in all_symbols:
            last_date = h_df[h_df['Ticker'] == symbol]['Date'].max()
            if (datetime.now() - last_date).days > 1:
                tasks.append((symbol, last_date))
        
        if tasks:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_HEAL) as executor:
                results = list(executor.map(lambda t: heal_single_asset(*t), tasks))
            
            gap_data = pd.concat([res for res in results if res is not None])
            if not gap_data.empty:
                h_df = pd.concat([h_df, gap_data])

        # Verheiraten mit Minuten-Buffer
        relevant_buffer = buffer_df[buffer_df['Ticker'].isin(all_symbols)]
        if not relevant_buffer.empty:
            h_df = pd.concat([h_df, relevant_buffer])

        # Finale Reinigung & Atomic Write
        h_df = h_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last').sort_values(['Ticker', 'Date'])
        h_df.to_parquet(path, index=False, compression='snappy')
        print(f"✅ {os.path.basename(path)} konsolidiert.")

    if os.path.exists(BUFFER_FILE): os.remove(BUFFER_FILE)
    update_status("✨ Archivierung abgeschlossen. Historie lückenlos.")

def run_sentinel_ticker():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    anchors = {}
    start_time = time.time()
    update_status(f"📡 Ticker läuft: {len(pool)} Assets...")

    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_LIVE) as executor:
            results = list(executor.map(lambda a: process_live_tick(a, anchors), pool))
        
        new_ticks = [r for r in results if r is not None]
        if new_ticks:
            df = pd.DataFrame(new_ticks)
            if os.path.exists(BUFFER_FILE):
                df = pd.concat([pd.read_parquet(BUFFER_FILE), df])
            df.to_parquet(BUFFER_FILE, index=False)
        
        time.sleep(max(0, 60 - (time.time() - loop_start)))

if __name__ == "__main__":
    if "--archive" in sys.argv:
        archive_and_heal()
    else:
        run_sentinel_ticker()
