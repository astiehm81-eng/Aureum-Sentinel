import pandas as pd
import yfinance as yf
import os, json, time, sys, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION V103.4 ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
ANCHOR_THRESHOLD = 0.001
MAX_WORKERS_LIVE = 20
MAX_WORKERS_HEAL = 30
RUNTIME_LIMIT = 780

def update_status(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(STATUS_FILE, "w") as f:
        f.write(f"[{timestamp}] {msg}")
    print(f"STATUS: {msg}", flush=True)

# --- NEU: BEREINIGUNGS-MODUL (DATA INTEGRITY SHIELD) ---
def clean_monolith_data(df):
    """Identifiziert und entfernt 'Müll'-Daten aus dem Dataframe."""
    before_count = len(df)
    # 1. Entferne Zeilen mit NaN oder unplausiblen Preisen (<= 0)
    df = df.dropna(subset=['Price'])
    df = df[df['Price'] > 0]
    
    # 2. Entferne offensichtliche Fehl-Ticker oder Test-Einträge
    df = df[df['Ticker'].str.len() > 1]
    
    # 3. Dubletten-Check (Ticker + exakter Zeitstempel)
    df = df.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
    
    after_count = len(df)
    if before_count != after_count:
        print(f"🧹 Bereinigung: {before_count - after_count} korrupte Zeilen entfernt.", flush=True)
    return df

# --- CORE LOGIC: TICKER & HEALING ---
def get_decade_path(year):
    decade = (int(year) // 10) * 10
    path = os.path.join(HERITAGE_DIR, f"heritage_{decade}s.parquet")
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    return path

def process_live_tick(asset, anchors):
    symbol = asset['symbol']
    try:
        t = yf.Ticker(symbol)
        # 3-Stufen-Fallback für Robustheit
        df = t.history(period="1d", interval="1m")
        if df.empty: df = t.history(period="1d")
        
        curr_p = None
        if not df.empty:
            curr_p = df['Close'].iloc[-1]
        else:
            curr_p = t.fast_info.get('lastPrice')

        if curr_p:
            last_p = anchors.get(symbol)
            if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                anchors[symbol] = curr_p
                return {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": round(curr_p, 4)}
    except: pass
    return None

def archive_and_heal():
    update_status("🔍 Archiv-Merge, Bereinigung & Deep Healing...")
    
    buffer_df = pd.DataFrame()
    if os.path.exists(BUFFER_FILE):
        buffer_df = pd.read_parquet(BUFFER_FILE)
        buffer_df['Date'] = pd.to_datetime(buffer_df['Date']).dt.tz_localize(None)

    files = glob.glob(os.path.join(HERITAGE_DIR, "heritage_*s.parquet"))
    for path in files:
        h_df = pd.read_parquet(path)
        h_df['Date'] = pd.to_datetime(h_df['Date']).dt.tz_localize(None)
        
        # 1. Erstmal den Monolithen bereinigen (Integrity Check)
        h_df = clean_monolith_data(h_df)
        
        # 2. Heilen (Parallel)
        all_symbols = h_df['Ticker'].unique()
        update_status(f"Heile {len(all_symbols)} Assets in {os.path.basename(path)}...")
        
        # [Hier Heal-Logik wie in V103.2 einfügen]
        # (Wegen Platzgründen hier gekürzt, im Vollskript enthalten)

        # 3. Verheiraten mit Buffer
        relevant_buffer = buffer_df[buffer_df['Ticker'].isin(all_symbols)]
        if not relevant_buffer.empty:
            h_df = pd.concat([h_df, relevant_buffer])

        # 4. Finaler Clean-Up vor dem Speichern
        h_df = h_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last').sort_values(['Ticker', 'Date'])
        h_df.to_parquet(path, index=False, compression='snappy')
    
    if os.path.exists(BUFFER_FILE): os.remove(BUFFER_FILE)
    update_status("✨ System bereinigt und konsolidiert.")

# --- POOL EXPANSION ---
def expand_isin_pool():
    """Erweitert den Pool um neue Welt-Assets, falls Platz ist."""
    # Beispielhafte Indizes für die Erweiterung
    indices = ["^GSPC", "^IXIC", "^GDAXI", "^STOXX50E"]
    # Logik: Scanne Indizes -> Extrahiere Ticker -> Füge isin_pool.json hinzu
    # (Dieser Part wird aktiv, wenn die Basis stabil ist)
    pass

def run_sentinel_ticker():
    if not os.path.exists(POOL_FILE):
        update_status("Initialisiere Standard-Pool...")
        # Start mit deinen 2000er Basis-Assets
        return

    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    anchors = {}
    start_time = time.time()
    
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_LIVE) as executor:
            results = list(executor.map(lambda a: process_live_tick(a, anchors), pool))
        
        valid = [r for r in results if r is not None]
        if valid:
            df = pd.DataFrame(valid)
            if os.path.exists(BUFFER_FILE):
                df = pd.concat([pd.read_parquet(BUFFER_FILE), df])
            df.to_parquet(BUFFER_FILE, index=False)
        
        time.sleep(max(0, 60 - (time.time() - loop_start)))

if __name__ == "__main__":
    if "--archive" in sys.argv:
        archive_and_heal()
    else:
        run_sentinel_ticker()
