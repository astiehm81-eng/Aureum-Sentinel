import pandas as pd
import yfinance as yf
import os, json, time, sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION EISERNER STANDARD ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.json"
HERITAGE_BASE = "heritage_vault"
ANCHOR_THRESHOLD = 0.001  # 0,1% Bewegung
MAX_WORKERS = 15          # Parallelität für 2000er Pool
RUNTIME_LIMIT = 780       # 13 Minuten Laufzeit für 15-Min-Workflow

# --- 1. HILFSFUNKTIONEN ---

def get_decade_folder(year):
    return f"{(int(year) // 10) * 10}s"

def heal_gaps(symbol, last_timestamp):
    """Besorgt fehlende Minuten-Daten seit dem letzten Eintrag."""
    try:
        y = yf.Ticker(symbol)
        df = y.history(start=last_timestamp, interval="1m")
        if not df.empty:
            df = df.reset_index()
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            gap_data = df[df['Date'] > pd.to_datetime(last_timestamp)]
            return gap_data[['Date', 'Close']].rename(columns={'Close': 'p', 'Date': 't'})
    except: pass
    return None

# --- 2. ARCHIVIERUNG & PURGE (TÄGLICH) ---

def move_buffer_to_heritage_v101_1():
    """Sortiert den Buffer in Dekaden ein und löscht flache Dateien im Root."""
    print("🏛️ Starte Archivierung & Root-Purge...", flush=True)
    
    if os.path.exists(BUFFER_FILE):
        with open(BUFFER_FILE, 'r') as f:
            try: buffer_data = json.load(f)
            except: buffer_data = {}

        for symbol, ticks in buffer_data.items():
            if not ticks: continue
            df = pd.DataFrame(ticks)
            df['t_dt'] = pd.to_datetime(df['t'])
            df['decade'] = df['t_dt'].dt.year.apply(get_decade_folder)

            for decade, decade_df in df.groupby('decade'):
                target_dir = os.path.join(HERITAGE_BASE, decade)
                if not os.path.exists(target_dir): os.makedirs(target_dir)
                
                file_path = os.path.join(target_dir, f"{symbol}.parquet")
                new_data = decade_df[['t', 'p']].rename(columns={'t': 'Date', 'p': 'Price'})

                if os.path.exists(file_path):
                    old_df = pd.read_parquet(file_path)
                    new_data = pd.concat([old_df, new_data]).drop_duplicates(subset=['Date'], keep='last')
                
                new_data.to_parquet(file_path, index=False)
        
        os.remove(BUFFER_FILE)
        print("✅ Buffer erfolgreich in Dekaden überführt.", flush=True)

    # DER PURGE: Alles löschen, was direkt in heritage_vault/ liegt
    flat_files = [f for f in os.listdir(HERITAGE_BASE) if os.path.isfile(os.path.join(HERITAGE_BASE, f))]
    for f in flat_files:
        try:
            os.remove(os.path.join(HERITAGE_BASE, f))
            print(f"🗑️ Purged: {f}", flush=True)
        except: pass
    print("✨ Reinigung abgeschlossen.", flush=True)

# --- 3. LIVETICKER & GAP-HEALING (ALLE 15 MIN) ---

def process_tick(asset, anchors):
    symbol = asset['symbol']
    try:
        t = yf.Ticker(symbol)
        df = t.history(period="1d", interval="1m")
        if df.empty: return

        current_price = df['Close'].iloc[-1]
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_anchor = anchors.get(symbol)

        # 0,1% Check oder Initialisierung
        if last_anchor is None or abs(current_price - last_anchor) / last_anchor >= ANCHOR_THRESHOLD:
            # GAP-HEALING vor dem Schreiben in den Buffer
            # (In einer echten Prod-Umgebung würde hier der Buffer-Load stehen)
            print(f"🚀 [ANCHOR] {symbol}: {current_price:.4f}", flush=True)
            anchors[symbol] = current_price
            
            # Hot-Update des Buffers
            update_local_buffer(symbol, current_price, now_str)
        else:
            print(f"  · {symbol}: {current_price:.4f}", flush=True)
    except: pass

def update_local_buffer(symbol, price, ts):
    data = {}
    if os.path.exists(BUFFER_FILE):
        with open(BUFFER_FILE, 'r') as f:
            try: data = json.load(f)
            except: data = {}
    
    if symbol not in data: data[symbol] = []
    data[symbol].append({"t": ts, "p": round(price, 4)})
    
    with open(BUFFER_FILE, 'w') as f:
        json.dump(data, f)

def run_cycle():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    anchors = {}
    start_time = time.time()
    print(f"📡 Aureum Sentinel V101.2 gestartet (13min Loop)...", flush=True)

    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(lambda a: process_tick(a, anchors), pool)
        
        elapsed = time.time() - loop_start
        time.sleep(max(0, 60 - elapsed))

if __name__ == "__main__":
    # Standardmäßig wird der Cycle gestartet. 
    # Für das Archiv-Skript rufen wir die Funktion direkt über die YML auf.
    run_cycle()
