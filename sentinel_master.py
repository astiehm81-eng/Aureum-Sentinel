import pandas as pd
import pandas_datareader.data as web
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V54 (10k+ MASS-SCALE) ---
HERITAGE_FILE = "sentinel_heritage.parquet"
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 40  # Maximale Parallelisierung
CYCLE_MINUTES = 4 # 4 Min sammeln, 1 Min Zeit für Push

def get_extensive_pool():
    """Generiert oder lädt den 10.000+ Asset Pool."""
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f:
            pool = json.load(f)
            if len(pool) >= 10000: return pool

    print("🚀 Discovery: Generiere 10.000+ Asset-Pool...")
    # Basismärkte für Stooq
    markets = [".US", ".DE", ".UK", ".JP", ".FR", ".CH"]
    base_tickers = ["SAP", "ENR", "AAPL", "MSFT", "ASML", "TSLA", "NVDA", "AMZN"]
    
    extensive_pool = []
    # 1. Reale Basis-Ticker injizieren
    for m in markets:
        for t in base_tickers:
            extensive_pool.append({"symbol": f"{t}{m}", "isin": "KNOWN"})
    
    # 2. Auffüllen auf 10.000+ (Platzhalter für automatische Index-Expansion)
    while len(extensive_pool) < 10500:
        idx = len(extensive_pool)
        extensive_pool.append({"symbol": f"ASSET_{idx}.US", "isin": "DISCOVERY_PENDING"})
    
    with open(POOL_FILE, 'w') as f:
        json.dump(extensive_pool, f, indent=4)
    return extensive_pool

def fetch_data_engine(asset, mode="heritage"):
    symbol = asset['symbol']
    if "ASSET_" in symbol: return None # Nur echte Ticker im ersten Schritt
    try:
        if mode == "heritage":
            start = datetime.now() - timedelta(days=5*365)
            df = web.DataReader(symbol, 'stooq', start=start)
        else:
            df = web.DataReader(symbol, 'stooq')
        
        if df is not None and not df.empty:
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns]
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Ticker'] = symbol
            df['Timestamp_Sentinel'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            return df
    except: return None

def save_data(h_res, l_res):
    # Heritage Sync (PyArrow)
    h_list = [r for r in h_res if r is not None]
    if h_list:
        df_h = pd.concat(h_list).reset_index(drop=True)
        if os.path.exists(HERITAGE_FILE):
            old_h = pd.read_parquet(HERITAGE_FILE)
            df_h = pd.concat([old_h, df_h]).drop_duplicates(subset=['Ticker', 'Date'])
        df_h.to_parquet(HERITAGE_FILE, engine='pyarrow', index=False)

    # Buffer Sync (1-Min-Ticker)
    l_list = [r for r in l_res if r is not None]
    if l_list:
        df_l = pd.concat(l_list).reset_index(drop=True)
        if os.path.exists(BUFFER_FILE):
            old_l = pd.read_parquet(BUFFER_FILE)
            df_l = pd.concat([old_l, df_l])
        # Begrenzung des Buffers auf die letzten 50.000 Einträge zur Performance-Wahrung
        if len(df_l) > 50000: df_l = df_l.tail(50000)
        df_l.to_parquet(BUFFER_FILE, engine='pyarrow', index=False)

def run_rapid_cycle():
    pool = get_extensive_pool()
    # Heritage-Schub: Verarbeite 500 Assets pro Lauf
    print(f"🏛️ Heritage-Mass-Sync gestartet...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        h_results = list(executor.map(lambda a: fetch_data_engine(a, "heritage"), pool[:500]))
    
    # Live-Ticker: Jede Minute Abfrage für die Top 100 Assets
    live_samples = []
    print(f"⏱️ Live-Ticker (1-Min-Intervall) für {CYCLE_MINUTES} Minuten aktiv...")
    for i in range(CYCLE_MINUTES):
        iteration_start = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            l_results = list(executor.map(lambda a: fetch_data_engine(a, "live"), pool[:100]))
        live_samples.extend([r for r in l_results if r is not None])
        
        # Präzise 60-Sekunden-Taktung
        elapsed = time.time() - iteration_start
        if i < CYCLE_MINUTES - 1 and elapsed < 60:
            time.sleep(60 - elapsed)

    save_data(h_results, live_samples)

if __name__ == "__main__":
    run_rapid_cycle()
