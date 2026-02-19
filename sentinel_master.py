import pandas as pd
import pandas_datareader.data as web
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V53 (RAPID-CYCLE + BUGFIX) ---
HERITAGE_FILE = "sentinel_heritage.parquet"
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 30 
CYCLE_MINUTES = 3 # Etwas kürzer, um Zeit für den Git-Push zu lassen

def get_pool():
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f: return json.load(f)
    return [{"symbol": "SAP.DE", "isin": "DE0007164600"}]

def fetch_data_engine(asset, mode="heritage"):
    symbol = asset['symbol']
    try:
        if mode == "heritage":
            start = datetime.now() - timedelta(days=5*365)
            df = web.DataReader(symbol, 'stooq', start=start)
        else:
            df = web.DataReader(symbol, 'stooq')
        
        if df is not None and not df.empty:
            # Bugfix: Index (Datum) in eine echte Spalte umwandeln und Typ erzwingen
            df = df.reset_index()
            df.columns = [str(c) for c in df.columns] # Spaltennamen als Strings
            
            if mode == "heritage":
                df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            else:
                df = df[['Date', 'Close']].iloc[:1].rename(columns={'Close': 'Price'})
            
            df['Ticker'] = symbol
            df['Timestamp_Sentinel'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Datum in String wandeln, um PyArrow-Konflikte zu vermeiden
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            return df
    except: return None

def save_data(h_res, l_res):
    # Heritage Sicherung
    h_list = [r for r in h_res if r is not None]
    if h_list:
        df_h = pd.concat(h_list).reset_index(drop=True)
        if os.path.exists(HERITAGE_FILE):
            old_h = pd.read_parquet(HERITAGE_FILE)
            df_h = pd.concat([old_h, df_h]).drop_duplicates(subset=['Ticker', 'Date'])
        df_h.to_parquet(HERITAGE_FILE, engine='pyarrow', index=False)

    # Buffer Sicherung (Live-Daten)
    l_list = [r for r in l_res if r is not None]
    if l_list:
        df_l = pd.concat(l_list).reset_index(drop=True)
        if os.path.exists(BUFFER_FILE):
            old_l = pd.read_parquet(BUFFER_FILE)
            df_l = pd.concat([old_l, df_l])
        df_l.to_parquet(BUFFER_FILE, engine='pyarrow', index=False)
    print("✅ Daten erfolgreich im Eiserner Standard Format gesichert.")

def run_rapid_cycle():
    pool = get_pool()
    # Heritage für die nächsten 200 Assets (Batch-Size für Stabilität reduziert)
    print(f"🚀 Heritage-Sync gestartet...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        h_results = list(executor.map(lambda a: fetch_data_engine(a, "heritage"), pool[:200]))
    
    # Live Collector
    live_samples = []
    print(f"⏱️ Live-Check für {CYCLE_MINUTES} Minuten...")
    for i in range(CYCLE_MINUTES):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            l_results = list(executor.map(lambda a: fetch_data_engine(a, "live"), pool[:20]))
        live_samples.extend([r for r in l_results if r is not None])
        if i < CYCLE_MINUTES - 1: time.sleep(60)

    save_data(h_results, live_samples)

if __name__ == "__main__":
    run_rapid_cycle()
