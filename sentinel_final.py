import pandas as pd
import yfinance as yf
import time
import os
import json
from datetime import datetime

# --- KONFIGURATION ---
HERITAGE_FILE = "sentinel_heritage.parquet"
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
BATCH_SIZE = 100  
CYCLE_MINUTES = 15

def discover_assets():
    """Sucht automatisch die Top-Assets und füllt den Pool."""
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f:
            return json.load(f)
    
    print("🔎 Discovery Mode: Initialisiere Asset Pool...")
    # Basis-Liste der wichtigsten globalen Ticker
    # Diese Liste kann beliebig auf >10.000 erweitert werden
    initial_assets = [
        {"isin": "DE0007164600", "symbol": "SAP.DE", "name": "SAP SE"},
        {"isin": "DE000ENER6Y0", "symbol": "ENR.DE", "name": "Siemens Energy"},
        {"isin": "US5949181045", "symbol": "MSFT", "name": "Microsoft"},
        {"isin": "US0378331005", "symbol": "AAPL", "name": "Apple"},
        {"isin": "US67066G1040", "symbol": "NVDA", "name": "NVIDIA"},
        {"isin": "US0231351067", "symbol": "AMZN", "name": "Amazon"},
        {"isin": "US02079K3059", "symbol": "GOOGL", "name": "Alphabet"}
    ]
    
    # Hier kann eine Logik implementiert werden, die Ticker aus Indizes (DAX, S&P500) zieht
    pool = initial_assets 
    
    with open(POOL_FILE, 'w') as f:
        json.dump(pool, f, indent=4)
    return pool

def write_heritage(pool):
    """Schreibt einmalig 5 Jahre Historie für alle Assets."""
    if os.path.exists(HERITAGE_FILE):
        return
    
    print(f"🏛️ Erzeuge Heritage-Vault für {len(pool)} Assets...")
    heritage_list = []
    symbols = [p['symbol'] for p in pool]
    isin_map = {p['symbol']: p['isin'] for p in pool}
    
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i+BATCH_SIZE]
        try:
            data = yf.download(batch, period="5y", interval="1d", progress=False, group_by='ticker')
            for sym in batch:
                try:
                    df = data[sym][['Close']].rename(columns={'Close': 'Price'}).dropna()
                    df['ISIN'] = isin_map[sym]
                    df['Source'] = 'Heritage_Initial'
                    heritage_list.append(df)
                except: continue
        except: continue

    if heritage_list:
        pd.concat(heritage_list).to_parquet(HERITAGE_FILE, compression='snappy')
        print("✅ Heritage-Vault finalisiert.")

def run_15m_cycle():
    """Sammelt 15 Minuten lang jede Minute Daten."""
    pool = discover_assets()
    write_heritage(pool)
    
    symbols = [p['symbol'] for p in pool]
    isin_map = {p['symbol']: p['isin'] for p in pool}
    minute_buffer = []
    
    print(f"🚀 Live-Zyklus gestartet ({len(symbols)} Assets)...")

    for minute in range(CYCLE_MINUTES):
        cycle_start = time.time()
        
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i+BATCH_SIZE]
            try:
                data = yf.download(batch, period="1d", interval="1m", progress=False, group_by='ticker').tail(1)
                for sym in batch:
                    try:
                        price = data[sym]['Close'].iloc[0] if len(batch) > 1 else data['Close'].iloc[0]
                        if not pd.isna(price):
                            minute_buffer.append({
                                'Timestamp': data.index[0].tz_localize(None),
                                'Price': float(price),
                                'ISIN': isin_map[sym],
                                'Source': 'Yahoo_Live_1m'
                            })
                    except: continue
            except: continue

        print(f"⏱️ Minute {minute + 1}/{CYCLE_MINUTES} erfasst.")
        
        # Warten auf die nächste Minute
        elapsed = time.time() - cycle_start
        if elapsed < 60 and minute < (CYCLE_MINUTES - 1):
            time.sleep(60 - elapsed)

    if minute_buffer:
        new_df = pd.DataFrame(minute_buffer)
        if os.path.exists(BUFFER_FILE):
            existing = pd.read_parquet(BUFFER_FILE)
            pd.concat([existing, new_df]).drop_duplicates().to_parquet(BUFFER_FILE, compression='snappy')
        else:
            new_df.to_parquet(BUFFER_FILE, compression='snappy')
        print(f"✅ Zyklus beendet. {len(minute_buffer)} Punkte gesichert.")

if __name__ == "__main__":
    run_15m_cycle()
