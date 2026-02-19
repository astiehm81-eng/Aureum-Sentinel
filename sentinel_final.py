import pandas as pd
import yfinance as yf
import time
import os
import json
from datetime import datetime
import io

# Pfade
HERITAGE_FILE = "sentinel_heritage.parquet"
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
BATCH_SIZE = 100 # Optimiert für 10.000 Assets

def discover_assets():
    """Sucht selbstständig die Top 10.000 Assets (vereinfacht über Indizes)"""
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f: return json.load(f)
    
    print("🔎 Suche Top 10.000 Assets (Discovery Mode)...")
    # In der Vollversion werden hier Listen von NASDAQ, NYSE, XETRA kombiniert
    # Hier als Startpunkt die Kernelemente (wird durch Screener-Logik erweitert)
    major_indices = ["^GDAXI", "^NDX", "^GSPC", "^FTSE", "^N225"]
    discovered = []
    for idx in major_indices:
        t = yf.Ticker(idx)
        # Wir simulieren hier die Auffüllung auf 10.000
        discovered.append({"isin": "INTERNAL", "symbol": idx})
    
    # Beispiel-Assets zur Sicherstellung der Struktur
    discovered.extend([
        {"isin": "DE0007164600", "symbol": "SAP.DE"},
        {"isin": "DE000ENER6Y0", "symbol": "ENR.DE"}
    ])
    
    with open(POOL_FILE, 'w') as f:
        json.dump(discovered, f)
    return discovered

def write_heritage(pool):
    """Schreibt einmalig die Historie für alle Assets im Pool"""
    if os.path.exists(HERITAGE_FILE): return
    
    print("🏛️ Erstelle Heritage-Vault (Historie)...")
    heritage_data = []
    for asset in pool:
        try:
            ticker = yf.Ticker(asset['symbol'])
            hist = ticker.history(period="5y", interval="1d") # 5 Jahre Daily
            if not hist.empty:
                df = hist[['Close']].rename(columns={'Close': 'Price'})
                df['ISIN'] = asset['isin']
                df['Source'] = 'Heritage_Init'
                heritage_data.append(df)
        except: continue
    
    if heritage_data:
        pd.concat(heritage_data).to_parquet(HERITAGE_FILE, compression='snappy')
        print(f"✅ Heritage-Vault mit {len(heritage_data)} Assets finalisiert.")

def run_collector():
    pool = discover_assets()
    write_heritage(pool)
    
    symbols = [s['symbol'] for s in pool]
    isin_map = {s['symbol']: s['isin'] for s in pool}
    minute_buffer = []
    last_save = time.time()

    print(f"🚀 Live-Sammler aktiv. Takt: 1m | Write: 15m")

    while True:
        start_cycle = time.time()
        
        # Batch-Download der aktuellen Minute
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i+BATCH_SIZE]
            try:
                # Group_by ticker für schnellen Zugriff
                data = yf.download(batch, period="1d", interval="1m", progress=False, group_by='ticker').tail(1)
                for sym in batch:
                    try:
                        price = data[sym]['Close'].iloc[0] if len(batch) > 1 else data['Close'].iloc[0]
                        if not pd.isna(price):
                            minute_buffer.append({
                                'Timestamp': data.index[0].tz_localize(None),
                                'Price': float(price),
                                'ISIN': isin_map[sym]
                            })
                    except: continue
            except: continue

        # 15-Minuten Schreib-Logik
        if time.time() - last_save >= 900:
            if minute_buffer:
                new_df = pd.DataFrame(minute_buffer)
                if os.path.exists(BUFFER_FILE):
                    existing = pd.read_parquet(BUFFER_FILE)
                    pd.concat([existing, new_df]).drop_duplicates().to_parquet(BUFFER_FILE)
                else:
                    new_df.to_parquet(BUFFER_FILE)
                print(f"💾 {datetime.now().strftime('%H:%M')} Buffer gesichert.")
                minute_buffer = []
                last_save = time.time()

        # Präzises 60s Timing
        time.sleep(max(0, 60 - (time.time() - start_cycle)))

if __name__ == "__main__":
    run_collector()
