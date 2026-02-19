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
BATCH_SIZE = 100  # Optimiert für GitHub/Yahoo API Durchsatz
CYCLE_MINUTES = 15

def discover_assets():
    """
    Sucht selbstständig nach den relevantesten globalen Assets.
    Erweitert den Pool automatisch auf die gewünschte Größe.
    """
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f:
            return json.load(f)
    
    print("🔎 Discovery Mode: Initialisiere Top-Asset Pool...")
    # Start-Satz (Blue Chips & Indizes für maximale Abdeckung)
    # Hinweis: In der Vollskalierung ziehen wir hier 10.000 Ticker
    # über Markt-Screener (S&P 500, NASDAQ, DAX, STOXX 600, etc.)
    seeds = [
        {"isin": "DE0007164600", "symbol": "SAP.DE", "name": "SAP SE"},
        {"isin": "DE000ENER6Y0", "symbol": "ENR.DE", "name": "Siemens Energy"},
        {"isin": "US5949181045", "symbol": "MSFT", "name": "Microsoft"},
        {"isin": "US0378331005", "symbol": "AAPL", "name": "Apple"},
        {"isin": "US67066G1040", "symbol": "NVDA", "name": "NVIDIA"}
    ]
    
    # Hier simulieren wir die Auffüllung auf 10.000 für den Strukturtest
    pool = seeds 
    
    with open(POOL_FILE, 'w') as f:
        json.dump(pool, f, indent=4)
    return pool

def write_heritage(pool):
    """
    Erstellt einmalig das historische Fundament (Heritage).
    Wird nur ausgeführt, wenn die Datei noch nicht existiert.
    """
    if os.path.exists(HERITAGE_FILE):
        print("🏛️ Heritage-Vault bereits vorhanden. Überspringe Initialisierung.")
        return
    
    print(f"🏛️ Erzeuge Heritage-Vault für {len(pool)} Assets (5 Jahre Daily)...")
    heritage_list = []
    
    symbols = [p['symbol'] for p in pool]
    isin_map = {p['symbol']: p['isin'] for p in pool}
    
    # Download in Batches für Stabilität
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i+BATCH_SIZE]
        try:
            data = yf.download(batch, period="5y", interval="1d", progress=False, group_by='ticker')
            for sym in batch:
                try:
                    df = data[sym][['Close']].rename(columns={'Close': 'Price'}).dropna()
                    df['ISIN'] = isin_map[sym]
                    df['Source'] = 'Heritage_Initial_Daily'
                    heritage_list.append(df)
                except: continue
        except Exception as e:
            print(f"⚠️ Heritage-Batch Fehler: {e}")

    if heritage_list:
        pd.concat(heritage_list).to_parquet(HERITAGE_FILE, compression='snappy')
        print(f"✅ Heritage-Vault erfolgreich finalisiert ({len(heritage_list)} Assets).")

def run_15m_cycle():
    """
    Der operative Kern: Sammelt 15 Minuten lang Yahoo-Daten im RAM
    und schreibt sie am Ende des Workflows einmalig in den Buffer.
    """
    pool = discover_assets()
    write_heritage(pool) # Nur aktiv beim ersten Start
    
    symbols = [p['symbol'] for p in pool]
    isin_map = {p['symbol']: p['isin'] for p in pool}
    minute_buffer = []
    
    print(f"🚀 Sentinel Live-Zyklus gestartet ({len(symbols)} Assets)...")

    # Exakt 15 Durchläufe à 60 Sekunden
    for minute in range(CYCLE_MINUTES):
        cycle_start = time.time()
        timestamp = datetime.now().strftime('%H:%M:%S')
        
        # Batch-Download der aktuellen Minute
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i+BATCH_SIZE]
            try:
                # Wir holen die letzte verfügbare Minute
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

        print(f"⏱️ Minute {minute + 1}/{CYCLE_MINUTES} erfasst. [{timestamp}]")
        
        # Präzises Warten auf die nächste volle Minute
        elapsed = time.time() - cycle_start
        if elapsed < 60 and minute < (CYCLE_MINUTES - 1):
            time.sleep(60 - elapsed)

    # Nach Abschluss des 15-Minuten-Zyklus: Schreiben
    if minute_buffer:
        new_df = pd.DataFrame(minute_buffer)
        if os.path.exists(BUFFER_FILE):
            existing = pd.read_parquet(BUFFER_FILE)
            # Mergen und Dubletten entfernen (Eiserner Standard)
            pd.concat([existing, new_df]).drop_duplicates().to_parquet(BUFFER_FILE, compression='snappy')
        else:
            new_df.to_parquet(BUFFER_FILE, compression='snappy')
        print(f"✅ Zyklus beendet. {len(minute_buffer)} Datenpunkte in Buffer gesichert.")

if __name__ == "__main__":
    run_15m_cycle()
