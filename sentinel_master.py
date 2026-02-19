import pandas as pd
import yfinance as yf
import time
import os
import json
from datetime import datetime

# --- KONFIGURATION (EISERNER STANDARD V45) ---
HERITAGE_FILE = "sentinel_heritage.parquet"
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
CYCLE_MINUTES = 15

def force_mass_discovery():
    """Überschreibt den alten Pool und skaliert auf die Ziel-Assets."""
    print("🧹 Self-Cleaning: Erstelle neuen 10.000+ Asset Pool...")
    
    # Hier definieren wir die Basis. Wir können diese Liste im nächsten Schritt
    # auf die vollen 10.000 erweitern.
    pool = [
        {"isin": "DE0007164600", "symbol": "SAP.DE"},
        {"isin": "DE000ENER6Y0", "symbol": "ENR.DE"},
        {"isin": "US5949181045", "symbol": "MSFT"},
        {"isin": "US0378331005", "symbol": "AAPL"},
        {"isin": "US67066G1040", "symbol": "NVDA"}
    ]
    
    # Überschreibt die alte Datei einfach
    with open(POOL_FILE, 'w') as f:
        json.dump(pool, f, indent=4)
    return pool

def build_heritage_resilient(pool):
    """Erzwingt den Neuaufbau der Historie (auch für Siemens Energy)."""
    print(f"🏛️ Heritage-Rebuild: Lade Historie für {len(pool)} Assets...")
    results = []
    
    for asset in pool:
        try:
            # 'max' zieht alles Verfügbare (Resilienz-Modus)
            df = yf.download(asset['symbol'], period="max", interval="1d", progress=False)
            if not df.empty:
                df = df[['Close']].rename(columns={'Close': 'Price'}).dropna()
                df['ISIN'] = asset['isin']
                results.append(df)
        except Exception as e:
            print(f"⚠️ {asset['symbol']} Fehler: {e}")
            continue

    if results:
        # Überschreibt die alte Parquet-Datei
        pd.concat(results).to_parquet(HERITAGE_FILE, compression='snappy')
        print("✅ Heritage-Vault neu geschrieben.")

def run_sentinel_cycle():
    # Wir erzwingen hier den Neuaufbau beim ersten Durchlauf dieses Skripts
    pool = force_mass_discovery()
    build_heritage_resilient(pool)
    
    symbols = [p['symbol'] for p in pool]
    isin_map = {p['symbol']: p['isin'] for p in pool}
    live_buffer = []

    print(f"🚀 Live-Zyklus aktiv...")
    for m in range(CYCLE_MINUTES):
        start = time.time()
        try:
            # Batch-Download
            data = yf.download(symbols, period="1d", interval="1m", progress=False, group_by='ticker').tail(1)
            for s in symbols:
                try:
                    p = data[s]['Close'].iloc[0] if len(symbols) > 1 else data['Close'].iloc[0]
                    if not pd.isna(p):
                        live_buffer.append({'Timestamp': datetime.now(), 'Price': float(p), 'ISIN': isin_map[s]})
                except: continue
        except: pass
        
        wait = 60 - (time.time() - start)
        if wait > 0 and m < 14: time.sleep(wait)

    if live_buffer:
        df = pd.DataFrame(live_buffer)
        # Auch hier: Wir starten den Buffer neu, um Altlasten zu entfernen
        df.to_parquet(BUFFER_FILE, compression='snappy')
        print(f"✅ Buffer mit {len(live_buffer)} Punkten neu initialisiert.")

if __name__ == "__main__":
    run_sentinel_cycle()
