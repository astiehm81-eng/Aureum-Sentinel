import pandas as pd
import yfinance as yf
import time
import os
import json
from datetime import datetime

# --- KONFIGURATION (EISERNER STANDARD V46 - MASS SCALE) ---
HERITAGE_FILE = "sentinel_heritage.parquet"
BUFFER_FILE = "sentinel_buffer.parquet"
POOL_FILE = "isin_pool.json"
CYCLE_MINUTES = 15
BATCH_SIZE = 100

def get_market_tickers():
    """Generiert eine Liste von ~10.000 Tickersymbolen aus globalen Indizes."""
    # Beispielhaft: DAX, S&P 500, NASDAQ 100, Russell 2000 & STOXX 600
    # In der Praxis laden wir hier die Listen der Börsenplätze
    core_symbols = [
        "SAP.DE", "ENR.DE", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", 
        "ASML.AS", "SIE.DE", "DTE.DE", "AIR.PA", "MC.PA"
    ]
    # Hier können wir dynamisch Ticker hinzufügen
    return list(set(core_symbols)) 

def run_sentinel_mass_scale():
    tickers = get_market_tickers()
    print(f"🌍 Discovery: Überwache {len(tickers)} Assets...")
    
    # 1. Heritage Check (Nur anhängen, nicht mehr löschen!)
    if not os.path.exists(HERITAGE_FILE):
        results = []
        for t in tickers:
            try:
                df = yf.download(t, period="max", interval="1d", progress=False)
                if not df.empty:
                    df = df[['Close']].rename(columns={'Close': 'Price'})
                    df['Ticker'] = t
                    results.append(df)
            except: continue
        if results:
            pd.concat(results).to_parquet(HERITAGE_FILE, compression='snappy')

    # 2. Live-Zyklus mit Hard-Refresh von Tradegate/Yahoo
    live_data = []
    print(f"🚀 Live-Monitoring startet...")
    for m in range(CYCLE_MINUTES):
        start = time.time()
        # Batch-Download für Speed
        try:
            data = yf.download(tickers, period="1d", interval="1m", progress=False, group_by='ticker').tail(1)
            for t in tickers:
                try:
                    price = data[t]['Close'].iloc[0] if len(tickers) > 1 else data['Close'].iloc[0]
                    if not pd.isna(price):
                        live_data.append({'Timestamp': datetime.now(), 'Price': float(price), 'Ticker': t})
                except: continue
        except: pass
        
        wait = 60 - (time.time() - start)
        if wait > 0 and m < 14: time.sleep(wait)

    # 3. Buffer sichern
    if live_data:
        df_new = pd.DataFrame(live_data)
        if os.path.exists(BUFFER_FILE):
            df_new = pd.concat([pd.read_parquet(BUFFER_FILE), df_new]).drop_duplicates()
        df_new.to_parquet(BUFFER_FILE, compression='snappy')
        print(f"✅ Zyklus beendet. {len(live_data)} Datenpunkte gesichert.")

if __name__ == "__main__":
    run_sentinel_mass_scale()
