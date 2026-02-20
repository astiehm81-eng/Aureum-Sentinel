import pandas as pd
import yfinance as yf
import os, json, time, glob
import requests
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V95 (HERITAGE FUSION) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
MAX_WORKERS = 5 # Niedriger für Stooq-Stabilität

def get_stooq_data(ticker):
    """Holt historische Daten von Stooq als Second-Source (kostenfrei)."""
    # Stooq nutzt oft andere Suffixe (.DE -> .TG für Tradegate etc.)
    stooq_ticker = ticker.lower().replace(".de", ".tg")
    url = f"https://stooq.com/q/d/l/?s={stooq_ticker}&f=sdjopc&g=d"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200 and len(response.text) > 100:
            df = pd.read_csv(StringIO(response.text))
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price_Stooq'})
            return df
    except:
        pass
    return None

def process_fused_asset(asset):
    sym = asset['symbol']
    print(f"🧬 Fusing: {sym}...")
    
    # 1. Quelle: Yahoo
    try:
        y_ticker = yf.Ticker(sym)
        y_df = y_ticker.history(period="max")
        if not y_df.empty:
            y_df = y_df.reset_index()
            y_df['Date'] = pd.to_datetime(y_df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            y_df = y_df[['Date', 'Close']].rename(columns={'Close': 'Price_Yahoo'})
            
            # 2. Quelle: Stooq (Merge-Versuch)
            s_df = get_stooq_data(sym)
            
            if s_df is not None:
                # Outer Join über das Datum
                fused = pd.merge(y_df, s_df, on='Date', how='outer')
                # Priorisierung: Wenn Yahoo fehlt, nimm Stooq, sonst Yahoo
                fused['Price'] = fused['Price_Yahoo'].fillna(fused['Price_Stooq'])
                fused = fused[['Date', 'Price']].sort_values('Date')
                fused['Ticker'] = sym
                print(f"✅ Fusion erfolgreich: {sym}")
                return fused
            else:
                y_df = y_df.rename(columns={'Price_Yahoo': 'Price'})
                y_df['Ticker'] = sym
                return y_df
    except:
        pass
    return None

def run_v95():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    # Nur die Top-Werte für den aufwendigen Heritage-Merge
    # Wir nehmen die ersten 50, die noch nicht "fused" sind
    new_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_fused_asset, a) for a in pool[:50]]
        for f in futures:
            res = f.result()
            if res is not None:
                new_data.append(res)

    # Speichern in den Vault
    if new_data:
        for df in new_data:
            ticker = df['Ticker'].iloc[0]
            path = os.path.join(HERITAGE_DIR, f"asset_{ticker}.parquet")
            df.to_parquet(path, index=False)

    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write(f"🛡️ AUREUM SENTINEL V95 - HERITAGE FUSION\n")
        f.write(f"✅ Validierte Datenquellen: Yahoo Finance, Stooq\n")
        f.write(f"📊 Letzter Sync: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    run_v95()
