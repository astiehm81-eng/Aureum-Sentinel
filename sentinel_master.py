import pandas as pd
import yfinance as yf
import os, json, time, glob
import requests
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V98 (SEAMLESS SPLICE & OVERLAP) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
MAX_WORKERS = 8

def get_historical_stooq(symbol):
    """Holt die langfristige Historie (Stooq)."""
    st_ticker = symbol.lower().replace(".de", ".tg")
    url = f"https://stooq.com/q/d/l/?s={st_ticker}&f=sdjopc&g=d"
    try:
        res = requests.get(url, timeout=15)
        if res.status_code == 200 and len(res.text) > 100:
            df = pd.read_csv(StringIO(res.text))
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            return df[['Date', 'Close']].rename(columns={'Close': 'Price_Hist'})
    except: pass
    return None

def get_overlap_yahoo(symbol):
    """Holt die letzten 7 Tage von Yahoo, um den Schnittpunkt zu finden."""
    y_sym = symbol.split('.')[0] if ".US" in symbol else symbol
    try:
        y = yf.Ticker(y_sym)
        df = y.history(period="7d") # Puffer für das Wochenende/Feiertage
        if not df.empty:
            df = df.reset_index()
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            return df[['Date', 'Close']].rename(columns={'Close': 'Price_Live'})
    except: pass
    return None

def splice_data(hist, live):
    """Verschmilzt die Datenströme ohne Duplikate und passt den Schnitt an."""
    if hist is None: return live.rename(columns={'Price_Live': 'Price'}) if live is not None else None
    if live is None: return hist.rename(columns={'Price_Hist': 'Price'})

    # Zusammenführen über das Datum
    merged = pd.merge(hist, live, on='Date', how='outer').sort_values('Date')
    
    # Priorisierung: Live-Daten von Yahoo überschreiben Historie am Schnittpunkt
    # 'Price' wird aus 'Price_Live' gefüllt, wenn vorhanden, sonst 'Price_Hist'
    merged['Price'] = merged['Price_Live'].fillna(merged['Price_Hist'])
    
    return merged[['Date', 'Price']]

def process_asset_v98(asset):
    sym = asset['symbol']
    isin = asset.get('isin', 'N/A')
    print(f"🧬 Splicing: {sym}...")
    
    hist = get_historical_stooq(sym)
    live = get_overlap_yahoo(sym)
    
    final_df = splice_data(hist, live)
    if final_df is not None:
        final_df['Ticker'] = sym
        final_df['ISIN'] = isin
        return final_df
    return None

def run_v98():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    with open(POOL_FILE, 'r') as f: pool = json.load(f)

    # Wir verarbeiten die ersten 40 Assets der 2000er Liste
    new_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_asset_v98, a) for a in pool[:40]]
        for f in futures:
            res = f.result()
            if res is not None: new_data.append(res)

    if new_data:
        for df in new_data:
            ticker = df['Ticker'].iloc[0]
            df.to_parquet(os.path.join(HERITAGE_DIR, f"asset_{ticker}.parquet"), index=False)

    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write(f"🛡️ AUREUM SENTINEL V98\n")
        f.write(f"✅ Splice-Status: Yahoo (7d Overlap) + Stooq (Full Hist)\n")
        f.write(f"⏱️ Letzter Batch-Abschluss: {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    run_v98()
