import pandas as pd
import yfinance as yf
import os, json, time, glob
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V91 (THE AGGREGATOR) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
TICKER_MAP_FILE = "ticker_mapping.json"
MAX_WORKERS = 10
START_TIME = time.time()

def get_master_assets():
    """Die fundamentale Liste der Weltmärkte (Auszug der Top-Titel)."""
    # US Tech & Finance
    tech = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "ORCL", "ADBE"]
    fin = ["JPM", "V", "MA", "BAC", "WFC", "GS", "MS", "AXP"]
    # Deutschland (DAX & Wachstum)
    de = ["SAP.DE", "SIE.DE", "DTE.DE", "AIR.DE", "ALV.DE", "MBG.DE", "BMW.DE", "BAS.DE", "IFX.DE", "ENR.DE"]
    # Europa & Rohstoffe
    eu_res = ["ASML.AS", "MC.PA", "NEST.SW", "NOVN.SW", "GC=F", "SI=F", "CL=F", "BTC-USD"]
    
    combined = tech + fin + de + eu_res
    return [{"symbol": s} for s in combined]

def ensure_infrastructure():
    if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
    # Erzwinge sauberen Pool, falls 'ASSET_' Platzhalter gefunden werden
    clean_needed = False
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f:
            if "ASSET_" in f.read(): clean_needed = True
    
    if clean_needed or not os.path.exists(POOL_FILE):
        print("🧼 Bereinige Pool und injiziere Master-Liste...")
        with open(POOL_FILE, 'w') as f:
            json.dump(get_master_assets(), f, indent=4)

def process_asset(asset):
    sym = asset['symbol']
    try:
        t = yf.Ticker(sym)
        df = t.history(period="max")
        if not df.empty:
            df = df.reset_index()
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df['Ticker'] = sym
            return df
    except: pass
    return None

def run_v91():
    ensure_infrastructure()
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    # Archiv-Abgleich
    archived = set()
    for f in glob.glob(f"{HERITAGE_DIR}/*.parquet"):
        try: archived.update(pd.read_parquet(f, columns=['Ticker'])['Ticker'].unique())
        except: pass

    missing = [a for a in pool if a['symbol'] not in archived]
    print(f"📡 V91: {len(missing)} Assets in der Warteschlange. Starte Batch...")

    new_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_asset, a) for a in missing[:30]]
        for f in futures:
            res = f.result()
            if res is not None: 
                new_data.append(res)
                print(f"✅ Archiviert: {res['Ticker'].iloc[0]}")

    if new_data:
        full_df = pd.concat(new_data)
        full_df['Decade'] = (full_df['Date'].str[:4].astype(int) // 10) * 10
        for decade, group in full_df.groupby('Decade'):
            path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
            save_df = group.drop(columns=['Decade'])
            if os.path.exists(path):
                old = pd.read_parquet(path)
                save_df = pd.concat([old, save_df]).drop_duplicates(subset=['Ticker', 'Date'])
            save_df.to_parquet(path, engine='pyarrow', index=False)

    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write(f"🛡️ AUREUM SENTINEL V91\n✅ Aktivierte Assets: {len(archived) + len(new_data)}\n📦 Letzter Batch: {len(new_data)}")

if __name__ == "__main__": run_v91()
