import pandas as pd
import yfinance as yf
import os, json, time, glob
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V88 (ISIN DISCOVERY) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
TICKER_MAP_FILE = "ticker_mapping.json"
MAX_WORKERS = 8 # Niedriger, um API-Sperren bei der Suche zu vermeiden

def discover_ticker_by_isin(isin):
    """Sucht den Yahoo-Ticker für eine gegebene ISIN."""
    try:
        # Yahoo Finance Search API
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={isin}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        if data.get('quotes'):
            # Wir sortieren nach Exchange (Tradegate/Xetra bevorzugt für .DE ISINs)
            quotes = data['quotes']
            # 1. Priorität: Equity (Aktien)
            equities = [q for q in quotes if q.get('quoteType') == 'EQUITY']
            if equities:
                # Wir nehmen den ersten Treffer (meist der mit dem höchsten Volumen)
                return equities[0]['symbol']
    except Exception as e:
        print(f"⚠️ Suche fehlgeschlagen für {isin}: {e}")
    return None

def process_asset_v88(asset, ticker_map):
    orig_id = asset['symbol'].replace("$", "")
    
    # 1. Zuordnung finden oder entdecken
    target_ticker = ticker_map.get(orig_id)
    
    if not target_ticker:
        # Wenn es wie eine ISIN aussieht (2 Buchstaben + 10 Zeichen)
        if len(orig_id) >= 12 and orig_id[:2].isalpha():
            print(f"🔍 ISIN erkannt: {orig_id}. Suche Ticker...")
            target_ticker = discover_ticker_by_isin(orig_id)
            if target_ticker:
                print(f"🎯 Gefunden: {orig_id} -> {target_ticker}")
            else:
                return None, orig_id, None
        else:
            # Wenn es kein Ticker und keine ISIN ist (z.B. ASSET_103)
            return None, None, None

    # 2. Daten mit dem gefundenen Ticker laden
    try:
        t = yf.Ticker(target_ticker)
        df = t.history(period="max") # Hole die komplette Historie
        if not df.empty:
            df = df.reset_index()
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df['Ticker'] = orig_id # Wir speichern es unter DEINER ID für die Zuordnung
            return df, None, {orig_id: target_ticker}
    except: pass
    
    return None, orig_id, None

def run_v88():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    if not os.path.exists(TICKER_MAP_FILE):
        with open(TICKER_MAP_FILE, 'w') as f: json.dump({}, f)
    with open(TICKER_MAP_FILE, 'r') as f: ticker_map = json.load(f)

    # Filter: Nur Assets, die wir noch nicht im Archiv haben
    archived = set()
    for f in glob.glob(f"{HERITAGE_DIR}/*.parquet"):
        try: archived.update(pd.read_parquet(f, columns=['Ticker'])['Ticker'].unique())
        except: pass

    missing = [a for a in pool if a['symbol'].replace("$", "") not in archived]
    print(f"📡 V88 ISIN-Check: {len(missing)} potenzielle Ziele.")

    new_data, new_mappings = [], {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_asset_v88, a, ticker_map) for a in missing[:30]]
        for f in futures:
            df, _, mapping = f.result()
            if df is not None: new_data.append(df)
            if mapping: new_mappings.update(mapping)

    # Speichern der Parquet-Daten und der Ticker-Map
    if new_data:
        full_df = pd.concat(new_data)
        # ... (Sharding Logik wie zuvor)
        full_df['Decade'] = (full_df['Date'].str[:4].astype(int) // 10) * 10
        for decade, group in full_df.groupby('Decade'):
            path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
            save_df = group.drop(columns=['Decade'])
            if os.path.exists(path):
                old = pd.read_parquet(path)
                save_df = pd.concat([old, save_df]).drop_duplicates(subset=['Ticker', 'Date'])
            save_df.to_parquet(path, engine='pyarrow', index=False)

    if new_mappings:
        ticker_map.update(new_mappings)
        with open(TICKER_MAP_FILE, 'w') as f: json.dump(ticker_map, f, indent=4)

    print(f"✅ Zyklus beendet. {len(new_data)} neue Assets archiviert.")

if __name__ == "__main__": run_v88()
