import pandas as pd
import yfinance as yf
import os, json, time, glob
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V84 (UNIVERSAL TRANSLATOR) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
BLACKLIST_FILE = "dead_assets.json"
TICKER_MAP_FILE = "ticker_mapping.json" # Speichert gefundene Übersetzungen
MAX_WORKERS = 20 # Weniger Workers, da Suche mehr Zeit benötigt
START_TIME = time.time()

def get_ticker_from_search(query):
    """Nutzt die Yahoo Search API, um den passenden Ticker zu finden."""
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={query}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=5)
        data = response.json()
        if data['quotes']:
            # Wir nehmen das erste Ergebnis, das ein 'EQUITY' ist
            for quote in data['quotes']:
                if quote.get('quoteType') == 'EQUITY':
                    return quote['symbol']
    except:
        pass
    return None

def fetch_universal(asset, ticker_map):
    original_id = asset['symbol'].replace("$", "")
    
    # 1. Prüfen, ob wir den Ticker schon übersetzt haben
    target_ticker = ticker_map.get(original_id)
    
    # 2. Wenn nicht, suchen (besonders bei ISINs oder ASSET_ Platzhaltern)
    if not target_ticker:
        target_ticker = get_ticker_from_search(original_id)
        if not target_ticker:
            return None, original_id, None

    # 3. Daten abrufen
    try:
        t = yf.Ticker(target_ticker)
        df = t.history(period="max")
        if not df.empty:
            df = df.reset_index()
            df = df[['Date', 'Close']].rename(columns={'Close': 'Price'})
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df['Ticker'] = original_id # Unter deiner Pool-ID speichern
            return df, None, {original_id: target_ticker}
    except:
        pass
    
    return None, original_id, None

def run_v84():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    blacklist = []
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, 'r') as f: blacklist = json.load(f)
    
    ticker_map = {}
    if os.path.exists(TICKER_MAP_FILE):
        with open(TICKER_MAP_FILE, 'r') as f: ticker_map = json.load(f)

    # Bestandsaufnahme
    existing_tickers = set()
    if os.path.exists(HERITAGE_DIR):
        for f in glob.glob(f"{HERITAGE_DIR}/*.parquet"):
            try:
                df = pd.read_parquet(f, columns=['Ticker'])
                existing_tickers.update(df['Ticker'].unique())
            except: continue

    missing = [a for a in pool if a['symbol'].replace("$", "") not in existing_tickers and a['symbol'] not in blacklist]
    print(f"📡 V84 Translator: {len(missing)} Lücken werden analysiert...")

    new_data = []
    new_dead = []
    found_mappings = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_universal, a, ticker_map) for a in missing[:50]]
        for f in futures:
            res, dead, mapping = f.result()
            if res is not None: new_data.append(res)
            if dead and not res: new_dead.append(dead)
            if mapping: found_mappings.update(mapping)

    # Ergebnisse speichern
    if new_data:
        full_df = pd.concat(new_data)
        full_df['Decade'] = (full_df['Date'].str[:4].astype(int) // 10) * 10
        for decade, group in full_df.groupby('Decade'):
            path = os.path.join(HERITAGE_DIR, f"history_{decade}s.parquet")
            to_save = group.drop(columns=['Decade'])
            if os.path.exists(path):
                old = pd.read_parquet(path)
                to_save = pd.concat([old, to_save]).drop_duplicates(subset=['Ticker', 'Date'])
            to_save.to_parquet(path, engine='pyarrow', index=False)

    # Mappings und Blacklist persistieren
    if found_mappings:
        ticker_map.update(found_mappings)
        with open(TICKER_MAP_FILE, 'w') as f: json.dump(ticker_map, f, indent=4)
    
    if new_dead:
        blacklist = list(set(blacklist + new_dead))
        with open(BLACKLIST_FILE, 'w') as f: json.dump(blacklist, f)

    # Report
    total = len(existing_tickers) + len(new_data)
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write(f"🛡️ AUREUM SENTINEL V84\n📊 Status: Translator aktiv\n✅ Archiviert: {total}\n🗺️ Mappings: {len(ticker_map)}\n💀 Blacklist: {len(blacklist)}")

if __name__ == "__main__":
    run_v84()
