import pandas as pd
import yfinance as yf
import os, json, time, sys, glob
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION V105.1 ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
ANCHOR_THRESHOLD = 0.001
MAX_WORKERS = 35  # Optimiert für ~500-1000 Assets/15min
RUNTIME_LIMIT = 780 

def update_status(msg, overwrite=False):
    timestamp = datetime.now().strftime('%d.%m. %H:%M:%S')
    mode = "w" if overwrite else "a"
    with open(STATUS_FILE, mode) as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"📊 {msg}", flush=True)

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

# --- 1. SINNHAFTIGKEITS-CHECK (PLAUSIBILITY SHIELD) ---
def is_plausible(symbol, price, last_price):
    """Prüft Daten auf statistisches Rauschen und Fehler."""
    if price <= 0 or pd.isna(price): return False
    if last_price:
        # Schutz vor 'Fat Finger' Fehlern oder Yahoo-Glitches (>20% Sprung in 1 Min)
        change = abs(price - last_price) / last_price
        if change > 0.20: 
            print(f"⚠️ Warnung: Unplausibler Sprung bei {symbol} ({change*100:.1f}%) - Ignoriert.", flush=True)
            return False
    return True

# --- 2. LÜCKEN-ERGÄNZUNG (GAP HEALER) ---
def heal_gaps(symbol, existing_df):
    """Identifiziert fehlende Tage in der Zeitreihe und füllt sie auf."""
    if existing_df.empty: return None
    
    last_entry = existing_df['Date'].max()
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Wenn die letzte Datenpunkt älter als 1 Tag ist (Wochenende ignoriert)
    if (today - last_entry.replace(tzinfo=None)).days > 1:
        try:
            t = yf.Ticker(symbol)
            # Hole die fehlende Spanne
            gap_data = t.history(start=last_entry, end=today, interval="1d").reset_index()
            if not gap_data.empty:
                gap_data['Ticker'] = symbol
                gap_data = gap_data[['Date', 'Ticker', 'Close']].rename(columns={'Close': 'Price'})
                gap_data['Date'] = pd.to_datetime(gap_data['Date']).dt.tz_localize(None)
                return gap_data
        except: pass
    return None

# --- ENGINE LOGIC ---
def process_asset(asset, anchors):
    symbol = asset['symbol']
    res = {'tick': None, 'healing': None}
    try:
        t = yf.Ticker(symbol)
        
        # A) LIVE TICKER mit Plausibilitäts-Check
        df_l = t.history(period="1d", interval="1m")
        if df_l.empty: df_l = t.history(period="1d")
        
        if not df_l.empty:
            curr_p = df_l['Close'].iloc[-1]
            last_p = anchors.get(symbol)
            
            if is_plausible(symbol, curr_p, last_p):
                if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                    anchors[symbol] = curr_p
                    res['tick'] = {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": round(curr_p, 4)}

        # B) AUTOMATISCHE ERGÄNZUNG (HEALING)
        year = datetime.now().year
        path = os.path.join(HERITAGE_DIR, f"heritage_{year // 10 * 10}s.parquet")
        if os.path.exists(path):
            # Wir prüfen nur stichprobenartig oder gezielt Assets mit Lücken
            # Um Performance zu sparen, laden wir hier nur die Metadaten der Zeitreihe
            h_df = pd.read_parquet(path, columns=['Date', 'Ticker']) 
            asset_history = h_df[h_df['Ticker'] == symbol]
            gap_fill = heal_gaps(symbol, asset_history)
            if gap_fill is not None:
                res['healing'] = gap_fill

    except: pass
    return res

def run_v105_cycle():
    if not os.path.exists(POOL_FILE): 
        update_status("ERROR: isin_pool.json nicht gefunden!", overwrite=True)
        return
        
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    anchors = {}
    update_status(f"START V105.1: Analyse von {len(pool)} Assets. Plausibilitäts-Check aktiv.", overwrite=True)

    start_time = time.time()
    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            all_res = list(executor.map(lambda a: process_asset(a, anchors), pool))

        # Datenverarbeitung
        new_ticks = [r['tick'] for r in all_res if r['tick'] is not None]
        new_heals = [r['healing'] for r in all_res if r['healing'] is not None]

        # Speichern & Schützen
        if new_ticks:
            tdf = pd.DataFrame(new_ticks)
            if os.path.exists(BUFFER_FILE):
                tdf = pd.concat([pd.read_parquet(BUFFER_FILE), tdf])
            tdf.to_parquet(BUFFER_FILE, index=False)
            update_status(f"Buffer: {len(new_ticks)} Anker validiert.")

        if new_heals:
            merged_heals = pd.concat(new_heals)
            year = datetime.now().year
            path = os.path.join(HERITAGE_DIR, f"heritage_{year // 10 * 10}s.parquet")
            
            # Non-Destructive Update
            if os.path.exists(path):
                existing = pd.read_parquet(path)
                final = pd.concat([existing, merged_heals]).drop_duplicates(subset=['Date', 'Ticker'], keep='last')
            else:
                final = merged_heals
            final.sort_values(['Ticker', 'Date']).to_parquet(path, index=False)
            update_status(f"Healing: {len(merged_heals)} Datenpunkte ergänzt.")

        time.sleep(max(0, 60 - (time.time() - loop_start)))

if __name__ == "__main__":
    run_v105_cycle()
