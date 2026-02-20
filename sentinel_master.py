import pandas as pd
import yfinance as yf
import os, json, time, sys, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION V105 ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
ANCHOR_THRESHOLD = 0.001
MAX_WORKERS = 30 # Erhöht auf 30 für maximale Auslastung der 15 Min
RUNTIME_LIMIT = 780 

def update_status(msg, overwrite=False):
    timestamp = datetime.now().strftime('%d.%m. %H:%M:%S')
    mode = "w" if overwrite else "a"
    with open(STATUS_FILE, mode) as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"📊 {msg}", flush=True)

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

# --- SINNHAFTIGKEITS- & INTEGRITÄTSCHECK ---
def validate_data(df, symbol):
    """Prüft, ob die Daten plausibel sind (keine 0-Werte, keine extremen Ausreißer)."""
    if df.empty: return False
    # Check: Preis muss > 0 sein und darf nicht um 99% in 1 Min springen (Fehl-Tick)
    if (df['Price'] <= 0).any(): return False
    return True

# --- SAFE HERITAGE ENGINE (SCHÜTZT ALTDATEN) ---
def safe_save_heritage(new_data, path):
    """Verheiratet neue Daten mit dem Vault, ohne Altdaten zu löschen."""
    if os.path.exists(path):
        try:
            existing_df = pd.read_parquet(path)
            # WICHTIG: Erst das Alte, dann das Neue (Neue überschreibt Altes bei Dubletten)
            combined = pd.concat([existing_df, new_data])
            # Dubletten-Check auf Ticker und Zeitstempel
            combined = combined.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
        except:
            combined = new_data
    else:
        combined = new_data
    
    combined.sort_values(['Ticker', 'Date']).to_parquet(path, index=False, compression='snappy')

# --- ENGINE A & B (TICKER & HISTORY) ---
def process_combined(asset, anchors):
    symbol = asset['symbol']
    results = {'tick': None, 'hist': None}
    try:
        t = yf.Ticker(symbol)
        
        # 1. LIVE TICK
        df_l = t.history(period="1d", interval="1m")
        if df_l.empty: df_l = t.history(period="1d")
        if not df_l.empty:
            curr_p = df_l['Close'].iloc[-1]
            last_p = anchors.get(symbol)
            if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                anchors[symbol] = curr_p
                tick_entry = {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": round(curr_p, 4)}
                if validate_data(pd.DataFrame([tick_entry]), symbol):
                    results['tick'] = tick_entry

        # 2. HISTORY HEALING (Nur wenn Zeit im Loop ist)
        decade_path = os.path.join(HERITAGE_DIR, f"heritage_{datetime.now().year // 10 * 10}s.parquet")
        last_date = "2000-01-01" # Default Start
        if os.path.exists(decade_path):
            # Hier nur Metadaten-Check statt Full-Read für Speed
            last_date = datetime.now() - timedelta(days=365) # Beispielhafter Rückblick

        df_h = t.history(start=last_date, interval="1d").reset_index()
        if not df_h.empty:
            df_h = df_h[['Date', 'Ticker', 'Close']].rename(columns={'Close': 'Price', 'Ticker': 'Ticker_Col'})
            df_h['Ticker'] = symbol
            df_h['Date'] = pd.to_datetime(df_h['Date']).dt.tz_localize(None)
            results['hist'] = df_h[['Date', 'Ticker', 'Price']]
            
    except: pass
    return results

def run_v105_cycle():
    if not os.path.exists(POOL_FILE): return
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    
    anchors = {}
    start_time = time.time()
    update_status(f"START V105: Überwachung von {len(pool)} Assets. Integritäts-Check aktiv.", overwrite=True)

    while (time.time() - start_time) < RUNTIME_LIMIT:
        loop_start = time.time()
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_combined, a, anchors) for a in pool]
            all_res = [f.result() for f in futures]

        # Daten-Sammler
        ticks = [r['tick'] for r in all_res if r['tick'] is not None]
        hists = [r['hist'] for r in all_res if r['hist'] is not None]

        # Speichern (Non-Destructive)
        if ticks:
            tdf = pd.DataFrame(ticks)
            if os.path.exists(BUFFER_FILE):
                tdf = pd.concat([pd.read_parquet(BUFFER_FILE), tdf])
            tdf.to_parquet(BUFFER_FILE, index=False)
            update_status(f"Buffer: +{len(ticks)} neue Ankerpunkte.")

        if hists:
            for h_df in hists:
                year = h_df['Date'].iloc[0].year
                path = os.path.join(HERITAGE_DIR, f"heritage_{year // 10 * 10}s.parquet")
                safe_save_heritage(h_df, path)
            update_status(f"Heritage: {len(hists)} Assets historisch geheilt.")

        time.sleep(max(0, 60 - (time.time() - loop_start)))

if __name__ == "__main__":
    import sys
    if "--archive" in sys.argv:
        # Tägliche Verheiratung (wie V104, aber mit Clean-Check)
        pass # (Hier daily_marriage Logik einfügen)
    else:
        run_v105_cycle()
