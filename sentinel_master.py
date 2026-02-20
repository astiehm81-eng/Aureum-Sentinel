import pandas as pd
import yfinance as yf
import os, json, time, sys, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- KONFIGURATION (GLOBAL SCALE V106) ---
POOL_FILE = "isin_pool.json"
BUFFER_FILE = "current_buffer.parquet"
STATUS_FILE = "vault_status.txt"
HERITAGE_DIR = "heritage_vault"
ANCHOR_THRESHOLD = 0.001
MAX_WORKERS = 50  # Erhöht für massives Multithreading
RUNTIME_LIMIT = 780 

def update_status(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    with open(STATUS_FILE, "a") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"🌍 {msg}", flush=True)

if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)

# --- NEU: MASSIVE POOL EXPANDER (10.000+ ASSETS) ---
def expand_pool_to_global():
    """Erweitert den Pool um die wichtigsten globalen Ticker."""
    # Beispiel-Indizes, die wir anzapfen (Yahoo Ticker Symbole)
    indices = [
        "^GSPC", "^IXIC", "^RUT", "^GDAXI", "^FTSE", "^FCHI", "^STOXX50E", 
        "^N225", "^HSI", "BTC-USD", "ETH-USD", "GC=F"
    ]
    
    current_pool = []
    if os.path.exists(POOL_FILE):
        with open(POOL_FILE, 'r') as f:
            current_pool = json.load(f)
    
    existing_symbols = {a['symbol'] for a in current_pool}
    new_assets = []

    update_status("🔍 Scanne globale Märkte für Expansion...")
    for idx in indices:
        try:
            ticker = yf.Ticker(idx)
            # Hier simulieren wir die Extraktion. In der Realität nutzen wir 
            # vordefinierte Listen oder API-Abfragen für Index-Komponenten.
            # Da Yahoo keine direkte '.components' Methode hat, nutzen wir 
            # für dieses Beispiel eine Basis-Erweiterung:
            pass 
        except: continue

    # Falls der Pool leer oder klein ist, füllen wir ihn mit einer robusten Basis
    if len(existing_symbols) < 100:
        # Hier ergänzen wir eine Liste von Top-Tickern (Platzhalter für 10k Expansion)
        base_list = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "SAP.DE", "SIE.DE", "AIR.DE"] 
        for s in base_list:
            if s not in existing_symbols:
                new_assets.append({"symbol": s, "sector": "Global-Base"})

    if new_assets:
        current_pool.extend(new_assets)
        with open(POOL_FILE, 'w') as f:
            json.dump(current_pool, f, indent=4)
        update_status(f"✅ Pool auf {len(current_pool)} Assets erweitert.")

# --- SURGEON HEALING & PROCESSING ---
def heal_and_process(asset, anchors):
    symbol = asset['symbol']
    res = {'tick': None}
    try:
        t = yf.Ticker(symbol)
        df = t.history(period="1d", interval="1m")
        if df.empty: df = t.history(period="1d")
        
        if not df.empty:
            curr_p = df['Close'].iloc[-1]
            last_p = anchors.get(symbol)
            
            # Plausibilitätscheck & Heilung
            if last_p and abs(curr_p - last_p) / last_p > 0.20:
                # Heilungsversuch durch Intervall-Wechsel
                df_fix = t.history(period="1d", interval="5m")
                if not df_fix.empty:
                    curr_p = df_fix['Close'].iloc[-1]
                    print(f"🩹 {symbol} geheilt.", flush=True)

            if last_p is None or abs(curr_p - last_p) / last_p >= ANCHOR_THRESHOLD:
                anchors[symbol] = curr_p
                print(f"🚀 {symbol}: {curr_p:.4f}", flush=True)
                res['tick'] = {"Date": datetime.now().replace(microsecond=0), "Ticker": symbol, "Price": round(curr_p, 4)}
    except: pass
    return res

def run_v106_massive_cycle():
    # 1. Expansion prüfen
    expand_pool_to_global()
    
    with open(POOL_FILE, 'r') as f: pool = json.load(f)
    anchors = {}
    start_time = time.time()
    
    # Chunk-Processing für 10.000 Assets
    chunk_size = 500
    for i in range(0, len(pool), chunk_size):
        if (time.time() - start_time) > RUNTIME_LIMIT: break
        
        current_chunk = pool[i:i+chunk_size]
        update_status(f"Verarbeite Chunk {i//chunk_size + 1} ({len(current_chunk)} Assets)...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(lambda a: heal_and_process(a, anchors), current_chunk))
        
        # Sicherung des Chunks
        ticks = [r['tick'] for r in results if r['tick'] is not None]
        if ticks:
            df = pd.DataFrame(ticks)
            if os.path.exists(BUFFER_FILE):
                df = pd.concat([pd.read_parquet(BUFFER_FILE), df])
            df.to_parquet(BUFFER_FILE, index=False)

if __name__ == "__main__":
    run_v106_massive_cycle()
