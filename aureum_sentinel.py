import pandas as pd
import yfinance as yf
import os
import json
import threading
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V152 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
TICKER_FILE = os.path.join(HERITAGE_DIR, "live_ticker.feather")
ANCHOR_FILE = "anchors_memory.json"

ANCHOR_THRESHOLD = 0.0005 
LOOKBACK_MINUTES = 60      
MAX_WORKERS = 12           # Reduziert auf 12, um Rate-Limits zu umgehen
RATE_LIMIT_HIT = False     # Globaler Stopper

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        self.anchors = {}
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass

    def process_asset(self, symbol):
        global RATE_LIMIT_HIT
        if RATE_LIMIT_HIT: return None
        
        try:
            # Kleiner Jitter gegen Erkennung
            time.sleep(random.uniform(0.1, 0.5))
            
            t = yf.Ticker(symbol)
            df = t.history(period="1d", interval="1m").tail(LOOKBACK_MINUTES)
            
            if df.empty:
                return {"fail": symbol}

            price = df['Close'].iloc[-1]
            # Hier Anker-Check und Logik...
            log("TICK", f"⚓ {symbol}: {price}")
            return {"Ticker": symbol, "Price": price}

        except Exception as e:
            if "Too Many Requests" in str(e) or "429" in str(e):
                log("WARNING", "⚠️ Rate Limit erreicht! Stoppe Zyklus zur Sicherheit.")
                RATE_LIMIT_HIT = True
            return None

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        results = []
        to_cleanup = []
        
        log("SYSTEM", f"Starte Scan mit {MAX_WORKERS} Workern...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_asset = {executor.submit(self.process_asset, a['symbol']): a for a in pool}
            for future in as_completed(future_to_asset):
                res = future.result()
                if res:
                    if "fail" in res and not RATE_LIMIT_HIT: 
                        # Nur aufräumen, wenn wir NICHT im Rate Limit hängen!
                        to_cleanup.append(res["fail"])
                    elif "Price" in res:
                        results.append(res)

        if to_cleanup and not RATE_LIMIT_HIT:
            # Cleanup-Logik hier...
            pass
        
        log("PROGRESS", f"✅ Zyklus beendet. {len(results)} erfolgreich.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
