import pandas as pd
import yfinance as yf
import os
import json
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V158 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
ANCHOR_FILE = "anchors_memory.json"
AUDIT_FILE = "heritage_audit.txt"

ANCHOR_THRESHOLD = 0.0005 
MAX_WORKERS = 25

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass

    def try_repair_ticker(self, symbol):
        """Versucht den Ticker durch Suffix-Erweiterung zu retten."""
        suffixes = [".DE", ".L", ".HK", ".PA"]
        for suffix in suffixes:
            test_symbol = f"{symbol}{suffix}"
            try:
                t = yf.Ticker(test_symbol)
                df = t.history(period="1d", interval="1m").tail(1)
                if not df.empty:
                    log("REPAIR", f"✅ Ticker repariert: {symbol} -> {test_symbol}")
                    return test_symbol
            except:
                continue
        return None

    def process_asset(self, symbol):
        try:
            t = yf.Ticker(symbol)
            df = t.history(period="1d", interval="1m").tail(5)
            
            if df.empty:
                # Startet den Reparaturversuch statt sofort zu löschen
                repaired = self.try_repair_ticker(symbol)
                if repaired:
                    return {"old_symbol": symbol, "new_symbol": repaired, "Price": 0} # Markierung für Update
                return {"fail": symbol}

            current_price = df['Close'].iloc[-1]
            last_anchor = self.anchors.get(symbol)
            is_new_anchor = last_anchor is None or abs(current_price - last_anchor) / last_anchor >= ANCHOR_THRESHOLD
            
            if is_new_anchor:
                self.anchors[symbol] = current_price
            
            return {"Ticker": symbol, "Price": current_price, "NewAnchor": is_new_anchor}
        except:
            return {"fail": symbol}

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        results = []
        failed_symbols = []
        updates = {} # Speichert reparierte Symbole
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_asset = {executor.submit(self.process_asset, a['symbol']): a['symbol'] for a in pool}
            for future in as_completed(future_to_asset):
                res = future.result()
                if res:
                    if "Price" in res:
                        if "new_symbol" in res: # Reparierter Ticker
                            updates[res["old_symbol"]] = res["new_symbol"]
                        else:
                            results.append(res)
                    elif "fail" in res:
                        failed_symbols.append(res["fail"])

        # --- POOL UPDATEN (Cleanup + Repairs) ---
        new_pool = []
        for asset in pool:
            s = asset['symbol']
            if s in updates:
                new_pool.append({"symbol": updates[s]}) # Ersetze durch reparierten Ticker
            elif s not in failed_symbols:
                new_pool.append(asset)

        with open(POOL_FILE, "w") as f:
            json.dump(new_pool, f, indent=4)

        # Audit & Anker speichern (Logik wie V156/157)
        with open(ANCHOR_FILE, "w") as f:
            json.dump(self.anchors, f)
            
        log("PROGRESS", f"✅ Zyklus beendet. {len(updates)} Ticker repariert, {len(failed_symbols)} entfernt.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
