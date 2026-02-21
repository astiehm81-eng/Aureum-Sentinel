import pandas as pd
import yfinance as yf
import os
import json
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V159 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
ANCHOR_FILE = "anchors_memory.json"
AUDIT_FILE = "heritage_audit.txt"

ANCHOR_THRESHOLD = 0.0005 
MAX_WORKERS = 25
RATE_LIMIT_HIT = False

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        if not os.path.exists(HERITAGE_DIR): os.makedirs(HERITAGE_DIR)
        self.anchors = {}
        self._load_anchors()

    def _load_anchors(self):
        if os.path.exists(ANCHOR_FILE):
            try:
                with open(ANCHOR_FILE, "r") as f: self.anchors = json.load(f)
            except: pass

    def try_repair_ticker(self, symbol):
        """Versucht Ticker mit gängigen Suffixen zu retten."""
        # Vermeide doppelte Suffixe
        if "." in symbol: return None
        suffixes = [".DE", ".L", ".HK", ".PA", ".AS"]
        for sfx in suffixes:
            try:
                t = yf.Ticker(f"{symbol}{sfx}")
                df = t.history(period="1d", interval="1m").tail(1)
                if not df.empty:
                    return f"{symbol}{sfx}"
            except: continue
        return None

    def process_asset(self, symbol):
        global RATE_LIMIT_HIT
        if RATE_LIMIT_HIT: return None
        try:
            time.sleep(random.uniform(0.05, 0.15))
            t = yf.Ticker(symbol)
            df = t.history(period="1d", interval="1m").tail(5)
            
            if df.empty:
                repaired = self.try_repair_ticker(symbol)
                if repaired: return {"old": symbol, "new": repaired}
                return {"fail": symbol}

            price = df['Close'].iloc[-1]
            last = self.anchors.get(symbol)
            is_new_anchor = last is None or abs(price - last) / last >= ANCHOR_THRESHOLD
            
            if is_new_anchor:
                self.anchors[symbol] = price
            
            return {"Ticker": symbol, "Price": price, "NewAnchor": is_new_anchor}
        except Exception as e:
            if "429" in str(e): RATE_LIMIT_HIT = True
            return {"fail": symbol}

    def write_audit(self, stats):
        """Erstellt die heritage_audit.txt für den User."""
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report = [
            f"=== AUREUM SENTINEL V159 PULSE [{ts}] ===",
            f"Pool-Größe: {stats['total']} Assets",
            f"Erfolgreich: {stats['success']}",
            f"Repariert (Suffix): {stats['repaired']}",
            f"Entfernt (Delisted): {stats['cleaned']}",
            f"Neue Ankerpunkte: {stats['anchors']}",
            f"System-Status: {'⚠️ LIMIT' if RATE_LIMIT_HIT else '✅ OK'}",
            "-" * 40,
            "Aktive Bewegungen (Top Ticker):",
            ", ".join(stats['moves'][:15]) if stats['moves'] else "Keine Bewegungen > 0.05%"
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        results, failed, repairs = [], [], []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.process_asset, a['symbol']): a['symbol'] for a in pool}
            for f in as_completed(futures):
                res = f.result()
                if not res: continue
                if "Price" in res: results.append(res)
                elif "new" in res: repairs.append(res)
                elif "fail" in res: failed.append(res["fail"])

        # Pool Update & Cleanup
        new_pool = []
        repair_map = {r['old']: r['new'] for r in repairs}
        for a in pool:
            s = a['symbol']
            if s in repair_map: new_pool.append({"symbol": repair_map[s]})
            elif s not in failed: new_pool.append(a)

        with open(POOL_FILE, "w") as f: json.dump(new_pool, f, indent=4)
        with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)

        # Audit schreiben
        self.write_audit({
            "total": len(pool),
            "success": len(results),
            "repaired": len(repairs),
            "cleaned": len(failed),
            "anchors": sum(1 for r in results if r.get("NewAnchor")),
            "moves": [r['Ticker'] for r in results if r.get("NewAnchor")]
        })
        log("PROGRESS", f"✅ Pulse beendet. Audit in {AUDIT_FILE} gespeichert.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
