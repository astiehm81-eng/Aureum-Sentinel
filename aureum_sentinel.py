import pandas as pd
import yfinance as yf
import os
import json
import threading
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V156 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
ANCHOR_FILE = "anchors_memory.json"
AUDIT_FILE = "heritage_audit.txt"  # Deine lesbare Zusammenfassung

ANCHOR_THRESHOLD = 0.0005 
LOOKBACK_MINUTES = 60      
MAX_WORKERS = 12
RATE_LIMIT_HIT = False

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

    def process_asset(self, symbol):
        global RATE_LIMIT_HIT
        if RATE_LIMIT_HIT: return None
        try:
            time.sleep(random.uniform(0.1, 0.2))
            t = yf.Ticker(symbol)
            df = t.history(period="1d", interval="1m").tail(LOOKBACK_MINUTES)
            
            if df.empty: return {"fail": symbol}

            current_price = df['Close'].iloc[-1]
            
            # Anker-Logik
            last_anchor = self.anchors.get(symbol)
            change = 0
            if last_anchor:
                change = abs(current_price - last_anchor) / last_anchor
            
            if last_anchor is None or change >= ANCHOR_THRESHOLD:
                self.anchors[symbol] = current_price
                log("TICK", f"⚓ {symbol}: {current_price}")
                return {"Ticker": symbol, "Price": current_price, "NewAnchor": True}
            
            return {"Ticker": symbol, "Price": current_price, "NewAnchor": False}
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                RATE_LIMIT_HIT = True
            return {"fail": symbol}

    def write_audit_report(self, stats):
        """Erstellt die für dich lesbare Zusammenfassung."""
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report = [
            f"=== AUREUM SENTINEL AUDIT REPORT [{ts}] ===",
            f"Gescannt: {stats['total']} Assets",
            f"Erfolgreich: {stats['success']}",
            f"Neue Ankerpunkte gesetzt: {stats['anchors']}",
            f"Gereinigte Assets (Delisted): {stats['cleaned']}",
            f"Status: {'⚠️ RATE LIMITED' if RATE_LIMIT_HIT else '✅ NORMAL'}",
            "-" * 40,
            "Top Volatilität / Bewegungen in diesem Lauf:"
        ]
        # Füge die Ticker hinzu, die einen neuen Anker gesetzt haben
        for t in stats['top_moves'][:10]:
            report.append(f" > {t}")
            
        with open(AUDIT_FILE, "w") as f:
            f.write("\n".join(report))

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        symbols = [a['symbol'] for a in pool]
        results = []
        failed_symbols = []
        new_anchors_count = 0
        
        log("SYSTEM", f"Starte Puls-Check für {len(symbols)} Assets...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_asset = {executor.submit(self.process_asset, s): s for s in symbols}
            for future in as_completed(future_to_asset):
                res = future.result()
                if res:
                    if "Price" in res:
                        results.append(res)
                        if res.get("NewAnchor"): new_anchors_count += 1
                    elif "fail" in res and not RATE_LIMIT_HIT:
                        failed_symbols.append(res["fail"])

        # Cleanup & Pool Update
        if failed_symbols:
            new_pool = [a for a in pool if a['symbol'] not in failed_symbols]
            with open(POOL_FILE, "w") as f:
                json.dump(new_pool, f, indent=4)
            log("CLEANUP", f"🧹 {len(failed_symbols)} Assets entfernt.")

        # Audit Statistik erstellen
        stats = {
            "total": len(symbols),
            "success": len(results),
            "anchors": new_anchors_count,
            "cleaned": len(failed_symbols),
            "top_moves": [r['Ticker'] for r in results if r.get("NewAnchor")]
        }
        self.write_audit_report(stats)

        # Anker speichern
        with open(ANCHOR_FILE, "w") as f:
            json.dump(self.anchors, f)

        log("PROGRESS", f"✅ Zyklus beendet. Report erstellt in {AUDIT_FILE}")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
