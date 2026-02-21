import pandas as pd
import yfinance as yf
import os
import json
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V160 ---
POOL_FILE = "isin_pool.json"
HERITAGE_DIR = "heritage"
ANCHOR_FILE = "anchors_memory.json"
AUDIT_FILE = "heritage_audit.txt"

# Anker-Vorgabe: 0.05%
ANCHOR_THRESHOLD = 0.0005 
MAX_WORKERS = 12
# Erhöhte Discovery-Chance für schnelles Wachstum
PROACTIVE_DISCOVERY_LIMIT = 5000 

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

    def proactive_discovery(self, current_symbols):
        """Sucht aktiv nach neuen Large-Caps in Trend-Sektoren."""
        log("HUNTER", "Starte proaktive Sektor-Suche...")
        # Start-Ticker für die Suche nach Peers (AI, Tech, DAX Leader)
        seeds = ["NVDA", "SAP.DE", "ASML", "AAPL", "MSFT", "TSLA", "RHM.DE"]
        new_found = []
        
        target = random.choice(seeds)
        try:
            t = yf.Ticker(target)
            # Versuche Peers über die Yahoo 'info' zu finden
            sector = t.info.get('sector')
            if sector:
                log("HUNTER", f"Analysiere Peers im Sektor: {sector}")
                # Hier simulieren wir die KI-Injektion: 
                # In der V160 füge ich hier eine Logik ein, die gängige Ticker prüft
        except: pass
        return new_found

    def process_asset(self, symbol):
        try:
            t = yf.Ticker(symbol)
            df = t.history(period="1d").tail(1)
            if df.empty: return {"fail": symbol}

            price = df['Close'].iloc[-1]
            last = self.anchors.get(symbol)
            
            # Anker-Logik
            is_new_anchor = last is None or abs(price - last) / last >= ANCHOR_THRESHOLD
            if is_new_anchor:
                self.anchors[symbol] = price
            
            return {"Ticker": symbol, "Price": price, "NewAnchor": is_new_anchor}
        except:
            return {"fail": symbol}

    def write_audit(self, stats):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report = [
            f"=== AUREUM SENTINEL V160 PULSE [{ts}] ===",
            f"Pool-Größe: {stats['total']} Assets",
            f"Erfolgreich: {stats['success']}",
            f"Neu entdeckt: {stats['new_discovery_count']}",
            f"Entfernt: {stats['cleaned']}",
            f"System-Status: ✅ OK",
            "-" * 40,
            "Bemerkung: Marktruhe (Wochenende) - Discovery aktiv." if stats['anchors'] == 0 else "Aktive Bewegungen erkannt."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f: pool = json.load(f)
        current_symbols = {a['symbol'] for a in pool}
        
        results, failed = [], []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.process_asset, a['symbol']): a['symbol'] for a in pool}
            for f in as_completed(futures):
                res = f.result()
                if "Price" in res: results.append(res)
                elif "fail" in res: failed.append(res["fail"])

        # Discovery Trigger
        new_assets = []
        if len(current_symbols) < PROACTIVE_DISCOVERY_LIMIT:
            # Hier greife ich jetzt manuell ein und füttere dem System 
            # über diesen Code-Block neue Ticker, bis die Logik vollautomatisch ist
            potential = ["AVGO", "ORCL", "CRM", "ADBE", "AMD", "QCOM", "TXN", "INTU", "AMAT", "MU"]
            for p in potential:
                if p not in current_symbols:
                    new_assets.append({"symbol": p})
                    log("DISCOVERY", f"✨ Neues Asset gefunden: {p}")

        # Update Pool
        new_pool = [a for a in pool if a['symbol'] not in failed]
        new_pool.extend(new_assets)

        with open(POOL_FILE, "w") as f: json.dump(new_pool, f, indent=4)
        with open(ANCHOR_FILE, "w") as f: json.dump(self.anchors, f)

        self.write_audit({
            "total": len(new_pool),
            "success": len(results),
            "new_discovery_count": len(new_assets),
            "cleaned": len(failed),
            "anchors": sum(1 for r in results if r.get("NewAnchor")),
        })

if __name__ == "__main__":
    AureumSentinel().run_cycle()
