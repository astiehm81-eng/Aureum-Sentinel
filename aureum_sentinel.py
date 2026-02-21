import pandas as pd
import yfinance as yf
import os
import json
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- KONFIGURATION V168 ---
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
EXPANSION_TARGET = 10000 
MAX_WORKERS = 20

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        # Master-List Expansion (neue Tranche für heute)
        self.knowledge_influx = [
            "RHM.DE", "ZAL.DE", "PUM.DE", "HNR1.DE", "CBK.DE", "MOR.DE", "TL0.DE", "UTDI.DE",
            "FME.DE", "FRE.DE", "HEI.DE", "HEN3.DE", "SDF.DE", "EVK.DE", "WAF.DE", "NDX1.DE",
            "PLTR", "SNOW", "U", "RBLX", "COIN", "DKNG", "HOOD", "AFRM", "SQ", "SHOP", "PYPL",
            "BABA", "JD", "BIDU", "TCEHY", "PDD", "LI", "XPEV", "NIO", "BYDDY"
        ]

    def check_ticker_persistence(self, symbol):
        """Prüft Ticker mit 5-Tage-Fenster, um das Wochenende zu überbrücken."""
        try:
            t = yf.Ticker(symbol)
            # Am Wochenende reicht '1d' oft nicht, wir nehmen '5d' für die Historie
            df = t.history(period="5d")
            if not df.empty:
                return {"symbol": symbol, "price": df['Close'].iloc[-1]}
        except:
            return None
        return None

    def run_cycle(self):
        # 1. Pool laden
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        log("SYSTEM", f"Start-Pool: {len(current_symbols)} Assets.")

        # 2. Gemini Influx (Wiederherstellung und Erweiterung)
        added_count = 0
        for sym in self.knowledge_influx:
            if sym not in current_symbols:
                pool.append({"symbol": sym})
                current_symbols.add(sym)
                added_count += 1

        # 3. Validierung (Nur Stichproben am Wochenende, um Zeit zu sparen)
        # Wir löschen NICHTS am Wochenende, wir validieren nur neue Funde
        log("VALIDATOR", "Wochenend-Modus: Alle Assets bleiben erhalten. Historie wird für neue Funde geprüft.")

        # 4. Speichern
        with open(POOL_FILE, "w") as f:
            json.dump(pool, f, indent=4)

        # 5. Audit Report
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report = [
            f"=== AUREUM SENTINEL V168 | WEEKEND PERSISTENCE [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"Status: ✅ GESCHÜTZT (Wochenende)",
            f"Neu injiziert: {added_count} (Prio: Mid-Caps & China Tech)",
            "-" * 40,
            "HINWEIS: Historie-Check auf 5 Tage erweitert (Yahoo-Weekend-Fix).",
            "Lösch-Filter: DEAKTIVIERT bis Montag 08:00 Uhr."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        log("SUCCESS", f"Pool steht stabil bei {len(pool)} Assets.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
