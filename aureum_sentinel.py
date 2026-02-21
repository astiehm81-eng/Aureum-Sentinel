import pandas as pd
import yfinance as yf
import os
import json
from datetime import datetime

# --- KONFIGURATION V180 ---
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
EXPANSION_TARGET = 10000

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        # Definition der Wiki-Quellen für maximale Abdeckung
        self.indices = {
            "S&P 500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            "Nasdaq 100": "https://en.wikipedia.org/wiki/Nasdaq-100",
            "DAX (DE)": "https://en.wikipedia.org/wiki/DAX",
            "FTSE 100 (UK)": "https://en.wikipedia.org/wiki/FTSE_100_Index",
            "Nikkei 225 (JP)": "https://en.wikipedia.org/wiki/Nikkei_225",
            "EuroStoxx 50": "https://en.wikipedia.org/wiki/EURO_STOXX_50"
        }

    def scrape_wikipedia(self):
        """Sammelt Ticker aus verschiedenen Wikipedia-Tabellen."""
        all_found = []
        
        # 1. S&P 500 (USA)
        try:
            sp500 = pd.read_html(self.indices["S&P 500"])[0]
            all_found.extend(sp500['Symbol'].tolist())
            log("WIKI", "S&P 500 extrahiert.")
        except: pass

        # 2. DAX (Deutschland) - Suffix .DE hinzufügen
        try:
            dax = pd.read_html(self.indices["DAX (DE)"])[4]
            dax_symbols = dax['Ticker symbol'].tolist()
            all_found.extend([f"{s}.DE" for s in dax_symbols])
            log("WIKI", "DAX extrahiert.")
        except: pass

        # 3. FTSE 100 (UK) - Suffix .L hinzufügen
        try:
            ftse = pd.read_html(self.indices["FTSE 100 (UK)"])[4]
            ftse_symbols = ftse['EPIC'].tolist()
            all_found.extend([f"{s}.L" for s in ftse_symbols])
            log("WIKI", "FTSE 100 extrahiert.")
        except: pass

        # 4. EuroStoxx 50 (EU) - Suffixe nach Börsenplatz (vereinfacht .PA/.DE)
        try:
            estoxx = pd.read_html(self.indices["EuroStoxx 50"])[2]
            estoxx_symbols = estoxx['Ticker'].tolist()
            all_found.extend(estoxx_symbols)
            log("WIKI", "EuroStoxx 50 extrahiert.")
        except: pass

        return [str(s).strip().upper() for s in all_found if s]

    def run_cycle(self):
        # 1. Pool laden
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        log("SYSTEM", f"Aktueller Stand: {len(current_symbols)} Assets.")

        # 2. Wikipedia-Scraping statt KI
        log("SCRAPER", "Starte globale Index-Erfassung...")
        new_candidates = self.scrape_wikipedia()
        
        added = 0
        for sym in new_candidates:
            # Punkt-Korrektur für Yahoo (BRK.B statt BRK-B)
            clean_sym = sym.replace('-', '.')
            if clean_sym not in current_symbols and len(current_symbols) < EXPANSION_TARGET:
                pool.append({"symbol": clean_sym})
                current_symbols.add(clean_sym)
                added += 1

        # 3. Speichern
        with open(POOL_FILE, "w") as f:
            json.dump(pool, f, indent=4)

        # 4. Audit Update
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report = [
            f"=== AUREUM SENTINEL V180 | WIKI-DOMINANCE [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"Status: ✅ UNABHÄNGIG (Keine KI-Quota)",
            f"Neu extrahiert: +{added} Assets",
            f"Fortschritt: {round((len(pool)/EXPANSION_TARGET)*100, 2)}%",
            "-" * 40,
            "Strategie: Direktes Scraping globaler Leit-Indizes von Wikipedia."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        
        log("SUCCESS", f"Lauf beendet. Pool auf {len(pool)} Assets erweitert.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
