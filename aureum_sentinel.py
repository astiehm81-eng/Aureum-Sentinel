import pandas as pd
import yfinance as yf
import os
import json
from datetime import datetime

# --- KONFIGURATION V181 ---
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
EXPANSION_TARGET = 10000

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        # Massive Erweiterung der Quellen für 99% Abdeckung
        self.indices = {
            "S&P 500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            "S&P 400 (MidCap)": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
            "S&P 600 (SmallCap)": "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
            "NASDAQ 100": "https://en.wikipedia.org/wiki/Nasdaq-100",
            "DAX": "https://en.wikipedia.org/wiki/DAX",
            "MDAX": "https://en.wikipedia.org/wiki/MDAX",
            "SDAX": "https://en.wikipedia.org/wiki/SDAX",
            "FTSE 100": "https://en.wikipedia.org/wiki/FTSE_100_Index",
            "FTSE 250": "https://en.wikipedia.org/wiki/FTSE_250_Index",
            "CAC 40": "https://en.wikipedia.org/wiki/CAC_40",
            "IBEX 35": "https://en.wikipedia.org/wiki/IBEX_35"
        }

    def safe_scrape(self, url, column_name, suffix=""):
        """Hilfsfunktion zum sicheren Scrapen einer Tabelle."""
        try:
            tables = pd.read_html(url)
            # Wir suchen die Tabelle, die den Spaltennamen enthält
            for df in tables:
                if column_name in df.columns:
                    tickers = df[column_name].astype(str).tolist()
                    return [f"{t.strip().upper().replace('-', '.')}{suffix}" for t in tickers if len(t) > 1]
        except Exception as e:
            log("WIKI-WARN", f"Konnte {url} nicht voll lesen: {e}")
        return []

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        log("SYSTEM", f"Aktuelle Basis: {len(current_symbols)} Assets.")

        # Marktabdeckung Schritt für Schritt
        all_new = []
        
        # 1. USA (S&P 500, 400, 600 + Nasdaq) -> Deckt ca. 90% des US Marktes ab
        all_new.extend(self.safe_scrape(self.indices["S&P 500"], "Symbol"))
        all_new.extend(self.safe_scrape(self.indices["S&P 400 (MidCap)"], "Ticker symbol"))
        all_new.extend(self.safe_scrape(self.indices["S&P 600 (SmallCap)"], "Ticker symbol"))
        all_new.extend(self.safe_scrape(self.indices["NASDAQ 100"], "Ticker"))

        # 2. Deutschland (DAX, MDAX, SDAX) -> Deckt fast 100% des dt. Hauptmarktes ab
        all_new.extend(self.safe_scrape(self.indices["DAX"], "Ticker symbol", ".DE"))
        all_new.extend(self.safe_scrape(self.indices["MDAX"], "Ticker symbol", ".DE"))
        all_new.extend(self.safe_scrape(self.indices["SDAX"], "Ticker symbol", ".DE"))

        # 3. Europa (UK, FR, ES)
        all_new.extend(self.safe_scrape(self.indices["FTSE 100"], "EPIC", ".L"))
        all_new.extend(self.safe_scrape(self.indices["FTSE 250"], "Ticker", ".L"))
        all_new.extend(self.safe_scrape(self.indices["CAC 40"], "Ticker", ".PA"))
        all_new.extend(self.safe_scrape(self.indices["IBEX 35"], "Ticker", ".MC"))

        # Integration in den Pool
        added = 0
        for sym in all_new:
            if sym not in current_symbols and len(current_symbols) < EXPANSION_TARGET:
                pool.append({"symbol": sym})
                current_symbols.add(sym)
                added += 1

        with open(POOL_FILE, "w") as f:
            json.dump(pool, f, indent=4)

        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report = [
            f"=== AUREUM SENTINEL V181 | 99% BROAD COVERAGE [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"Neu durch Broad-Scrape: +{added} Assets",
            f"Abgedeckte Indizes: S&P 500/400/600, NASDAQ100, DAX/MDAX/SDAX, FTSE, CAC, IBEX",
            "-" * 40,
            "Strategie: Volle Marktabdeckung über Blue-Chips, Mid-Caps und Small-Caps."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        log("SUCCESS", f"Lauf beendet. Pool wächst auf {len(pool)}.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
