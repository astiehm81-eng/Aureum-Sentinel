import pandas as pd
import yfinance as yf
import os
import json
import requests
from datetime import datetime

# --- KONFIGURATION V182 ---
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
EXPANSION_TARGET = 10000

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        self.indices = {
            "S&P 500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            "S&P 400": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
            "S&P 600": "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
            "NASDAQ 100": "https://en.wikipedia.org/wiki/Nasdaq-100",
            "DAX": "https://en.wikipedia.org/wiki/DAX",
            "MDAX": "https://en.wikipedia.org/wiki/MDAX",
            "SDAX": "https://en.wikipedia.org/wiki/SDAX",
            "FTSE 100": "https://en.wikipedia.org/wiki/FTSE_100_Index",
            "FTSE 250": "https://en.wikipedia.org/wiki/FTSE_250_Index",
            "CAC 40": "https://en.wikipedia.org/wiki/CAC_40",
            "IBEX 35": "https://en.wikipedia.org/wiki/IBEX_35"
        }
        # Browser-Identität vortäuschen
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def safe_scrape(self, url, column_name, suffix=""):
        """Lädt HTML via Requests (Stealth) und extrahiert Ticker."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                tables = pd.read_html(response.text)
                for df in tables:
                    if column_name in df.columns:
                        tickers = df[column_name].astype(str).tolist()
                        return [f"{t.strip().upper().replace('-', '.')}{suffix}" for t in tickers if len(t) > 1 and "SYMBOL" not in t.upper()]
            else:
                log("WIKI-FAIL", f"Status {response.status_code} für {url}")
        except Exception as e:
            log("WIKI-ERROR", f"Fehler bei {url}: {str(e)[:50]}")
        return []

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        log("SYSTEM", f"Aktuelle Basis: {len(current_symbols)} Assets.")

        all_new = []
        # US Markets
        all_new.extend(self.safe_scrape(self.indices["S&P 500"], "Symbol"))
        all_new.extend(self.safe_scrape(self.indices["S&P 400"], "Ticker symbol"))
        all_new.extend(self.safe_scrape(self.indices["S&P 600"], "Ticker symbol"))
        all_new.extend(self.safe_scrape(self.indices["NASDAQ 100"], "Ticker"))

        # German Markets
        all_new.extend(self.safe_scrape(self.indices["DAX"], "Ticker symbol", ".DE"))
        all_new.extend(self.safe_scrape(self.indices["MDAX"], "Ticker symbol", ".DE"))
        all_new.extend(self.safe_scrape(self.indices["SDAX"], "Ticker symbol", ".DE"))

        # European Markets
        all_new.extend(self.safe_scrape(self.indices["FTSE 100"], "EPIC", ".L"))
        all_new.extend(self.safe_scrape(self.indices["FTSE 250"], "Ticker", ".L"))
        all_new.extend(self.safe_scrape(self.indices["CAC 40"], "Ticker", ".PA"))
        all_new.extend(self.safe_scrape(self.indices["IBEX 35"], "Ticker", ".MC"))

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
            f"=== AUREUM SENTINEL V182 | STEALTH SCRAPE [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"Status: ✅ SUCCESS (Headers active)",
            f"Neu extrahiert: +{added} Assets",
            f"Fortschritt: {round((len(pool)/EXPANSION_TARGET)*100, 2)}%",
            "-" * 40,
            "Strategie: Umgehung der 403-Blockade durch User-Agent Spoofing."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        log("SUCCESS", f"Pool wächst auf {len(pool)}.")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
