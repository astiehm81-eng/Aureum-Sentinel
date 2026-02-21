import pandas as pd
import yfinance as yf
import os
import json
import time
import random
from datetime import datetime

try:
    from google import genai
    HAS_AI = True
except ImportError:
    HAS_AI = False

# --- KONFIGURATION V178 ---
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
EXPANSION_TARGET = 10000
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY) if HAS_AI and GEMINI_API_KEY else None

    def get_wiki_tickers(self):
        """Scraped die wichtigsten Indizes direkt von Wikipedia."""
        log("WIKI-MINING", "Starte Fallback-Suche auf Wikipedia...")
        urls = {
            "SP500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            "DAX": "https://en.wikipedia.org/wiki/DAX",
            "NASDAQ100": "https://en.wikipedia.org/wiki/Nasdaq-100"
        }
        wiki_tickers = []
        try:
            # S&P 500
            table = pd.read_html(urls["SP500"])[0]
            wiki_tickers.extend(table['Symbol'].tolist())
            
            # DAX (Suffix .DE hinzufügen)
            table_dax = pd.read_html(urls["DAX"])[4] # Tabelle 4 ist meist der aktuelle Index
            dax_symbols = table_dax['Ticker symbol'].tolist()
            wiki_tickers.extend([f"{s}.DE" for s in dax_symbols])
            
            log("WIKI-SUCCESS", f"{len(wiki_tickers)} Ticker von Wikipedia geladen.")
            return wiki_tickers
        except Exception as e:
            log("WIKI-ERROR", f"Wikipedia-Scraping fehlgeschlagen: {e}")
            return []

    def get_ai_bulk(self, current_count):
        """Standard KI-Anfrage."""
        if not self.client: return []
        prompt = f"Gib mir 300 internationale Aktien-Ticker (kommagetrennt). Fokus Mid-Caps. Aktuell: {current_count}."
        try:
            response = self.client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            return [t.strip().upper() for t in response.text.split(',') if len(t.strip()) > 1]
        except Exception as e:
            log("AI-ERROR", f"KI-Quota vermutlich erschöpft: {e}")
            return []

    def run_cycle(self):
        # 1. Pool laden
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        initial_count = len(current_symbols)

        # 2. Zweigleisige Suche
        new_candidates = []
        
        # Weg A: KI (wenn möglich)
        ai_finds = self.get_ai_bulk(initial_count)
        new_candidates.extend(ai_finds)

        # Weg B: Wikipedia (Immer als Backup oder Ergänzung)
        if len(new_candidates) < 50: # Wenn KI nichts liefert oder wenig liefert
            wiki_finds = self.get_wiki_tickers()
            new_candidates.extend(wiki_finds)

        # 3. Integration
        added = 0
        for sym in new_candidates:
            if sym and sym not in current_symbols and len(current_symbols) < EXPANSION_TARGET:
                pool.append({"symbol": sym})
                current_symbols.add(sym)
                added += 1

        # 4. Speichern & Audit
        with open(POOL_FILE, "w") as f:
            json.dump(pool, f, indent=4)

        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report = [
            f"=== AUREUM SENTINEL V178 | HYBRID VOYAGER [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"Zuwachs: +{added}",
            f"Status: {'🤖 AI + 🌐 WIKI' if added > 0 else '💤 IDLE'}",
            "-" * 40,
            "Strategie: Zweigleisiges Mining (AI & Wikipedia Scraper)."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        log("SUCCESS", f"Lauf beendet. Stand: {len(pool)}")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
