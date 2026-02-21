import pandas as pd
import yfinance as yf
import os
import json
from datetime import datetime

# Versuch, das KI-Modul zu laden, sonst Fallback auf "Safe Mode"
try:
    import google.generativeai as genai
    HAS_AI = True
except ImportError:
    HAS_AI = False

# --- KONFIGURATION V173 ---
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
EXPANSION_TARGET = 10000

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if HAS_AI and GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        self.model = genai.GenerativeModel('gemini-1.5-flash') if HAS_AI and GEMINI_API_KEY else None

    def ask_gemini_for_tickers(self, current_count):
        if not self.model:
            log("SKIP", "KI-Modul nicht installiert oder Key fehlt. Nutze Master-Load.")
            return ["AAPL", "SAP.DE", "MSFT", "NVDA", "RHM.DE"]

        prompt = f"Gib mir 100 Ticker-Symbole (kommagetrennt) für den Aktienmarkt (Prio Mid-Caps), die noch nicht in einer Liste von {current_count} Werten sind. Nur Ticker, kein Text."
        try:
            response = self.model.generate_content(prompt)
            return [t.strip().upper() for t in response.text.split(',')]
        except Exception as e:
            log("ERROR", f"API-Fehler: {e}")
            return []

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        
        # KI-Injektion
        new_tickers = self.ask_gemini_for_tickers(len(current_symbols))
        added = 0
        for sym in new_tickers:
            if sym not in current_symbols and len(current_symbols) < EXPANSION_TARGET:
                pool.append({"symbol": sym})
                current_symbols.add(sym)
                added += 1

        with open(POOL_FILE, "w") as f:
            json.dump(pool, f, indent=4)

        # Audit
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        status = "🤖 AI-READY" if HAS_AI else "⚠️ MODULE MISSING"
        report = [
            f"=== AUREUM SENTINEL V173 | RECOVERY [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"Status: {status}",
            f"Zuwachs: +{added} Assets",
            "-" * 40,
            "HINWEIS: Prüfe GitHub Workflow auf 'pip install google-generativeai'."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        log("SUCCESS", f"Lauf beendet. Pool: {len(pool)}")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
