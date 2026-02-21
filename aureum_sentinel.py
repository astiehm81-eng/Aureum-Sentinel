import pandas as pd
import yfinance as yf
import os
import json
from datetime import datetime
import google.generativeai as genai

# --- KONFIGURATION V172 ---
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
EXPANSION_TARGET = 10000

# Hole den API-Key aus den Umgebungsvariablen (GitHub Secrets)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        self.model = genai.GenerativeModel('gemini-1.5-flash') if GEMINI_API_KEY else None

    def ask_gemini_for_tickers(self, current_count):
        """Ruft aktiv die KI auf, um das nächste 1000er Paket an ISINs/Tickern zu generieren."""
        if not self.model:
            log("WARNING", "Kein API-Key gefunden. Nutze Fallback-Logik.")
            return ["AAPL", "SAP.DE", "MSFT", "NVDA", "TSLA"] # Minimaler Fallback

        prompt = f"""
        Du bist der Core-Agent des Aureum Sentinel. Dein Ziel ist 99% Marktabdeckung.
        Wir haben aktuell {current_count} Assets.
        Nenne mir 100 neue, valide Aktien-Ticker (inkl. Suffixe wie .DE, .L, .PA) aus dem Bereich 
        Mid-Caps, Small-Caps und internationale Emerging Markets.
        Gib NUR eine kommagetrennte Liste der Ticker zurück, kein Text.
        """
        try:
            response = self.model.generate_content(prompt)
            ticker_list = response.text.strip().split(',')
            return [t.strip().upper() for t in ticker_list]
        except Exception as e:
            log("ERROR", f"Gemini API Call fehlgeschlagen: {e}")
            return []

    def run_cycle(self):
        # 1. Pool laden
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        
        # 2. Aktive KI-Suche
        if len(current_symbols) < EXPANSION_TARGET:
            log("AI-SEARCH", "Frage Gemini nach neuen Markt-Daten...")
            new_tickers = self.ask_gemini_for_tickers(len(current_symbols))
            
            added_this_run = 0
            for sym in new_tickers:
                if sym not in current_symbols:
                    pool.append({"symbol": sym})
                    current_symbols.add(sym)
                    added_this_run += 1
            
            log("AI-SEARCH", f"✅ Gemini hat {added_this_run} neue Assets identifiziert.")

        # 3. Speichern
        with open(POOL_FILE, "w") as f:
            json.dump(pool, f, indent=4)

        # 4. Audit
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report = [
            f"=== AUREUM SENTINEL V172 | AI-LIVE-EXPANSION [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"KI-Entdeckungen: {added_this_run if 'added_this_run' in locals() else 0}",
            f"Status: {'🤖 AI-CONNECTED' if GEMINI_API_KEY else '⚠️ STANDALONE'}",
            "-" * 40,
            "Strategie: Direkte Anbindung an Gemini-Core für 99% Abdeckung."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))

if __name__ == "__main__":
    AureumSentinel().run_cycle()
