import pandas as pd
import yfinance as yf
import os
import json
from datetime import datetime

# Neues SDK importieren
try:
    from google import genai
    HAS_AI = True
except ImportError:
    HAS_AI = False

# --- KONFIGURATION V174 ---
POOL_FILE = "isin_pool.json"
AUDIT_FILE = "heritage_audit.txt"
EXPANSION_TARGET = 10000

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def log(tag, msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] [{tag}] {msg}", flush=True)

class AureumSentinel:
    def __init__(self):
        # Initialisierung des neuen Clients
        self.client = genai.Client(api_key=GEMINI_API_KEY) if HAS_AI and GEMINI_API_KEY else None

    def ask_gemini_for_tickers(self, current_count):
        if not self.client:
            log("SKIP", "KI-Client nicht bereit.")
            return []

        prompt = f"""
        Handle als Aureum Sentinel Marktanalyse-Kern. 
        Ziel: 99% Marktabdeckung. Aktuell: {current_count} Assets.
        Nenne mir 100 neue, valide Aktien-Ticker (mit Suffixen wie .DE, .L, .PA, .HK).
        Gib NUR eine kommagetrennte Liste zurück, kein Text, kein Markdown.
        """
        try:
            # Aufruf über das neue SDK (gemini-2.0-flash ist der aktuelle Standard 2026)
            response = self.client.models.generate_content(
                model="gemini-2.0-flash", 
                contents=prompt
            )
            raw_text = response.text.strip()
            return [t.strip().upper() for t in raw_text.split(',')]
        except Exception as e:
            log("ERROR", f"KI-Anfrage fehlgeschlagen: {e}")
            return []

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        
        # KI-Expansion
        added = 0
        if len(current_symbols) < EXPANSION_TARGET:
            log("AI-MINING", "Frage Gemini 2.0 nach neuen Daten...")
            new_tickers = self.ask_gemini_for_tickers(len(current_symbols))
            
            for sym in new_tickers:
                if sym and sym not in current_symbols and len(current_symbols) < EXPANSION_TARGET:
                    pool.append({"symbol": sym})
                    current_symbols.add(sym)
                    added += 1

        with open(POOL_FILE, "w") as f:
            json.dump(pool, f, indent=4)

        # Audit
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        status = "🚀 GEMINI 2.0 READY" if (HAS_AI and GEMINI_API_KEY) else "⚠️ SDK/KEY ISSUE"
        
        report = [
            f"=== AUREUM SENTINEL V174 | NEXT-GEN AI [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"System-Status: {status}",
            f"KI-Injektion: +{added} Assets",
            f"Fortschritt: {round((len(pool)/EXPANSION_TARGET)*100, 2)}%",
            "-" * 40,
            "HINWEIS: Upgrade auf google-genai SDK erfolgreich."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        log("SUCCESS", f"Lauf beendet. Stand: {len(pool)}")

if __name__ == "__main__":
    AureumSentinel().run_cycle()
