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

# --- KONFIGURATION V176 ---
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

    def get_bulk_tickers(self, current_count):
        """Macht EINE große Anfrage statt vieler kleiner."""
        if not self.client:
            return []

        # Wir fragen nach einer massiven Liste in einem Rutsch
        prompt = f"""
        Handle als strategischer Markt-Analyst. Wir benötigen 500 Ticker-Symbole 
        für globale Aktien (Fokus: US Mid-Caps, EU Small-Caps, Asian Growth). 
        Wir haben bereits {current_count} Assets.
        Gib NUR die Ticker als kommagetrennte Liste zurück. Keine Erklärungen.
        Format-Beispiel: AAPL, SAP.DE, 7203.T, VOD.L
        """
        
        try:
            log("AI-BULK", "Sende Bulk-Anfrage an Gemini 2.0...")
            response = self.client.models.generate_content(
                model="gemini-2.0-flash", 
                contents=prompt
            )
            # API-Schonfrist nach dem Call
            time.sleep(5) 
            
            raw_text = response.text.strip().replace("`", "").replace("\n", "")
            return [t.strip().upper() for t in raw_text.split(',') if len(t.strip()) > 1]
        except Exception as e:
            if "429" in str(e):
                log("QUOTA", "API gedrosselt. Wartezeit einhalten.")
            else:
                log("ERROR", f"Fehler bei Bulk-Anfrage: {e}")
            return []

    def run_cycle(self):
        if not os.path.exists(POOL_FILE): pool = []
        else:
            with open(POOL_FILE, "r") as f: pool = json.load(f)
        
        current_symbols = {a['symbol'] for a in pool}
        initial_len = len(current_symbols)

        if initial_len < EXPANSION_TARGET:
            # Nur ein einziger API Call pro Zyklus!
            new_candidates = self.get_bulk_tickers(initial_len)
            
            added = 0
            for sym in new_candidates:
                if sym not in current_symbols and len(current_symbols) < EXPANSION_TARGET:
                    pool.append({"symbol": sym})
                    current_symbols.add(sym)
                    added += 1
            
            log("SUCCESS", f"Bulk-Injektion abgeschlossen: +{added} Assets.")

        # Speichern
        with open(POOL_FILE, "w") as f:
            json.dump(pool, f, indent=4)

        # Audit
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        progress = round((len(pool)/EXPANSION_TARGET)*100, 2)
        report = [
            f"=== AUREUM SENTINEL V176 | BULK-MODE [{ts}] ===",
            f"Pool-Größe: {len(pool)} / {EXPANSION_TARGET}",
            f"Strategie: Ein Bulk-Request pro 15 Min.",
            f"Fortschritt: {progress}%",
            "-" * 40,
            "HINWEIS: Quota-Schutz durch Request-Bündelung aktiv."
        ]
        with open(AUDIT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(report))

if __name__ == "__main__":
    AureumSentinel().run_cycle()
