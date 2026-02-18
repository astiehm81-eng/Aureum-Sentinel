import requests
import pandas as pd
import time
from datetime import datetime

# --- TEST-KONFIGURATION (V52) ---
FILENAME = "sentinel_market_data.csv"
RUNTIME_LIMIT = 60  # TESTMODUS: 1 Minute Laufzeit
ANCHOR_THRESHOLD = 0.001  # 0.1% für neue Ankerpunkte

# Sektoren-Mapping (Beispielhaft für den Test)
ASSETS = {
    "DAX": ["SAP", "SIE", "DTE", "AIR", "ALV"],
    "MDAX": ["LHA", "EVK", "BOSS"],
    "US_TECH": ["AAPL", "TSLA", "NVDA", "MSFT"]
}

def log_status(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] 🛡️ {message}")

def run_sentinel():
    start_time = time.time()
    log_status("Aureum Sentinel TEST-RUN (60s) gestartet.")
    
    # Vorhandene Daten laden
    try:
        df_history = pd.read_csv(FILENAME)
        log_status(f"Historie geladen ({len(df_history)} Einträge).")
    except:
        df_history = pd.DataFrame(columns=["Timestamp", "Asset", "Price", "Sector"])
        log_status("Neue CSV wird initialisiert.")

    new_entries = []

    for sector, tickers in ASSETS.items():
        # Zeitlimit-Check für den 60s Test
        if time.time() - start_time > RUNTIME_LIMIT:
            log_status("60s Zeitlimit erreicht. Beende Test und speichere...")
            break

        for ticker in tickers:
            try:
                # Hier Simulation des Tradegate-Abrufs
                # (Ersetze dies durch deine spezifische Request-Logik)
                current_price = 100.00 
                
                # JEDER erfolgreiche Read wird sofort geloggt
                log_status(f"DATA SUCCESS: {ticker} ({sector}) geloggt.")
                
                new_entries.append({
                    "Timestamp": datetime.now().isoformat(),
                    "Asset": ticker,
                    "Price": current_price,
                    "Sector": sector
                })
                
            except Exception as e:
                log_status(f"ERROR bei {ticker}: {str(e)}")

        # Sofort-Speicherung nach jedem Sektor
        if new_entries:
            df_new = pd.DataFrame(new_entries)
            df_final = pd.concat([df_history, df_new]).tail(1000)
            df_final.to_csv(FILENAME, index=False)
            log_status(f"SPEICHER-CHECK: Sektor {sector} in CSV gesichert.")

    log_status("Test-Zyklus beendet. Bereit für GitHub-Push.")

if __name__ == "__main__":
    run_sentinel()
