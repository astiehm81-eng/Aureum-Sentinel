import csv
import time
from datetime import datetime
import os

# --- TEST-KONFIGURATION (V52 - No Pandas Version) ---
FILENAME = "sentinel_market_data.csv"
RUNTIME_LIMIT = 60  # TESTMODUS: 1 Minute
ANCHOR_THRESHOLD = 0.001 

# Sektoren-Mapping
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
    log_status("Aureum Sentinel TEST-RUN (No-Pandas) gestartet.")
    
    # Datei-Header schreiben, falls sie neu ist
    if not os.path.exists(FILENAME):
        with open(FILENAME, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Asset", "Price", "Sector"])
        log_status("Neue CSV-Datei mit Header erstellt.")

    for sector, tickers in ASSETS.items():
        if time.time() - start_time > RUNTIME_LIMIT:
            log_status("Zeitlimit erreicht. Beende...")
            break

        # Daten sammeln und SOFORT schreiben (Eiserner Standard)
        with open(FILENAME, 'a', newline='') as f:
            writer = csv.writer(f)
            for ticker in tickers:
                try:
                    # Hier erfolgt der Tradegate-Abruf (simuliert für Test)
                    current_price = 123.45 
                    
                    timestamp = datetime.now().isoformat()
                    writer.writerow([timestamp, ticker, current_price, sector])
                    
                    # Live-Log für GitHub Actions
                    log_status(f"READ SUCCESS: {ticker} -> {current_price}€")
                    
                except Exception as e:
                    log_status(f"ERROR bei {ticker}: {str(e)}")
            
            # Force-Flush: Datei wird nach jedem Sektor sicher auf Disk geschrieben
            f.flush()
            log_status(f"SPEICHER-CHECK: Sektor {sector} erfolgreich weggeschrieben.")

    log_status("Zyklus beendet. Daten sind bereit für den Push.")

if __name__ == "__main__":
    run_sentinel()
