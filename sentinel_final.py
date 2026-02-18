import requests
import csv
import os
import time
from datetime import datetime

# Assets laut deiner Vorgabe (erweiterbar)
ASSETS = [
    "ENER61", "SAP000", "A1EWWW", "A0AE1X", "BASF11", "DTE000", "VOW300", 
    "ADS000", "DBK100", "ALV001", "BAY001", "BMW111", "IFX000", "MUV200"
]

def get_robust_history(wkn):
    """Holt die maximale Historie direkt vom L&S JSON-Endpunkt."""
    url = f"https://www.ls-tc.de/_rpc/json/chart/chart.json?symbol={wkn}&range=max"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }
    
    for attempt in range(3):  # 3 Versuche bei Netzwerkfehlern
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # Tiefe Pfad-Validierung, damit das Skript bei Strukturänderungen nicht crasht
            series = data.get('series', {}).get('main', {}).get('data', [])
            if not series:
                print(f"⚠️ Keine Daten für {wkn} gefunden.")
                return []
            
            rows = []
            for entry in series:
                if len(entry) >= 2:
                    dt = datetime.fromtimestamp(entry[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    price = float(entry[1])
                    rows.append([dt, wkn, price])
            
            print(f"✅ {wkn}: {len(rows)} Datenpunkte extrahiert.")
            return rows

        except Exception as e:
            print(f"🔄 Versuch {attempt+1} für {wkn} fehlgeschlagen: {e}")
            time.sleep(2) # Kurze Pause vor Neustart
    return []

if __name__ == "__main__":
    start_time = time.time()
    master_file = 'sentinel_deep_history.csv'
    all_data = []

    print(f"🚀 Starte Deep-History Import für {len(ASSETS)} Assets...")

    for wkn in ASSETS:
        asset_data = get_robust_history(wkn)
        all_data.extend(asset_data)
        time.sleep(0.5) # Anti-Blocking Delay

    if all_data:
        # Robustes Schreiben: Überschreibt die Master-Datei mit dem kompletten neuen Stand
        try:
            with open(master_file, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Timestamp', 'WKN', 'Price'])
                writer.writerows(all_data)
            print(f"💾 Datei gespeichert: {master_file} ({len(all_data)} Zeilen)")
        except IOError as e:
            print(f"❌ Dateifehler: {e}")
    
    print(f"🏁 Prozess beendet in {round(time.time() - start_time, 2)}s")
