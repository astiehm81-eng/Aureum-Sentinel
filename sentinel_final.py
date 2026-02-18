import requests
from bs4 import BeautifulSoup
import csv
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Konfiguration
CSV_FILE = "sentinel_market_data.csv"
ASSETS = {
    "Siemens_Energy": "DE000ENER6Y0",
    "Gold_A1KWPQ": "DE000A1KWPQ1",
    "SAP_SE": "DE0007164600",
    "NASDAQ_100": "US6311011026"
}

# Header um Blocking zu vermeiden
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def fetch_asset(name, isin):
    url = f"https://www.ls-tc.de/de/aktie/{isin}"
    try:
        # Session nutzen für bessere Performance bei vielen Workern
        with requests.Session() as s:
            response = s.get(url, headers=HEADERS, timeout=5)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # Die L&S mono Klassen für Geld/Brief
                spans = soup.find_all("div", {"class": "mono"})
                if len(spans) >= 3:
                    ask = spans[2].text.strip().replace('.', '').replace(',', '.')
                    return [name, ask]
    except Exception as e:
        return [name, None]
    return [name, None]

def run_sentinel():
    # 1. Einmaliges Löschen beim Start
    if os.path.exists(CSV_FILE):
        os.remove(CSV_FILE)
    
    # 2. Header schreiben
    with open(CSV_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Asset", "Ask"])

    print(f"[*] Aureum Sentinel GitHub-Worker gestartet...")

    # 3. Endlosschleife für Daten-Logging
    while True:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Parallelisierung mit 8 Workern
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda p: fetch_asset(*p), ASSETS.items()))
        
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            for name, ask in results:
                if ask:
                    writer.writerow([ts, name, ask])
                    # Sofortiger Output für das GitHub-Log
                    print(f"[{ts}] {name}: {ask} €")
        
        # Kurze Pause für die Stabilität
        time.sleep(0.5)

if __name__ == "__main__":
    run_sentinel()
