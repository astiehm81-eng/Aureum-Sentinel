import requests
from bs4 import BeautifulSoup
import csv
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Konfiguration der Assets
ASSETS = {
    "Siemens Energy": "DE000ENER6Y0",
    "Siemens AG": "DE0007236101",
    "Gold (A1KWPQ)": "DE000A1KWPQ1",
    "SAP SE": "DE0007164600",
    "NASDAQ 100": "US6311011026"
}

CSV_FILE = "sentinel_market_data.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MAX_WORKERS = 10 # Wie in der erfolgreichen Historie besprochen

def fetch_asset_data(name, isin):
    """Holt Geld/Brief-Daten von L&S ohne Blocking."""
    url = f"https://www.ls-tc.de/de/aktie/{isin}"
    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=3)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Suche nach den Bid/Ask Containern (Brief-Wert)
            # In der L&S Struktur sind das oft die mono-Klassen unter dem Hauptkurs
            spans = soup.find_all("div", {"class": "mono"})
            
            # Index-Logik aus unserem alten Skript: 
            # Meistens: [0] = Aktueller Kurs, [1] = Geld (Bid), [2] = Brief (Ask)
            if len(spans) >= 3:
                bid = spans[1].text.strip().replace('.', '').replace(',', '.')
                ask = spans[2].text.strip().replace('.', '').replace(',', '.')
                return [name, bid, ask]
    except Exception:
        pass
    return [name, None, None]

def initialize_csv():
    if os.path.exists(CSV_FILE):
        os.remove(CSV_FILE)
    with open(CSV_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Asset", "Bid", "Ask", "Source"])
    print(f"[*] {CSV_FILE} mit Brief-Wert-Spalte neu initialisiert.")

def run_sentinel():
    initialize_csv()
    print(f"[!] Start mit {MAX_WORKERS} Workern...")
    
    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Parallelisiertes Abgreifen aller Assets
            futures = [executor.submit(fetch_asset_data, name, isin) for name, isin in ASSETS.items()]
            results = [f.result() for f in futures]
            
        # Sofortiges Schreiben in die CSV
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            for asset_name, bid, ask in results:
                if ask: # Fokus auf Brief-Wert-Validität
                    writer.writerow([timestamp, asset_name, bid, ask, "L&S_Direct"])
        
        # Minimale Pause für 1ms-Struktur (Latenz kommt durch Netz-I/O)
        time.sleep(0.01)

if __name__ == "__main__":
    run_sentinel()
