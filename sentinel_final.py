import requests
from bs4 import BeautifulSoup
import csv
import time
import os
from datetime import datetime

# Konfiguration der wichtigsten Assets (L&S ISINs/IDs)
ASSETS = {
    "Siemens Energy": "DE000ENER6Y0",
    "Siemens AG": "DE0007236101",
    "Gold (A1KWPQ)": "DE000A1KWPQ1",
    "SAP SE": "DE0007164600",
    "NASDAQ 100": "US6311011026"
}

CSV_FILE = "sentinel_market_data.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def initialize_csv():
    """Löscht die alte Datei und erstellt den Header."""
    with open(CSV_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Asset", "Price", "Exchange"])
    print(f"[*] {CSV_FILE} wurde neu initialisiert.")

def get_ls_price(isin):
    """Liest den aktuellen Kurs von Lang & Schwarz ohne Blocking."""
    url = f"https://www.ls-tc.de/de/aktie/{isin}"
    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=5)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Suche nach dem Preis-Container (L&S spezifisch)
            price_span = soup.find("div", {"class": "mono"}).find("span")
            if price_span:
                return price_span.text.replace('.', '').replace(',', '.')
    except Exception as e:
        return None
    return None

def run_sentinel_logger():
    initialize_csv()
    print("[!] Sentinel Logger gestartet. Drücke Strg+C zum Beenden.")
    
    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            for name, isin in ASSETS.items():
                price = get_ls_price(isin)
                if price:
                    writer.writerow([timestamp, name, price, "L&S"])
                    # Sofortiger Ankerpunkt-Check (0,1% Regel)
                    print(f"[{timestamp}] {name}: {price} € (Logged)")
                time.sleep(1) # Schutz gegen Blocking
        
        time.sleep(10) # Intervall zwischen den Scans

if __name__ == "__main__":
    run_sentinel_logger()
