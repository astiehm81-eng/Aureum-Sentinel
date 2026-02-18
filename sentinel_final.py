import requests
import csv
import time
import re

# Die validierten ISINs für den direkten Datenzugriff
ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

CSV_FILE = 'sentinel_history.csv'

def fetch_data(isin, name):
    # Wir simulieren einen echten Browser-Request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    
    url = f"https://www.ls-tc.de/de/aktie/{isin}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return [time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, "0.00", "BLOCKED"]

        # Wir extrahieren den Preis mit RegEx direkt aus dem Quelltext
        # Suche nach dem Bid-Wert im JSON-Block der Seite
        content = response.text
        # Das Muster sucht nach "bid":161.45 oder ähnlichem im JS-Teil
        match = re.search(r'"bid":([\d\.]+)', content)
        
        if match:
            price = match.group(1)
            print(f"✅ {name}: {price} € (Direkt-Extraktion)")
            return [time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, price, "Intraday"]
        else:
            # Fallback: Suche nach dem Preis im HTML-String
            match_html = re.search(r'itemprop="price" content="([\d\.]+)"', content)
            if match_html:
                price = match_html.group(1)
                return [time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, price, "Intraday"]
                
        return [time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, "0.00", "NOT_FOUND"]

    except Exception as e:
        return [time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, "0.00", f"ERROR: {str(e)[:20]}"]

if __name__ == "__main__":
    # 1. CSV Reset
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'ISIN', 'Asset', 'Price', 'Type'])

    # 2. Daten sammeln
    results = []
    for isin, name in ASSETS.items():
        results.append(fetch_data(isin, name))

    # 3. Speichern
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(results)
    
    print(f"🏁 Sentinel-Run beendet. {len(results)} Zeilen geschrieben.")
