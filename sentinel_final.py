import requests
import csv
import time
import re

ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

def fetch_tradegate_ultimate(isin, name):
    url = f"https://www.tradegate.de/refresh.php?isin={isin}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            content = response.text
            # Wir suchen alle Zahlenkombinationen mit Komma (z.B. 161,45)
            # Tradegate liefert: bid|ask|last|high|low...
            matches = re.findall(r'(\d+\,\d+)', content)
            
            if matches:
                # Wir nehmen den 'Last' Preis - bei Tradegate meist der 3. oder 4. Wert
                # Um sicherzugehen, nehmen wir den ersten Wert, der nicht 0,00 ist
                for val in matches:
                    if val != "0,00":
                        clean_price = val.replace(',', '.')
                        print(f"✅ {name}: {clean_price} €")
                        return [time.strftime('%H:%M:%S'), isin, name, clean_price, "SUCCESS"]
            
        return [time.strftime('%H:%M:%S'), isin, name, "0.00", "RETRY_STRING"]
    except Exception:
        return [time.strftime('%H:%M:%S'), isin, name, "0.00", "ERROR"]

if __name__ == "__main__":
    results = []
    for isin, name in ASSETS.items():
        results.append(fetch_tradegate_ultimate(isin, name))
        time.sleep(1.5) 

    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Time', 'ISIN', 'Asset', 'Price', 'Status'])
        writer.writerows(results)
