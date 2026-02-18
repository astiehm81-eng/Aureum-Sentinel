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

def fetch_tradegate_final(isin, name):
    url = f"https://www.tradegate.de/refresh.php?isin={isin}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            # Wir suchen die erste Zahl im Format XX.YY oder XXX.YY
            # Tradegate sendet oft: bid|ask|last|... -> wir nehmen 'last'
            content = response.text
            match = re.search(r'([\d\.]+),([\d]+)', content) # Sucht nach 161,45 Format
            
            if match:
                # Umwandeln in US-Format für die CSV (Punkt statt Komma)
                price = match.group(0).replace('.', '').replace(',', '.')
                print(f"✅ {name}: {price} €")
                return [time.strftime('%H:%M:%S'), isin, name, price, "SUCCESS"]
            
            # Zweiter Versuch: Punkt-Format
            match_alt = re.search(r'([\d]+\.[\d]+)', content)
            if match_alt:
                price = match_alt.group(0)
                return [time.strftime('%H:%M:%S'), isin, name, price, "SUCCESS"]

        return [time.strftime('%H:%M:%S'), isin, name, "0.00", f"NO_DATA_STRING"]
    except Exception as e:
        return [time.strftime('%H:%M:%S'), isin, name, "0.00", "ERROR"]

if __name__ == "__main__":
    results = []
    for isin, name in ASSETS.items():
        results.append(fetch_tradegate_final(isin, name))
        time.sleep(2) # 2s Delay für Tradegate-Compliance

    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Time', 'ISIN', 'Asset', 'Price', 'Status'])
        writer.writerows(results)
