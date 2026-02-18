import requests
import csv
import time
import random

ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

def fetch_with_jitter(isin, name):
    # Zufällige Verzögerung vor dem Start, um Cluster-Anfragen zu vermeiden
    time.sleep(random.uniform(2.1, 4.8))
    
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.ls-tc.de/de/aktie/" + isin,
        "Origin": "https://www.ls-tc.de"
    }
    
    url = f"https://www.ls-tc.de/_rpc/json/instrument/chart/data?isin={isin}&period=intraday"
    
    try:
        # Wir nutzen eine Session, um Cookies über Requests hinweg zu behalten
        with requests.Session() as session:
            # 1. Wir "besuchen" erst die Hauptseite (Landing Page Simulation)
            session.get(f"https://www.ls-tc.de/de/aktie/{isin}", headers=headers, timeout=10)
            time.sleep(random.uniform(1.5, 3.0)) # "Bedenkzeit" eines Menschen
            
            # 2. Jetzt erst die Daten-Abfrage
            response = session.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                price = data["series"]["intraday"]["data"][-1][1]
                print(f"✅ {name}: {price} €")
                return [isin, name, price, "OK"]
            else:
                return [isin, name, "0.00", f"HTTP_{response.status_code}"]
                
    except Exception as e:
        return [isin, name, "0.00", f"FEHLER_{str(e)[:10]}"]

if __name__ == "__main__":
    results = []
    for isin, name in ASSETS.items():
        results.append(fetch_with_jitter(isin, name))
    
    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['ISIN', 'Asset', 'Price', 'Status'])
        writer.writerows(results)
