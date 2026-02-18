import requests
import csv
import time
import re

# ISINs bleiben gleich, Tradegate nutzt diese als Primärschlüssel
ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

def fetch_tradegate(isin, name):
    # Tradegate Realtime-Abfrage (bekannt für hohe Stabilität)
    url = f"https://www.tradegate.de/refresh.php?isin={isin}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/plain"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            # Tradegate liefert einen String: bid|ask|last|...
            # Beispiel: 161.450|161.550|161.480|...
            data = response.text.split('|')
            if len(data) > 2:
                last_price = data[2] # Der 'Last' Preis
                print(f"✅ {name}: {last_price} € (Tradegate-Realtime)")
                return [time.strftime('%H:%M:%S'), isin, name, last_price, "REALTIME"]
        
        return [time.strftime('%H:%M:%S'), isin, name, "0.00", f"HTTP_{response.status_code}"]
            
    except Exception as e:
        return [time.strftime('%H:%M:%S'), isin, name, "0.00", f"ERR_{str(e)[:10]}"]

if __name__ == "__main__":
    results = []
    for isin, name in ASSETS.items():
        results.append(fetch_tradegate(isin, name))
        time.sleep(1) 

    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Time', 'ISIN', 'Asset', 'Price', 'Status'])
        writer.writerows(results)
    
    print("🏁 Tradegate-Sentinel Run beendet.")
