import requests
import csv
import time

ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

def fetch_stealth(isin, name):
    # Wir tarnen uns als mobiles Endgerät (wie dein Handy im Screenshot)
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
        "Accept": "application/json",
        "Referer": "https://www.ls-tc.de/de/",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    # Wir nutzen den Chart-Endpoint, aber mit einem Cache-Buster Zeitstempel
    url = f"https://www.ls-tc.de/_rpc/json/instrument/chart/data?isin={isin}&period=intraday&_={int(time.time()*1000)}"
    
    try:
        session = requests.Session()
        # Vorab-Besuch der Hauptseite für Session-Cookies
        session.get("https://www.ls-tc.de/de/", headers=headers, timeout=5)
        
        response = session.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if "series" in data and "intraday" in data["series"]:
                points = data["series"]["intraday"]["data"]
                if points:
                    price = points[-1][1]
                    print(f"✅ {name}: {price} €")
                    return [time.strftime('%H:%M:%S'), isin, name, price, "SUCCESS"]
        
        return [time.strftime('%H:%M:%S'), isin, name, "0.00", f"HTTP_{response.status_code}"]
    except Exception as e:
        return [time.strftime('%H:%M:%S'), isin, name, "0.00", f"ERR_{type(e).__name__}"]

if __name__ == "__main__":
    results = []
    for isin, name in ASSETS.items():
        results.append(fetch_stealth(isin, name))
        time.sleep(2) # Sanfte Pausen gegen Bot-Erkennung

    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Time', 'ISIN', 'Asset', 'Price', 'Status'])
        writer.writerows(results)
