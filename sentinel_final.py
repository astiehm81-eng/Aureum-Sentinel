import requests
import csv
import time

# OnVista nutzt interne IDs (Notations), die direkt auf den L&S Feed verweisen
# Siemens Energy (175338787), BASF (372225), SAP (380922), BMW (380252)
ASSETS = {
    "175338787": "Siemens Energy",
    "372225": "BASF",
    "380922": "SAP",
    "380252": "BMW"
}

def fetch_onvista_realtime(notation_id, name):
    # OnVista API-Endpunkt für Realtime-Kurse
    url = f"https://api.onvista.de/api/v1/instrument/L&S/{notation_id}/tick"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # Der aktuellste Tick der Kurve
            price = data.get('last', 0)
            print(f"✅ {name}: {price} € (OnVista-Realtime)")
            return [time.strftime('%H:%M:%S'), notation_id, name, price, "REALTIME"]
        else:
            return [time.strftime('%H:%M:%S'), notation_id, name, "0.00", f"HTTP_{response.status_code}"]
            
    except Exception as e:
        return [time.strftime('%H:%M:%S'), notation_id, name, "0.00", f"ERR_{str(e)[:10]}"]

if __name__ == "__main__":
    results = []
    for notation_id, name in ASSETS.items():
        results.append(fetch_onvista_realtime(notation_id, name))
        time.sleep(1) # Schutz-Delay

    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Time', 'ID', 'Asset', 'Price', 'Status'])
        writer.writerows(results)
