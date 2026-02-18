import requests
import csv
import time
import random

ASSETS = [
    "ENER61", "SAP000", "A1EWWW", "A0AE1X", "BASF11", "DTE000", "VOW300", 
    "ADS000", "DBK100", "ALV001", "BAY001", "BMW111", "IFX000", "MUV200"
]

def get_history_human_style(wkn, session):
    # Wir tarnen uns als moderner Browser
    url = f"https://www.ls-tc.de/_rpc/json/chart/chart.json?symbol={wkn}&range=max"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': f'https://www.ls-tc.de/de/aktie/{wkn}'
    }

    try:
        # Kurzer Vorab-Check (wie ein Mensch, der die Seite aufruft)
        # session.get(f"https://www.ls-tc.de/de/aktie/{wkn}", timeout=5)
        # time.sleep(0.1) # Kurze Denkpause
        
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            series = data.get('series', {}).get('main', {}).get('data', [])
            if series:
                print(f"✅ {wkn}: {len(series)} Datenpunkte geladen.")
                return [[time.strftime('%Y-%m-%d', time.gmtime(e[0]/1000)), wkn, e[1]] for e in series]
        print(f"⚠️ {wkn} übersprungen (Status {response.status_code})")
    except Exception as e:
        print(f"❌ Fehler bei {wkn}")
    return []

if __name__ == "__main__":
    all_data = []
    session = requests.Session() # Wir nutzen EINE Session für alle Aufrufe
    
    print("🚀 Starte schnellen Human-Style Import (V82)...")
    
    for wkn in ASSETS:
        result = get_history_human_style(wkn, session)
        if result:
            all_data.extend(result)
        
        # Das "menschliche" aber schnelle Delay
        time.sleep(random.uniform(0.2, 0.5)) 

    if all_data:
        with open('sentinel_deep_history.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Date', 'WKN', 'Price'])
            writer.writerows(all_data)
        print(f"💾 CSV mit {len(all_data)} Zeilen erstellt.")
