import requests
import csv
import time

ASSETS = [
    "ENER61", "SAP000", "A1EWWW", "A0AE1X", "BASF11", "DTE000", "VOW300", 
    "ADS000", "DBK100", "ALV001", "BAY001", "BMW111", "IFX000", "MUV200"
]

def get_history_fast_sequential(wkn):
    url = f"https://www.ls-tc.de/_rpc/json/chart/chart.json?symbol={wkn}&range=max"
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'X-Requested-With': 'XMLHttpRequest'
    }

    try:
        # Der nacheinander-Prozess
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            series = data.get('series', {}).get('main', {}).get('data', [])
            return [[time.strftime('%Y-%m-%d', time.gmtime(e[0]/1000)), wkn, e[1]] for e in series]
        else:
            print(f"⚠️ {wkn} fehlgeschlagen: Status {response.status_code}")
            return []
    except:
        return []

if __name__ == "__main__":
    all_data = []
    for wkn in ASSETS:
        result = get_history_fast_sequential(wkn)
        all_data.extend(result)
        
        # Hier ist dein 1ms Delay (0.001 Sekunden)
        time.sleep(0.001) 
        print(f"🚀 {wkn} verarbeitet...")

    # Speichern der CSV
    if all_data:
        with open('sentinel_deep_history.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Date', 'WKN', 'Price'])
            writer.writerows(all_data)
        print(f"✅ Historie mit {len(all_data)} Zeilen gesichert.")
