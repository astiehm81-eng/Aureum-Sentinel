import requests
import csv
import time
from concurrent.futures import ThreadPoolExecutor

# Deine Asset-Liste
ASSETS = ["ENER61", "SAP000", "A1EWWW", "A0AE1X", "BASF11", "DTE000", "VOW300", 
          "ADS000", "DBK100", "ALV001", "BAY001", "BMW111", "IFX000", "MUV200"]

def worker_task(wkn):
    """Repräsentiert einen Tab/Worker, der die Historie zieht."""
    url = f"https://www.ls-tc.de/_rpc/json/chart/chart.json?symbol={wkn}&range=max"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    try:
        # Dein 1ms Delay vor dem 'Klick' im Tab
        time.sleep(0.001)
        response = requests.get(url, headers=headers, timeout=20)
        
        if response.status_code == 200:
            series = response.json().get('series', {}).get('main', {}).get('data', [])
            print(f"✅ Tab-Sync abgeschlossen: {wkn} ({len(series)} Punkte)")
            return [[time.strftime('%Y-%m-%d', time.gmtime(e[0]/1000)), wkn, e[1]] for e in series]
        else:
            print(f"⚠️ Tab blockiert für {wkn}: Status {response.status_code}")
    except Exception as e:
        print(f"❌ Tab-Fehler bei {wkn}: {e}")
    return []

if __name__ == "__main__":
    print(f"🚀 Starte Aureum Sentinel mit 8 parallelen Workern...")
    start_time = time.time()
    
    # 8 Worker simulieren 8 offene Tabs
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(worker_task, ASSETS))

    # Zusammenführung der Daten (Flatten the list)
    final_history = [item for sublist in results for item in sublist]

    if final_history:
        with open('sentinel_history.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Date', 'WKN', 'Price'])
            writer.writerows(final_history)
        
        duration = round(time.time() - start_time, 2)
        print(f"🏁 Historie eingefroren! {len(final_history)} Zeilen in {duration}s gesichert.")
