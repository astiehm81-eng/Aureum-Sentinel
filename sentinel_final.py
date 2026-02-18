import requests
import csv
import time
import random

ASSETS = [
    "ENER61", "SAP000", "A1EWWW", "A0AE1X", "BASF11", "DTE000", "VOW300", 
    "ADS000", "DBK100", "ALV001", "BAY001", "BMW111", "IFX000", "MUV200"
]

def get_history_safe(wkn):
    # Wir nutzen genau die URL, die wir für die Historie brauchen
    url = f"https://www.ls-tc.de/_rpc/json/chart/chart.json?symbol={wkn}&range=max"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': f'https://www.ls-tc.de/de/aktie/{wkn}'
    }

    for attempt in range(5):
        try:
            # Hier ist das Delay-Prinzip: Wir geben dem Server Zeit
            response = requests.get(url, headers=headers, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                series = data.get('series', {}).get('main', {}).get('data', [])
                if series:
                    print(f"✅ {wkn}: Daten erhalten.")
                    return [[time.strftime('%Y-%m-%d', time.gmtime(e[0]/1000)), wkn, e[1]] for e in series]
            
            elif response.status_code in [502, 503, 429]:
                # Wenn der Server blockt, massiv verlangsamen (Exponential Backoff)
                wait_time = (attempt + 1) * 15 
                print(f"⚠️ Blockade ({response.status_code}). Kühle ab für {wait_time}s...")
                time.sleep(wait_time)
            
        except Exception as e:
            print(f"🔄 Fehler bei {wkn}: {e}")
            time.sleep(10)
            
    return []

if __name__ == "__main__":
    all_history = []
    
    for wkn in ASSETS:
        data = get_history_safe(wkn)
        if data:
            all_history.extend(data)
        
        # DAS ENTSCHEIDENDE DELAY: 
        # Wir warten zwischen den Assets, damit die IP nicht geflaggt wird.
        # 1ms wäre für die Historie zu wenig, ich setze es auf ein sicheres Maß.
        time.sleep(2.0) 

    if all_history:
        with open('sentinel_deep_history.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Date', 'WKN', 'Price'])
            writer.writerows(all_history)
        print("🏁 Historie erfolgreich gesichert.")
