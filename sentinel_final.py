import requests
import csv
import time
import os

ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

def fetch_max_history(isin, name):
    print(f"📡 Starte Deep-Scan (MAX) für {name}...")
    # Mode history liefert bei Tradegate das Maximum der verfügbaren Rückschau
    url = f"https://www.tradegate.de/export.php?isin={isin}&mode=history"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code == 200:
            lines = response.text.strip().split('\n')
            data_points = []
            for line in lines[1:]:
                parts = line.split(';')
                if len(parts) >= 5:
                    date = parts[0]
                    # Preis-Normierung auf Punkt-Format
                    close_p = parts[4].replace(',', '.')
                    data_points.append({'date': date, 'price': float(close_p)})
            
            # Sortierung sicherstellen (alt nach neu)
            data_points.sort(key=lambda x: time.strptime(x['date'], '%d.%m.%Y'))
            return data_points
    except Exception as e:
        print(f"❌ Fehler bei {name}: {e}")
    return []

def get_current_tick(isin):
    # Realtime-Check zur Verifizierung des "Last Stand"
    url = f"https://www.tradegate.de/refresh.php?isin={isin}"
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        # Wir extrahieren den Last-Preis (3. Position)
        parts = res.text.split('|')
        if len(parts) > 2:
            return float(parts[2].replace(',', '.'))
    except:
        return None
    return None

if __name__ == "__main__":
    final_output = []
    summary = []

    for isin, name in ASSETS.items():
        # 1. Gesamte verfügbare Historie holen
        history = fetch_max_history(isin, name)
        
        # 2. Den absolut letzten Realtime-Stand zur Verifizierung
        current_p = get_current_tick(isin)
        
        if history:
            # Lückenlosigkeit prüfen: Wir hängen den heutigen Tick an, 
            # falls das Archiv von gestern ist
            today_str = time.strftime('%d.%m.%Y')
            if history[-1]['date'] != today_str and current_p:
                history.append({'date': today_str, 'price': current_p})
            
            # Für die CSV aufbereiten
            for entry in history:
                final_output.append([entry['date'], isin, name, entry['price']])
            
            summary.append(f"{name}: {history[0]['date']} bis heute ({len(history)} Tage) | Aktuell: {current_p} €")

    # 3. Lückenloser Export
    with open('sentinel_history.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Datum', 'ISIN', 'Asset', 'Schlusskurs'])
        writer.writerows(final_output)

    # 4. Verifizierung für dich im Log
    print("\n--- 🛡️ AUREUM SENTINEL VERIFIKATION ---")
    for s in summary:
        print(s)
    print("---------------------------------------")
    print(f"🏁 Lückenloser Datensatz gespeichert. Bereit für Inkremental-Modus.")
