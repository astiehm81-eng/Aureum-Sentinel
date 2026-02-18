import requests
import csv
import time

ASSETS = {
    "DE000ENER610": "Siemens Energy",
    "DE000BASF111": "BASF",
    "DE000SAPG003": "SAP",
    "DE0005190003": "BMW"
}

def fetch_rpc_data(isin, name, period="intraday"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": f"https://www.ls-tc.de/de/aktie/{isin}"
    }
    
    # Der direkte Daten-Kanal für die Kurve
    url = f"https://www.ls-tc.de/_rpc/json/instrument/chart/data?isin={isin}&period={period}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        
        # Wir nehmen den allerletzten Punkt der Kurve (Series -> Intraday -> Letzter Eintrag)
        if "series" in data and "intraday" in data["series"]:
            points = data["series"]["intraday"]["data"]
            if points:
                last_point = points[-1] # [Timestamp, Price]
                price = last_point[1]
                print(f"✅ {name} ({period}): {price} €")
                return [time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, price, period]
        
        return [time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, "0.00", "EMPTY_RPC"]
    except Exception as e:
        return [time.strftime('%Y-%m-%d %H:%M:%S'), isin, name, "0.00", f"RPC_ERR: {str(e)[:15]}"]

if __name__ == "__main__":
    with open('sentinel_history.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'ISIN', 'Asset', 'Price', 'Type'])
        
        for isin, name in ASSETS.items():
            # Erst Intraday (für den 161,xx Check)
            res_intra = fetch_rpc_data(isin, name, "intraday")
            writer.writerow(res_intra)
            
            # Dann 1 Monat (für die Historie)
            res_month = fetch_rpc_data(isin, name, "history")
            writer.writerow(res_month)
            
    print("🏁 RPC-Sentinel Run beendet. Historie & Intraday synchronisiert.")
