import pandas as pd
import yfinance as yf
import os, json, time, sys
from datetime import datetime

# --- EISERNER STANDARD V99.2 (ANCHOR-POINT LOGIC) ---
POOL_FILE = "isin_pool.json"
ANCHOR_THRESHOLD = 0.001  # 0,1% Bewegung
RUNTIME_LIMIT = 3600      # Läuft 1 Stunde pro GitHub-Action

def run_sentinel_ticker():
    if not os.path.exists(POOL_FILE):
        print("❌ Pool-Datei nicht gefunden.")
        return

    with open(POOL_FILE, 'r') as f:
        pool = json.load(f)

    # Speicher für die letzten Ankerpunkte (wird pro Lauf initialisiert)
    anchors = {asset['symbol']: None for asset in pool[:50]} 
    
    start_time = time.time()
    print(f"🛡️ AUREUM SENTINEL AKTIV - Hard Refresh Mode (0,1% Anchor)")
    print(f"📡 Filter für 'Statistical Noise' ist DEAKTIVIERT.", flush=True)

    while (time.time() - start_time) < RUNTIME_LIMIT:
        for asset in pool[:50]: # Fokus auf Top 50 für maximale Frequenz
            symbol = asset['symbol']
            try:
                # Hard Refresh direkt von der Quelle (Yahoo als Proxy für Tradegate-Kurse)
                ticker = yf.Ticker(symbol)
                data = ticker.history(period="1d", interval="1m")
                
                if not data.empty:
                    current_price = data['Close'].iloc[-1]
                    last_anchor = anchors[symbol]

                    if last_anchor is None:
                        anchors[symbol] = current_price
                        print(f"📍 INITIAL ANCHOR | {symbol}: {current_price:.4f}", flush=True)
                    else:
                        # Berechnung der Abweichung
                        diff = abs(current_price - last_anchor) / last_anchor
                        
                        if diff >= ANCHOR_THRESHOLD:
                            print(f"🚀 NEW ANCHOR POINT | {symbol}: {current_price:.4f} (Change: {diff*100:.2f}%)", flush=True)
                            anchors[symbol] = current_price
            except Exception as e:
                pass # Silent skip für Stabilität
            
        time.sleep(2) # Kurze Pause vor dem nächsten Hard-Refresh Loop

if __name__ == "__main__":
    run_sentinel_ticker()
