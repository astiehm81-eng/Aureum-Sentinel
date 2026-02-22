import pandas as pd
import yfinance as yf
import os
import json
import time
from datetime import datetime

# --- KONFIGURATION (GOLDENER STANDARD RE-INTEGRATION) ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"
AUDIT_FILE = "heritage_audit.txt"
BATCH_SIZE = 40  # Konservative Batch-Größe für maximale Stabilität

class AureumSentinelV284:
    def __init__(self):
        self.stats = {"done": 0, "blacklisted": 0, "start": time.time()}
        self.load_resources()

    def clean_ticker(self, ticker):
        """Bereinigt Ticker von $ und Doppel-Suffixen (z.B. .PA.PA)"""
        if not ticker: return None
        t = ticker.replace('$', '').strip()
        parts = t.split('.')
        # Verhindert Suffix-Wildwuchs: Behalte nur Name und erstes Suffix
        return f"{parts[0]}.{parts[1]}" if len(parts) > 2 else t

    def load_resources(self):
        """Lädt Pool und Blacklist mit Primär-Asset-Logik"""
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r") as f: self.blacklist = set(json.load(f))
        else: self.blacklist = set()
        
        if not os.path.exists(POOL_FILE): 
            self.pool_tickers = []
            return
            
        with open(POOL_FILE, "r") as f: raw_data = json.load(f)
        
        # Dubletten-Filter: US-Börse bevorzugt, dann DE (.DE)
        refined = {}
        for entry in raw_data:
            t = self.clean_ticker(entry.get('symbol', ''))
            if not t or t in self.blacklist: continue
            base = t.split('.')[0]
            if base not in refined or '.' not in t: 
                refined[base] = t
            elif '.DE' in t and '.' in refined[base]: 
                refined[base] = t
        
        self.pool_tickers = list(refined.values())
        print(f"[{datetime.now().strftime('%H:%M:%S')}] POOL | Refined: {len(self.pool_tickers)} Primär-Assets")

    def run(self):
        print(f"🚀 Sentinel V284 Start | Batch-Modus aktiv")
        
        # Verarbeitet Ticker in stabilen 40er-Gruppen
        for i in range(0, len(self.pool_tickers), BATCH_SIZE):
            batch = self.pool_tickers[i:i+BATCH_SIZE]
            ticker_str = " ".join(batch)
            
            try:
                # Batch-Download ist bei Yahoo wesentlich stabiler als Einzel-Requests
                data = yf.download(ticker_str, period="5d", interval="5m", group_by='ticker', progress=False, threads=False)
                
                for ticker in batch:
                    # Ticker-Daten aus dem Batch extrahieren
                    t_data = data[ticker] if len(batch) > 1 else data
                    
                    if t_data.empty or t_data.dropna().empty:
                        self.blacklist.add(ticker)
                        self.stats["blacklisted"] += 1
                        continue

                    # Speichern im Heritage-Format (Partitioniert nach Anfangsbuchstabe)
                    char = ticker[0].upper() if ticker[0].isalpha() else "_"
                    path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    
                    df_save = t_data.reset_index()
                    df_save['Ticker'] = ticker
                    
                    if os.path.exists(path):
                        existing = pd.read_parquet(path)
                        pd.concat([existing, df_save]).drop_duplicates(subset=['Date', 'Ticker']).to_parquet(path, index=False)
                    else:
                        df_save.to_parquet(path, index=False)
                    
                    self.stats["done"] += 1

                # Kurze Pause zum Schutz vor IP-Sperren
                time.sleep(1.5)
                
                # Fortschritts-Log (Sichtbar im GitHub Action Log)
                elapsed = (time.time() - self.stats['start']) / 60
                print(f"📊 Progress: {i+len(batch)}/{len(self.pool_tickers)} | Done: {self.stats['done']} | Speed: {self.stats['done']/max(0.1, elapsed):.1f} Ast/Min")

            except Exception as e:
                print(f"⚠️ Batch-Fehler: {e}")
                time.sleep(3)

        # Blacklist für nächsten Lauf sichern
        with open(BLACKLIST_FILE, "w") as f:
            json.dump(list(self.blacklist), f, indent=4)
        print(f"🏁 Zyklus beendet. Erfolgreich: {self.stats['done']}")

if __name__ == "__main__":
    AureumSentinelV284().run()
