import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import time
from datetime import datetime

# --- STRUKTUR & LOGIK ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
MAX_WORKERS = 60 
STOOQ_LOCK = threading.Lock()
FILE_LOCKS = {} 
FILE_LOCKS_LOCK = threading.Lock()
ANCHOR = 0.0005 # 0,05% Eiserner Standard

class AureumInspector:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {"processed": 0, "gaps_closed": 0, "start": time.time()}

    def log(self, level, ticker, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] [{level:7}] {ticker:10} | {message}", flush=True)

    def get_lock(self, path):
        with FILE_LOCKS_LOCK:
            if path not in FILE_LOCKS: FILE_LOCKS[path] = threading.Lock()
            return FILE_LOCKS[path]

    def apply_iron_standard(self, df):
        """Kern-Anforderung: Zeitreihen-Vervollständigung & Anker-Filterung"""
        if df.empty: return df
        df = df.sort_values('Date').drop_duplicates(subset=['Date'])
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        
        # Anker-Logik zur Rauschreduzierung
        anchors = [df.iloc[0].to_dict()]
        for i in range(1, len(df)):
            last_p = anchors[-1]['Close']
            curr_p = df.iloc[i]['Close']
            if abs((curr_p / last_p) - 1) >= ANCHOR:
                anchors.append(df.iloc[i].to_dict())
        return pd.DataFrame(anchors)

inspector = AureumInspector()

class AureumSentinel:
    def __init__(self):
        self.load_pool()
        self.ensure_structure()

    def ensure_structure(self):
        """Erzeugt Dekaden-Ordner falls nötig"""
        for d in ["1990s", "2000s", "2010s", "2020s"]:
            os.makedirs(os.path.join(HERITAGE_ROOT, d), exist_ok=True)

    def load_pool(self):
        if os.path.exists(POOL_FILE):
            with open(POOL_FILE, "r") as f: self.pool = json.load(f)
        else: self.pool = [{"symbol": "SAP.DE"}]

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. AKQUISE (Historie + Aktuell)
            # Stooq für die Tiefe (Vervollständigung)
            with STOOQ_LOCK:
                time.sleep(random.uniform(0.3, 0.6))
                r = requests.get(f"https://stooq.com/q/d/l/?s={ticker}&i=d", timeout=7)
                hist = pd.read_csv(io.StringIO(r.text), parse_dates=['Date']) if r.status_code == 200 else pd.DataFrame()
            
            # Yahoo für die letzte Woche (Heirat)
            recent = yf.Ticker(ticker).history(period="7d", interval="5m").reset_index()
            recent.rename(columns={'Datetime': 'Date'}, inplace=True, errors='ignore')

            # 2. INTEGRATION (Der "Oberaufseher" Mechanismus)
            full_series = pd.concat([hist, recent], ignore_index=True)
            if full_series.empty: return

            # Jedes Jahr der Zeitreihe muss einzeln in die richtige Datei wandern
            for year, group in full_series.groupby(pd.to_datetime(full_series['Date']).dt.year):
                decade_dir = os.path.join(HERITAGE_ROOT, f"{(int(year)//10)*10}s")
                os.makedirs(decade_dir, exist_ok=True)
                file_path = os.path.join(decade_dir, f"{int(year)}.parquet")

                with inspector.get_lock(file_path):
                    # Laden der bestehenden Jahres-Daten für dieses Asset (falls vorhanden)
                    existing_data = pd.DataFrame()
                    if os.path.exists(file_path):
                        existing_data = pd.read_parquet(file_path)
                    
                    # Verschmelzen & Zeitreihen vervollständigen
                    # Wir filtern das aktuelle Asset aus der Jahresdatei und fügen die neuen Daten hinzu
                    other_assets = existing_data[existing_data.get('Ticker', '') != ticker] if not existing_data.empty else pd.DataFrame()
                    
                    # Das aktuelle Asset wird neu berechnet (Eiserner Standard)
                    current_asset_old = existing_data[existing_data.get('Ticker', '') == ticker] if not existing_data.empty else pd.DataFrame()
                    merged_asset = pd.concat([current_asset_old, group], ignore_index=True)
                    
                    # Wichtig: Hier wird die Zeitreihe für das Jahr finalisiert
                    final_asset_year = inspector.apply_iron_standard(merged_asset)
                    final_asset_year['Ticker'] = ticker # Spalte für den Oberaufseher

                    # Zurück in die Jahres-Datei schreiben
                    final_df = pd.concat([other_assets, final_asset_year], ignore_index=True)
                    final_df.to_parquet(file_path, index=False, compression='snappy')

            inspector.log("STABLE", ticker, f"Zeitreihe vervollständigt.")
            with inspector.stats_lock: inspector.stats["processed"] += 1
        except Exception as e:
            inspector.log("ERROR", ticker, f"Sync fehlgeschlagen: {str(e)[:50]}")

    def run(self):
        inspector.log("SYSTEM", "START", f"V258 Sync & Repair Mode | Pool: {len(self.pool)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in range(0, len(self.pool), 300):
                batch = self.pool[i:i+300]
                futures = [executor.submit(self.worker_task, a) for a in batch]
                concurrent.futures.wait(futures)
                
                # Status Update
                elapsed = (time.time() - inspector.stats['start']) / 60
                print(f"\n[MISSION CONTROL] Coverage: {inspector.stats['processed']} Assets | Speed: {inspector.stats['processed']/elapsed:.1f} Ast/Min\n")

if __name__ == "__main__":
    import random # Sicherstellen, dass random für den Jitter da ist
    AureumSentinel().run()
