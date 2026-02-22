import pandas as pd
import yfinance as yf
import os
import json
import concurrent.futures
import threading
import time
from datetime import datetime

# --- CONFIG V280 ---
MAX_WORKERS = 50  # Wir gehen massiv runter, um die Sperre zu umgehen
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"

class AureumSentinelV280:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats = {"done": 0, "cleaned": 0, "start": time.time()}
        self.load_resources()

    def clean_ticker_name(self, ticker):
        """Entfernt Doppel-Suffixe und korrigiert Formatierungsfehler"""
        if not ticker: return None
        # Entferne $ Zeichen und Leerzeichen
        t = ticker.replace('$', '').strip()
        # Fix für .PA.PA oder .DE.DE
        parts = t.split('.')
        if len(parts) > 2:
            # Behalte nur den Namen und das erste Suffix
            t = f"{parts[0]}.{parts[1]}"
        return t

    def load_resources(self):
        if os.path.exists(BLACKLIST_FILE):
            with open(BLACKLIST_FILE, "r") as f: self.blacklist = set(json.load(f))
        else: self.blacklist = set()
        
        if not os.path.exists(POOL_FILE): 
            self.pool = []
            return
            
        with open(POOL_FILE, "r") as f: raw_data = json.load(f)
        
        refined = {}
        for entry in raw_data:
            t = self.clean_ticker_name(entry.get('symbol', ''))
            if not t or t in self.blacklist: continue
            
            base = t.split('.')[0]
            # Heimatmarkt-Priorität
            if base not in refined:
                refined[base] = t
            else:
                current = refined[base]
                if '.' not in t: refined[base] = t # US bevorzugen
                elif '.DE' in t and '.' in current: refined[base] = t # DE bevorzugen
        
        self.pool = [{"symbol": s} for s in refined.values()]
        self.stats["initial_count"] = len(raw_data)

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # Hard-Refresh direkt über die Ticker-Instanz
            # Wir nutzen period="1d" zum Test, ob überhaupt Daten kommen
            df = yf.download(ticker, period="5d", interval="5m", progress=False, group_by='ticker')
            
            if df.empty:
                with self.stats_lock:
                    self.blacklist.add(ticker)
                    self.stats["cleaned"] += 1
                return

            # Speicherpfad mit korrekter Hierarchie
            char = ticker[0].upper() if ticker[0].isalpha() else "_"
            path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # Daten-Transformation
            df_to_save = df.reset_index()
            df_to_save['Ticker'] = ticker
            
            with self.stats_lock:
                # Sicherer Schreibvorgang
                if os.path.exists(path):
                    existing = pd.read_parquet(path)
                    combined = pd.concat([existing, df_to_save]).drop_duplicates(subset=['Date', 'Ticker'])
                    combined.to_parquet(path, engine='pyarrow', index=False)
                else:
                    df_to_save.to_parquet(path, engine='pyarrow', index=False)
                self.stats["done"] += 1
        except Exception as e:
            pass

    def run(self):
        total = len(self.pool)
        print(f"🚀 V280 Deep-Fix | Bereinigter Pool: {total}")
        
        # Speichere die gesäuberten Ticker in der Blacklist-Datei ab
        with open(BLACKLIST_FILE, "w") as f:
            json.dump(list(self.blacklist), f, indent=4)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(self.worker_task, self.pool)
            
        elapsed = (time.time() - self.stats['start']) / 60
        print(f"\n✅ FINISHED: {self.stats['done']} erfolgreich | {self.stats['cleaned']} Blacklisted | Dauer: {elapsed:.1f}m")

if __name__ == "__main__":
    AureumSentinelV280().run()
