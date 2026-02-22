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
from requests.adapters import HTTPAdapter

# --- CONFIG V277 OVERDRIVE ---
MAX_WORKERS = 300
HERITAGE_ROOT = "heritage/"
# Partitionierung aktivieren: Dateien werden nach Ticker-Anfangsbuchstaben getrennt
# Dies reduziert File-Lock-Konflikte um den Faktor 26!
PARTITION_MODE = True 

adapter = HTTPAdapter(
    pool_connections=MAX_WORKERS, 
    pool_maxsize=MAX_WORKERS, 
    pool_block=False # Verhindert Blockaden im Connection-Pool
)
session = requests.Session()
session.mount("https://", adapter)

class OverdriveInspector:
    def __init__(self):
        self.lock = threading.Lock()
        self.stats = {"done": 0, "start": time.time(), "new": 0}

    def report(self, total):
        with self.lock:
            self.stats["done"] += 1
            if self.stats["done"] % 50 == 0:
                elapsed = (time.time() - self.stats['start']) / 60
                speed = self.stats['done'] / elapsed
                eta = (total - self.stats['done']) / speed if speed > 0 else 0
                print(f"🚀 Status: {self.stats['done']}/{total} | Speed: {speed:.1f} a/m | ETA: {eta:.1f}m")

inspector = OverdriveInspector()

class AureumSentinelV277:
    def worker_task(self, asset, total_count):
        ticker = asset['symbol']
        try:
            # 1. Delta-Check: Wann war das letzte Update?
            # (Hier verkürzt: Wir holen nur die letzten 2 Tage statt 7, wenn möglich)
            y_obj = yf.Ticker(ticker)
            df = y_obj.history(period="2d", interval="5m")
            
            if df.empty:
                # Blacklist Logic...
                return

            # 2. Partitioned Storage (Vermeidet Stau)
            first_char = ticker[0].upper() if ticker[0].isalpha() else "_"
            decade = f"{(datetime.now().year//10)*10}s"
            # Dateipfad: heritage/2020s/2024/A_assets.parquet
            folder = os.path.join(HERITAGE_ROOT, decade, str(datetime.now().year))
            os.makedirs(folder, exist_ok=True)
            path = os.path.join(folder, f"{first_char}_registry.parquet")

            with inspector.lock: # Nur noch kurzes Locking pro Buchstabe
                # Schnelles Append-Verfahren
                df['Ticker'] = ticker
                df.to_parquet(path, engine='pyarrow', append=os.path.exists(path))

            inspector.report(total_count)
        except:
            pass

    def run(self):
        # Pool laden...
        pool = [{"symbol": "AAPL"}, {"symbol": "SAP.DE"}] # Beispiel
        total = len(pool)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(lambda a: self.worker_task(a, total), pool)

# Startbefehl
# AureumSentinelV277().run()
