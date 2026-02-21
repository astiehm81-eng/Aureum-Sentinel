import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
import concurrent.futures
import threading
import time
import random
from datetime import datetime

# --- KONFIGURATION ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
MAX_WORKERS_YAHOO = 30 
STOOQ_LOCK = threading.Lock() # DIE LÖSUNG: Nur ein Thread darf zu Stooq
ANCHOR_THRESHOLD = 0.0005

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/121.0.0.0 Safari/537.36"
]

class AureumInspector:
    # ... (Wie gehabt: log, apply_iron_anchor, save_heritage)
    def save_heritage(self, df, ticker):
        for year, group in df.groupby(df['Date'].dt.year):
            path = f"{HERITAGE_ROOT}{(int(year)//10)*10}s/heritage_{int(year)}.parquet"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if os.path.exists(path):
                old = pd.read_parquet(path)
                # HEILUNG: Duplikate weg, sortieren, konsolidieren
                group = pd.concat([old, group]).drop_duplicates(subset=['Date']).sort_values('Date')
            group.to_parquet(path, index=False, compression='snappy')

inspector = AureumInspector()

class AureumSentinel:
    def fetch_stooq_safe(self, ticker):
        """Die damals erfolgreiche Strategie: Seriell und mit Pause"""
        with STOOQ_LOCK:
            # Sicherheits-Pause (Jitter) wie in V207
            time.sleep(random.uniform(1.5, 3.5))
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            try:
                url = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 200 and len(r.content) > 300:
                    return pd.read_csv(io.StringIO(r.text), parse_dates=['Date'])
            except:
                pass
            return pd.DataFrame()

    def worker_task(self, asset):
        ticker = asset['symbol']
        try:
            # 1. Lokale Heilung: Was ist schon da?
            # (Prüfung ob wir Stooq überhaupt brauchen)
            
            # 2. Stooq Abruf (Serialisiert über Lock)
            hist = self.fetch_stooq_safe(ticker)
            
            # 3. Yahoo Abruf (Parallel und schnell)
            stock = yf.Ticker(ticker)
            recent = stock.history(period="7d", interval="5m").reset_index()
            recent.rename(columns={'Datetime': 'Date'}, inplace=True, errors='ignore')
            
            # 4. Daten-Heirat & Eiserner Standard
            combined = pd.concat([hist, recent], ignore_index=True)
            combined['Date'] = pd.to_datetime(combined['Date'], utc=True).dt.tz_localize(None)
            
            clean_df = inspector.apply_iron_anchor(combined)
            inspector.save_heritage(clean_df, ticker)
            
            return "OK"
        except Exception as e:
            return f"FAIL: {str(e)}"

    def run(self):
        # ... (Executor-Logik wie gehabt)
