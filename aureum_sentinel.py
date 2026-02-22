import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
import logging
from datetime import datetime, timedelta

# Yahoo-Logger stummschalten für sauberes Sentinel-Log
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# --- KONFIGURATION V289.1 ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"

class AureumSentinelV289_1:
    def __init__(self):
        self.stats = {"done": 0, "skipped": 0, "blacklisted": 0, "start": time.time()}
        self.processed_tickers = set()
        self.internal_blacklist = self.load_json(BLACKLIST_FILE, [])
        self.load_pool(initial=True)

    def log(self, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] {message}", flush=True)
        sys.stdout.flush()

    def load_json(self, path, default):
        if not os.path.exists(path): return default
        with open(path, "r") as f:
            try: return json.load(f)
            except: return default

    def load_pool(self, initial=False):
        raw_data = self.load_json(POOL_FILE, [])
        # Support für verschiedene JSON Formate (Liste von Objekten oder Liste von Strings)
        raw_tickers = [e['symbol'] if isinstance(e, dict) else e for e in raw_data]
        
        # Markt-Filter & Blacklist-Abgleich
        unique_map = {}
        for t in raw_tickers:
            base = t.split('.')[0]
            if base not in unique_map or any(ext in t for ext in ['.DE', '.US']):
                unique_map[base] = t
        
        new_pool = [t for t in unique_map.values() if t not in self.internal_blacklist]
        
        if not initial and len(new_pool) > len(self.pool):
            self.log(f"✨ POOL-UPDATE: {len(new_pool)} Assets aktiv.")
        
        self.pool = new_pool
        if initial:
            self.log(f"SENTINEL V289.1: {len(self.pool)} Hauptmarkt-Assets im Visier.")

    def update_blacklist(self, ticker, reason):
        if ticker not in self.internal_blacklist:
            self.internal_blacklist.append(ticker)
            self.stats["blacklisted"] += 1
            self.log(f"🚫 BLACKLIST: {ticker} ({reason})")
            with open(BLACKLIST_FILE, "w") as f:
                json.dump(self.internal_blacklist, f)

    def run(self):
        self.log("🚀 START PERPETUAL CYCLE")
        
        idx = 0
        while idx < len(self.pool):
            ticker = self.pool[idx]
            if idx % 20 == 0: self.load_pool()

            if ticker in self.processed_tickers:
                idx += 1; continue

            try:
                path = f"{HERITAGE_ROOT}2026/{ticker}/registry.parquet"
                # Skip Check
                if os.path.exists(path) and (datetime.now() - datetime.fromtimestamp(os.path.getmtime(path)) < timedelta(minutes=5)):
                    idx += 1; self.stats["skipped"] += 1; continue

                # Download (Mit unterdrücktem Error-Output)
                y_obj = yf.Ticker(ticker)
                recent_df = y_obj.history(period="2d", interval="5m", progress=False)
                
                if recent_df.empty:
                    self.update_blacklist(ticker, "Delisted / No 5m Data")
                    idx += 1; continue

                recent_df = recent_df.reset_index()
                last_price = recent_df['Close'].iloc[-1]
                
                # Volatilitäts-Check (Warnung bei > 4% Intraday)
                day_change = ((recent_df['Close'].max() - recent_df['Close'].min()) / recent_df['Close'].min()) * 100
                alert = " ⚡" if day_change > 4 else ""

                # Speichern
                os.makedirs(os.path.dirname(path), exist_ok=True)
                recent_df['Ticker'] = ticker
                if 'Datetime' in recent_df.columns: recent_df = recent_df.rename(columns={'Datetime': 'Date'})

                if os.path.exists(path):
                    existing = pd.read_parquet(path)
                    pd.concat([existing, recent_df]).drop_duplicates(subset=['Date']).to_parquet(path, index=False)
                else:
                    recent_df.to_parquet(path, index=False)

                self.log(f"✅ [{idx+1}/{len(self.pool)}] {ticker.ljust(8)} | {last_price:8.2f}{alert}")
                self.stats["done"] += 1
                self.processed_tickers.add(ticker)
                time.sleep(0.7)

            except Exception as e:
                self.log(f"⚠️ FAIL {ticker}: {str(e)[:30]}")
            
            idx += 1

        self.log(f"🏁 ZYKLUS BEENDET | Erneuert: {self.stats['done']} | Blacklist: {self.stats['blacklisted']}")

if __name__ == "__main__":
    AureumSentinelV289_1().run()
