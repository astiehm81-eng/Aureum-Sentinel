import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
from datetime import datetime, timedelta

# --- KONFIGURATION V289 ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"

class AureumSentinelV289:
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
        """Lädt Pool, filtert Doubletten und Markt-Redundanz"""
        raw_data = self.load_json(POOL_FILE, [])
        raw_tickers = [e['symbol'] for e in raw_data if 'symbol' in e]
        
        # 1. Markt-Filter: Wir behalten nur den primären Ticker (Beispiel: SAP.DE statt SAP.F)
        # Wir gruppieren nach dem Namen vor dem Punkt und wählen den 'besten' Suffix
        unique_map = {}
        for t in raw_tickers:
            base = t.split('.')[0]
            if base not in unique_map:
                unique_map[base] = t
            else:
                # Priorität: Heimatbörsen-Suffixe bevorzugen
                if any(ext in t for ext in ['.DE', '.US', '.L']):
                    unique_map[base] = t
        
        new_pool = list(unique_map.values())
        
        # 2. Blacklist Abgleich
        new_pool = [t for t in new_pool if t not in self.internal_blacklist]

        if not initial and len(new_pool) > len(self.pool):
            self.log(f"✨ NEUE ASSETS: {len(new_pool) - len(self.pool)} Ticker nach Markt-Filterung erkannt.")
        
        self.pool = new_pool
        if initial:
            self.log(f"POOL READY: {len(self.pool)} unikale Hauptmarkt-Assets geladen.")

    def update_blacklist(self, ticker, reason):
        """Setzt Asset auf Blacklist und speichert diese"""
        if ticker not in self.internal_blacklist:
            self.internal_blacklist.append(ticker)
            self.stats["blacklisted"] += 1
            self.log(f"🚫 BLACKLIST: {ticker} ({reason})")
            with open(BLACKLIST_FILE, "w") as f:
                json.dump(self.internal_blacklist, f)

    def run(self):
        self.log("🚀 START INTELLIGENT SYNC V289")
        
        idx = 0
        while idx < len(self.pool):
            ticker = self.pool[idx]
            
            # Alle 15 Assets: Refresh der Liste (inkl. dynamischer Blacklist)
            if idx % 15 == 0: self.load_pool()

            if ticker in self.processed_tickers:
                idx += 1
                continue

            try:
                # Delta-Check (Skip if fresh < 5m)
                path = f"{HERITAGE_ROOT}2026/{ticker}/registry.parquet"
                if os.path.exists(path) and (datetime.now() - datetime.fromtimestamp(os.path.getmtime(path)) < timedelta(minutes=5)):
                    idx += 1
                    self.stats["skipped"] += 1
                    continue

                y_obj = yf.Ticker(ticker)
                # 5-Minuten Daten sind Pflicht für Aureum Sentinel
                recent_df = y_obj.history(period="2d", interval="5m").reset_index()
                
                if recent_df.empty:
                    self.update_blacklist(ticker, "Keine 5m-Daten verfügbar")
                    idx += 1
                    continue

                # Preis & Info für Log
                last_price = recent_df['Close'].iloc[-1]
                currency = y_obj.info.get('currency', 'UNIT')

                # Speichern (Partitioniert)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                recent_df['Ticker'] = ticker
                if 'Datetime' in recent_df.columns: recent_df = recent_df.rename(columns={'Datetime': 'Date'})

                if os.path.exists(path):
                    existing = pd.read_parquet(path)
                    pd.concat([existing, recent_df]).drop_duplicates(subset=['Date']).to_parquet(path, index=False, compression='snappy')
                else:
                    recent_df.to_parquet(path, index=False, compression='snappy')

                self.log(f"✅ [{idx+1}/{len(self.pool)}] {ticker.ljust(8)} | {last_price:8.2f} {currency}")
                self.stats["done"] += 1
                self.processed_tickers.add(ticker)
                
                time.sleep(0.7)

            except Exception as e:
                err_msg = str(e).lower()
                if "404" in err_msg or "not found" in err_msg:
                    self.update_blacklist(ticker, "Symbol existiert nicht mehr")
                else:
                    self.log(f"⚠️ FEHLER {ticker}: {str(e)[:40]}")
            
            idx += 1

        self.log(f"🏁 FINISH: Erneuert: {self.stats['done']} | Blacklisted: {self.stats['blacklisted']}")

if __name__ == "__main__":
    AureumSentinelV289().run()
