import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
from datetime import datetime, timedelta

# --- KONFIGURATION V288 ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"

class AureumSentinelV288:
    def __init__(self):
        self.stats = {"done": 0, "migrated": 0, "skipped": 0, "start": time.time()}
        self.processed_tickers = set()
        self.load_pool()

    def log(self, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] {message}", flush=True)
        sys.stdout.flush()

    def load_pool(self):
        """Lädt und dedupliziert den ISIN Pool"""
        if not os.path.exists(POOL_FILE):
            self.pool = []
            return
        with open(POOL_FILE, "r") as f:
            data = json.load(f)
            # Extrahiere Symbole und entferne Dubletten
            self.pool = list(dict.fromkeys([e['symbol'] for e in data if 'symbol' in e]))
        
        self.log("="*40)
        self.log(f"AUREUM SENTINEL V288")
        self.log(f"Aktueller ISIN Pool: {len(self.pool)} Assets")
        self.log(f"Speicher-Struktur: Partitioniert (Ticker-Level)")
        self.log("="*40)

    def migrate_legacy_data(self):
        """Sucht nach alten .parquet Dateien (wie 1997.parquet) und partitioniert sie neu"""
        # Suche in alten Verzeichnissen (z.B. 1990s oder direkt in heritage)
        legacy_paths = [os.path.join(HERITAGE_ROOT, f) for f in os.listdir(HERITAGE_ROOT) if f.endswith('.parquet')]
        
        for old_path in legacy_paths:
            try:
                self.log(f"📦 Migration: Verarbeite Altdaten {os.path.basename(old_path)}...")
                df = pd.read_parquet(old_path)
                if 'Ticker' in df.columns:
                    for ticker, group in df.groupby('Ticker'):
                        target_dir = f"{HERITAGE_ROOT}2026/{ticker}"
                        os.makedirs(target_dir, exist_ok=True)
                        target_file = f"{target_dir}/registry.parquet"
                        
                        if os.path.exists(target_file):
                            existing = pd.read_parquet(target_file)
                            group = pd.concat([existing, group]).drop_duplicates(subset=['Date'])
                        
                        group.to_parquet(target_file, index=False, compression='snappy')
                os.remove(old_path) # Löschen nach erfolgreicher Migration
                self.stats["migrated"] += 1
            except Exception as e:
                self.log(f"⚠️ Migrationsfehler {old_path}: {e}")

    def should_skip(self, ticker):
        """Prüft, ob das Asset bereits vor kurzem (5 Min) aktualisiert wurde"""
        path = f"{HERITAGE_ROOT}2026/{ticker}/registry.parquet"
        if not os.path.exists(path): return False
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if datetime.now() - mtime < timedelta(minutes=5):
                return True
        except: pass
        return False

    def run(self):
        self.log("🚀 START SYNC & MIGRATION")
        self.migrate_legacy_data()
        
        # Endlosschleife bzw. dynamische Abarbeitung
        idx = 0
        while idx < len(self.pool):
            ticker = self.pool[idx]
            
            # Dynamische Erweiterung: Pool während der Laufzeit neu laden
            if idx % 10 == 0:
                self.load_pool() 

            if ticker in self.processed_tickers or self.should_skip(ticker):
                self.stats["skipped"] += 1
                idx += 1
                continue

            try:
                # Download
                y_obj = yf.Ticker(ticker)
                recent_df = y_obj.history(period="5d", interval="5m").reset_index()
                
                if not recent_df.empty:
                    folder = f"{HERITAGE_ROOT}2026/{ticker}"
                    path = f"{folder}/registry.parquet"
                    os.makedirs(folder, exist_ok=True)

                    recent_df['Ticker'] = ticker
                    if 'Datetime' in recent_df.columns:
                        recent_df = recent_df.rename(columns={'Datetime': 'Date'})

                    if os.path.exists(path):
                        existing = pd.read_parquet(path)
                        df_final = pd.concat([existing, recent_df]).drop_duplicates(subset=['Date'])
                    else:
                        df_final = recent_df

                    df_final.to_parquet(path, index=False, compression='snappy')
                    self.stats["done"] += 1
                    self.log(f"✅ [{idx+1}/{len(self.pool)}] {ticker} synchronisiert.")

                self.processed_tickers.add(ticker)
                time.sleep(0.7) # Stabiler Takt

            except Exception as e:
                self.log(f"⚠️ Fehler {ticker}: {str(e)[:40]}")
            
            idx += 1

        self.log(f"🏁 Zyklus beendet. Erneuert: {self.stats['done']} | Migriert: {self.stats['migrated']} | Skip: {self.stats['skipped']}")

if __name__ == "__main__":
    AureumSentinelV288().run()
