import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
from datetime import datetime, timedelta

# --- DER NEUE GOLDSTANDARD (V288.4) ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"

class AureumSentinelV288_4:
    def __init__(self):
        self.stats = {"done": 0, "skipped": 0, "start": time.time()}
        self.load_pool()

    def log(self, message):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] {message}", flush=True)
        sys.stdout.flush()

    def load_pool(self):
        if not os.path.exists(POOL_FILE): return
        with open(POOL_FILE, "r") as f:
            data = json.load(f)
            self.pool = [e['symbol'] for e in data if 'symbol' in e]
        self.log(f"Pool geladen: {len(self.pool)} Assets.")

    def should_skip(self, path):
        """Verbesserung: Schaut in den Speicher, um Doppelt-Arbeit zu vermeiden"""
        if not os.path.exists(path): return False
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            # Wenn die Datei jünger als 5 Minuten ist, haben wir erst gerade aktualisiert
            if datetime.now() - mtime < timedelta(minutes=5):
                return True
        except: pass
        return False

    def get_stooq_data(self, ticker):
        """Historische Komponente von Skooq"""
        try:
            url = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
            df = pd.read_csv(url)
            if df.empty: return None
            df['Date'] = pd.to_datetime(df['Date'])
            return df
        except: return None

    def run(self):
        self.log("🚀 START SYNC (DER NEUE GOLDSTANDARD V288.4)")
        
        for idx, ticker in enumerate(self.pool):
            try:
                char = ticker[0].upper() if ticker[0].isalpha() else "_"
                path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
                
                # Check 1: Schon im Speicher und aktuell?
                if self.should_skip(path):
                    self.stats["skipped"] += 1
                    # Nur alle 50 Skips loggen, um das Log sauber zu halten
                    if idx % 50 == 0: self.log(f"Skipping {ticker} (bereits aktuell)")
                    continue

                # Check 2: Daten holen (Yahoo für 5m Intervalle)
                y_obj = yf.Ticker(ticker)
                recent_df = y_obj.history(period="5d", interval="5m").reset_index()
                
                if recent_df.empty:
                    self.log(f"[-] {ticker} keine Daten.")
                    continue

                recent_df['Ticker'] = ticker
                if 'Datetime' in recent_df.columns:
                    recent_df = recent_df.rename(columns={'Datetime': 'Date'})

                # Speichern & Verheiraten
                os.makedirs(os.path.dirname(path), exist_ok=True)
                if os.path.exists(path):
                    existing = pd.read_parquet(path)
                    pd.concat([existing, recent_df]).drop_duplicates(subset=['Date', 'Ticker']).to_parquet(path, index=False)
                else:
                    # Bei neuen Assets auch Skooq für Historie anfragen
                    hist_df = self.get_stooq_data(ticker)
                    final = pd.concat([hist_df, recent_df]) if hist_df is not None else recent_df
                    final.to_parquet(path, index=False)

                self.stats["done"] += 1
                self.log(f"[OK] {idx+1}/{len(self.pool)}: {ticker}")

                # Taktung einhalten (0.5s statt 1s für mehr Speed)
                time.sleep(0.5)

            except Exception as e:
                self.log(f"[!] Fehler {ticker}: {str(e)[:50]}")

        self.log(f"🏁 Zyklus beendet. Erneuert: {self.stats['done']} | Übersprungen: {self.stats['skipped']}")

if __name__ == "__main__":
    AureumSentinelV288_4().run()
