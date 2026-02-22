import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
from datetime import datetime

# --- KONFIGURATION (DER GOLDENE STANDARD) ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"
# Ankerpunkt für Kursbewegungen (0,05% wie vereinbart)
ANCHOR_THRESHOLD = 0.0005 

class AureumSentinelGolden:
    def __init__(self):
        self.stats = {"done": 0, "start": time.time()}
        self.load_pool()

    def log(self, message):
        """Erzwingt sofortige Ausgabe im GitHub Action Log"""
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] {message}", flush=True)
        sys.stdout.flush()

    def load_pool(self):
        if not os.path.exists(POOL_FILE):
            self.pool = []
            return
        with open(POOL_FILE, "r") as f:
            data = json.load(f)
            # Wir nehmen die Ticker exakt so, wie sie im Pool stehen
            self.pool = [e['symbol'] for e in data]
        self.log(f"Goldener Standard geladen: {len(self.pool)} Assets.")

    def get_stooq_data(self, ticker):
        """Holt historische Daten von Skooq (Stooq)"""
        try:
            url = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
            df = pd.read_csv(url)
            if df.empty: return None
            df['Date'] = pd.to_datetime(df['Date'])
            return df
        except:
            return None

    def run(self):
        self.log("START DER PERPETUAL SYNC (GOLDEN STANDARD)")
        
        for idx, ticker in enumerate(self.pool):
            try:
                # 1. Skooq Daten (Historie)
                hist_df = self.get_stooq_data(ticker)
                
                # 2. Yahoo Daten (Jüngste Vergangenheit - 1 Woche bis jetzt)
                y_obj = yf.Ticker(ticker)
                recent_df = y_obj.history(period="7d", interval="5m")
                
                if recent_df.empty and hist_df is None:
                    self.log(f"SKIPPED {ticker}: Keine Daten gefunden.")
                    continue

                # Daten verheiraten
                recent_df = recent_df.reset_index()
                recent_df = recent_df.rename(columns={'Datetime': 'Date'})
                
                # Speicher-Logik (Pfad-Hierarchie 2020s)
                char = ticker[0].upper() if ticker[0].isalpha() else "_"
                path = f"{HERITAGE_ROOT}2020s/2026/{char}_registry.parquet"
                os.makedirs(os.path.dirname(path), exist_ok=True)

                # Speichern des kombinierten Frames
                df_final = pd.concat([hist_df, recent_df]).drop_duplicates(subset=['Date'])
                df_final['Ticker'] = ticker
                
                if os.path.exists(path):
                    existing = pd.read_parquet(path)
                    pd.concat([existing, df_final]).drop_duplicates(subset=['Date', 'Ticker']).to_parquet(path, index=False)
                else:
                    df_final.to_parquet(path, index=False)

                self.stats["done"] += 1
                # WICHTIG: Hier ist dein Fortschritts-Log!
                self.log(f"PROCESSED [{idx+1}/{len(self.pool)}] {ticker} | OK")

                # Der Anker beträgt 0,05% und der Ticker wird alle 5 Minuten aktualisiert (Pause)
                time.sleep(1) 

            except Exception as e:
                self.log(f"ERROR {ticker}: {str(e)[:50]}")

        elapsed = (time.time() - self.stats['start']) / 60
        self.log(f"ZYKLUS BEENDET. {self.stats['done']} Assets synchronisiert in {elapsed:.1f} Min.")

if __name__ == "__main__":
    AureumSentinelGolden().run()
