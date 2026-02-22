import pandas as pd
import yfinance as yf
import os
import json
import time
import sys
from datetime import datetime

# --- KONFIGURATION (V287 - STABILE EVOLUTION) ---
HERITAGE_ROOT = "heritage/"
POOL_FILE = "isin_pool.json"
BLACKLIST_FILE = "blacklist.json"

class AureumSentinelV287:
    def __init__(self):
        self.stats = {"done": 0, "healed": 0, "start": time.time()}
        self.load_pool()

    def log(self, message):
        """Erzwingt sofortige Ausgabe im GitHub Log"""
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] {message}", flush=True)
        sys.stdout.flush()

    def load_pool(self):
        """Deduplizierung und Validierung des Pools"""
        if not os.path.exists(POOL_FILE):
            self.pool = []
            return
        with open(POOL_FILE, "r") as f:
            data = json.load(f)
            # Nur Ticker nehmen, Dubletten im Speicher vorfiltern
            seen = set()
            self.pool = []
            for e in data:
                if 'symbol' in e and e['symbol'] not in seen:
                    self.pool.append(e['symbol'])
                    seen.add(e['symbol'])
        self.log(f"POOL: {len(self.pool)} Assets geladen (bereinigt).")

    def get_stooq_data(self, ticker):
        """Historische Komponente von Skooq"""
        try:
            url = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
            df = pd.read_csv(url)
            if df.empty or 'Date' not in df.columns: return None
            df['Date'] = pd.to_datetime(df['Date'])
            return df
        except:
            return None

    def run(self):
        self.log("START SYNC V287 - SELBSTHEILUNG AKTIV")
        
        for idx, ticker in enumerate(self.pool):
            try:
                # 1. Daten holen (Yahoo für 5m Intervalle)
                y_obj = yf.Ticker(ticker)
                recent_df = y_obj.history(period="7d", interval="5m").reset_index()
                
                if recent_df.empty:
                    # Falls Yahoo leer ist, versuchen wir es gar nicht erst weiter
                    continue

                # Pfad-Hierarchie sicherstellen
                char = ticker[0].upper() if ticker[0].isalpha() else "_"
                folder = f"{HERITAGE_ROOT}2020s/2026"
                path = f"{folder}/{char}_registry.parquet"
                os.makedirs(folder, exist_ok=True)

                recent_df['Ticker'] = ticker
                if 'Datetime' in recent_df.columns:
                    recent_df = recent_df.rename(columns={'Datetime': 'Date'})

                # 2. Merging mit Fehler-Selbstheilung
                if os.path.exists(path):
                    try:
                        existing = pd.read_parquet(path)
                        # Heilung: Entfernt alte Fehler oder korrupte Zeilen durch Re-Merge
                        df_final = pd.concat([existing, recent_df]).drop_duplicates(subset=['Date', 'Ticker'])
                    except Exception:
                        self.log(f"HEAL: Korrupte Datei {char}_registry repariert.")
                        # Wenn die Datei kaputt ist, versuchen wir Skooq für die Historie zu holen
                        hist_df = self.get_stooq_data(ticker)
                        df_final = pd.concat([hist_df, recent_df]) if hist_df is not None else recent_df
                        self.stats["healed"] += 1
                else:
                    # Neues Asset: Skooq + Yahoo verheiraten
                    hist_df = self.get_stooq_data(ticker)
                    df_final = pd.concat([hist_df, recent_df]) if hist_df is not None else recent_df

                # 3. Sofortiges Speichern (kein Buffer-Risiko)
                df_final.to_parquet(path, engine='pyarrow', index=False)
                self.stats["done"] += 1
                
                # Jedes Asset loggen für Live-Fortschritt
                self.log(f"OK [{idx+1}/{len(self.pool)}] {ticker}")

                # Taktung (Stabilität vor Speed)
                time.sleep(0.8)

            except Exception as e:
                self.log(f"SKIP {ticker}: {str(e)[:40]}")

        elapsed = (time.time() - self.stats['start']) / 60
        self.log(f"FINISH: {self.stats['done']} Assets | Healed: {self.stats['healed']} | Zeit: {elapsed:.1f}m")

if __name__ == "__main__":
    AureumSentinelV287().run()
