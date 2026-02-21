import pandas as pd
import yfinance as yf
import requests
import io
import os
import json
from datetime import datetime, timedelta

# --- KONFIGURATION ---
DB_FILE = "aureum_heritage_db.json"
POOL_FILE = "isin_pool.json" # Der ISIN-Pool vor der KI-Suche
ANCHOR_THRESHOLD = 0.0005    # 0,05% Anker aus den Anforderungen

class HeritageBuilder:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self.db = self.load_json(DB_FILE, {})
        self.pool = self.load_json(POOL_FILE, [])

    def load_json(self, path, default):
        if os.path.exists(path):
            with open(path, "r") as f: return json.load(f)
        return default

    def get_stooq_longterm(self, ticker):
        """Holt die Jahrzehnte-Historie von Stooq."""
        symbol = ticker.split('.')[0].lower()
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200 and len(r.content) > 100:
                df = pd.read_csv(io.StringIO(r.text))
                return df.to_dict(orient='records')
        except: pass
        return None

    def get_yahoo_recent(self, ticker):
        """Holt die Daten der letzten 7 Tage von Yahoo."""
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="7d")
            if not df.empty:
                df.reset_index(inplace=True)
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
                return df.to_dict(orient='records')
        except: pass
        return None

    def build_heritage(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starte Datenbank-Aufbau...")
        
        for entry in self.pool:
            ticker = entry['symbol']
            if ticker in self.db: continue # Nur neue oder fehlende Assets laden
            
            print(f"Verarbeite {ticker}...")
            
            # Die "Hochzeit" der Daten
            history_stooq = self.get_stooq_longterm(ticker)
            recent_yahoo = self.get_yahoo_recent(ticker)
            
            if history_stooq:
                self.db[ticker] = {
                    "heritage_long": history_stooq,
                    "recent_yahoo": recent_yahoo,
                    "last_update": datetime.now().isoformat(),
                    "anchor": recent_yahoo[-1]['Close'] if recent_yahoo else None
                }
        
        self.save_db()

    def save_db(self):
        with open(DB_FILE, "w") as f:
            json.dump(self.db, f, indent=2)
        print("Heritage-Datenbank erfolgreich gespeichert.")

if __name__ == "__main__":
    builder = HeritageBuilder()
    builder.build_heritage()
