import os
import pandas as pd
import yfinance as yf
import requests
import io
from datetime import datetime

class AureumSentinelFusion:
    def __init__(self, isin, symbol):
        self.isin = isin
        self.symbol = symbol
        self.storage_file = f"sentinel_{isin}.parquet"
        
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛡️ {msg}", flush=True)

    def get_tradegate_history(self):
        """Lädt die echte CSV-Historie direkt von Tradegate ohne Webseiten-Scraping"""
        try:
            # Direkte URL zum CSV-Export (Beispielstruktur für Tradegate Historie)
            url = f"https://www.tradegate.de/export.php?isin={self.isin}&type=csv"
            headers = {'User-Agent': 'Mozilla/5.0'}
            res = requests.get(url, headers=headers, timeout=15)
            
            if res.status_code == 200:
                # Wir lesen die CSV ein (Tradegate nutzt oft Semikolon)
                df_tg = pd.read_csv(io.StringIO(res.text), sep=';', decimal=',')
                df_tg['Datum'] = pd.to_datetime(df_tg['Datum'], dayfirst=True)
                df_tg = df_tg.rename(columns={'Schluss': 'Price', 'Datum': 'Date'})
                return df_tg[['Date', 'Price']].set_index('Date')
        except Exception as e:
            self.log(f"Tradegate-Historie Export nicht verfügbar: {e}")
        return pd.DataFrame()

    def fuse_data(self):
        self.log(f"Starte Daten-Fusion für {self.isin}")
        
        # 1. Yahoo für die Langzeit-Basis (30 Jahre)
        self.log("Beziehe Yahoo-Legacy Daten...")
        y_data = yf.Ticker(self.symbol).history(period="max")[['Close']]
        y_data.columns = ['Price']
        y_data.index = pd.to_datetime(y_data.index).tz_localize(None)
        
        # 2. Tradegate für die präzise Kurzzeit-Historie
        self.log("Infiltriere Tradegate-Historie (CSV-Schnittstelle)...")
        tg_data = self.get_tradegate_history()
        
        if not tg_data.empty:
            # Die Flickstelle: Wir nehmen Yahoo bis zum Start von Tradegate
            split_date = tg_data.index.min()
            self.log(f"Flickstelle identifiziert bei: {split_date}")
            
            y_legacy = y_data[y_data.index < split_date].copy()
            y_legacy['Source'] = 'Yahoo_Legacy'
            
            tg_current = tg_data.copy()
            tg_current['Source'] = 'Tradegate_Archive'
            
            # Zusammenfügen
            final_df = pd.concat([y_legacy, tg_current]).sort_index()
        else:
            self.log("⚠️ Tradegate-Archiv leer, nutze reinen Yahoo-Stock.")
            final_df = y_data
            final_df['Source'] = 'Yahoo_Only'

        # Speichern
        final_df.to_parquet(self.storage_file, compression='snappy')
        self.log(f"✅ Fusion abgeschlossen. {len(final_df)} Datenpunkte im Speicher.")

if __name__ == "__main__":
    # Test mit Siemens Energy
    fusion = AureumSentinelFusion("DE000ENER6Y0", "ENR.DE")
    fusion.fuse_data()
