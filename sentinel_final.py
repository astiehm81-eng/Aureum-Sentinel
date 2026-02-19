import os
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime

FILENAME = "sentinel_master_storage.csv"

def get_stooq_data(symbol):
    """Holt aktuelle Daten von Stooq (weniger geschützt)"""
    try:
        # Stooq nutzt oft Ticker wie ENR.DE oder SAP.DE
        url = f"https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        df_stooq = pd.read_csv(url)
        
        if not df_stooq.empty:
            price = float(df_stooq['Close'].iloc[0])
            return price
    except Exception as e:
        print(f"Stooq-Fehler bei {symbol}: {e}")
    return None

def run_sentinel():
    # Wir nutzen Stooq-Ticker (identisch mit Yahoo für DE)
    assets = {
        "DE0007164600": {"symbol": "SAP.DE", "name": "SAP SE"},
        "DE000ENER6Y0": {"symbol": "ENR.DE", "name": "Siemens Energy"}
    }
    
    print(f"🛡️ Sentinel Context-Check (Stooq-Schnittstelle)")
    
    if os.path.exists(FILENAME):
        master_df = pd.read_csv(FILENAME, index_col=0)
        master_df.index = pd.to_datetime(master_df.index)
    else:
        master_df = pd.DataFrame(columns=['Price', 'Source', 'ISIN'])

    print(f"\n{'Asset':<18} | {'Yahoo (Legacy)':<15} | {'Stooq (Live)':<12} | {'Status'}")
    print("-" * 70)

    for isin, info in assets.items():
        # 1. Yahoo als historischer Anker
        ticker = yf.Ticker(info['symbol'])
        y_price = ticker.history(period="1d")['Close'].iloc[-1]
        
        # 2. Stooq als detailliertere/offene Quelle
        stooq_price = get_stooq_data(info['symbol'])
        
        y_str = f"{y_price:.2f} €" if y_price else "N/A"
        s_str = f"{stooq_price:.2f} €" if stooq_price else "N/A"
        
        if stooq_price:
            # 0,1% Regel
            last_entry = master_df[master_df['ISIN'] == isin]
            last_val = last_entry['Price'].iloc[-1] if not last_entry.empty else 0
            
            if abs((stooq_price - last_val) / (last_val if last_val > 0 else 1)) >= 0.001:
                new_row = pd.DataFrame([{
                    'Price': stooq_price, 'Source': 'Stooq_Data', 'ISIN': isin
                }], index=[pd.Timestamp.now()])
                master_df = pd.concat([master_df, new_row])
                status = "✅ NEUER ANKER"
            else:
                status = "⏳ STABIL"
        else:
            status = "❌ OFFLINE"

        print(f"{info['name']:<18} | {y_str:>15} | {s_str:>12} | {status}")

    master_df.to_csv(FILENAME)
    print("-" * 70)

if __name__ == "__main__":
    run_sentinel()
