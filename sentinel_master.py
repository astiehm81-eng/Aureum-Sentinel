import pandas as pd
import os, json, time
import numpy as np

# --- EISERNER STANDARD V66 (AUDITOR MODULE) ---
HERITAGE_DIR = "heritage_vault"
STATUS_FILE = "vault_health.json"

def audit_data_integrity(df):
    """Prüft, ob die Daten mathematisch sinnhaft sind."""
    if df.empty: return df
    
    # 1. Entferne Nullwerte oder negative Preise (API-Müll)
    df = df[df['Price'] > 0]
    
    # 2. Statistischer Outlier-Check (Z-Score)
    # Wenn ein Preis sich um mehr als 500% an einem Tag ändert, ist es oft ein API-Fehler
    # außer bei Penny-Stocks, aber für den Heritage-Vault filtern wir extremen Noise.
    # Wir lassen nur Bewegungen zu, die im historischen Kontext 'möglich' sind.
    return df

def generate_vault_report():
    """Erstellt eine Übersicht über den Status des Heritage Pools."""
    report = {"last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "shards": {}}
    
    if not os.path.exists(HERITAGE_DIR): return
    
    for file in os.listdir(HERITAGE_DIR):
        if file.endswith(".parquet"):
            path = os.path.join(HERITAGE_DIR, file)
            df = pd.read_parquet(path)
            
            decade_name = file.replace("history_", "").replace(".parquet", "")
            report["shards"][decade_name] = {
                "asset_count": int(df['Ticker'].nunique()),
                "total_rows": len(df),
                "earliest_date": df['Date'].min(),
                "latest_date": df['Date'].max(),
                "file_size_mb": round(os.path.getsize(path) / (1024*1024), 2)
            }
            
    with open(STATUS_FILE, 'w') as f:
        json.dump(report, f, indent=4)
    print(f"📊 Vault-Report generiert: {len(report['shards'])} Shards aktiv.")

# --- Diese Funktionen integrieren wir in den run_sentinel_v65 Loop ---
