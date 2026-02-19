import pandas as pd
import pandas_datareader.data as web
import os, json, time, glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# --- EISERNER STANDARD V68 (CLEAN ARCHITECTURE) ---
HERITAGE_DIR = "heritage_vault"
POOL_FILE = "isin_pool.json"
HUMAN_REPORT = "vault_status.txt"
START_TIME = time.time()

def cleanup_legacy_garbage():
    """Löscht alten Datenmüll aus dem Root-Verzeichnis."""
    patterns = ["sentinel_*.csv", "sentinel_*.parquet", "sentinel_*.txt", "vault_health.json"]
    for pattern in patterns:
        for f in glob.glob(pattern):
            try: os.remove(f)
            except: pass
    print("🧹 Legacy-Müll entfernt.")

def generate_human_report():
    """Erzeugt den Text-Statusbericht für das Handy."""
    lines = [f"🛡️ AUREUM SENTINEL V68 - STATUS [{datetime.now().strftime('%d.%m. %H:%M')}]", "="*40]
    if os.path.exists(HERITAGE_DIR):
        files = sorted(os.listdir(HERITAGE_DIR))
        total_assets = 0
        for f in files:
            if f.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(HERITAGE_DIR, f))
                assets = int(df['Ticker'].nunique())
                total_assets = max(total_assets, assets)
                lines.append(f"{f[:15]:15} | {assets:4} Assets")
        
        coverage = (total_assets / 10000) * 100
        lines.append("="*40)
        lines.append(f"📊 Abdeckung: {coverage:.2f}% der 10k Assets")
    else:
        lines.append("Vault im Aufbau...")
    
    with open(HUMAN_REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# ... (Hier die restlichen Funktionen fetch_data, save_shards, run_v68 einfügen) ...
# WICHTIG: Am Ende von run_v68() muss generate_human_report() stehen!
