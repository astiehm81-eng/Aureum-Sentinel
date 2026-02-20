import pandas as pd
import os, json, glob

# --- EISERNER STANDARD V101.1 (THE PURGE & RESTRUCTURE) ---
BUFFER_FILE = "current_buffer.json"
HERITAGE_BASE = "heritage_vault"

def purge_flat_files():
    """Löscht alle Dateien, die direkt im Root von heritage_vault liegen."""
    print("🧹 Starte 'The Purge': Lösche flache Dateien im Root...", flush=True)
    # Findet alle Dateien direkt im Ordner (keine Unterordner)
    flat_files = [f for f in os.listdir(HERITAGE_BASE) if os.path.isfile(os.path.join(HERITAGE_BASE, f))]
    
    for f in flat_files:
        try:
            os.remove(os.path.join(HERITAGE_BASE, f))
            print(f"🗑️ Gelöscht: {f}", flush=True)
        except Exception as e:
            print(f"⚠️ Fehler beim Löschen von {f}: {e}", flush=True)

def move_buffer_to_heritage_v101_1():
    """Sortiert Buffer ein und räumt danach radikal auf."""
    if not os.path.exists(BUFFER_FILE): 
        # Auch wenn kein Buffer da ist, führen wir den Purge durch
        purge_flat_files()
        return

    with open(BUFFER_FILE, 'r') as f:
        buffer_data = json.load(f)

    print("📁 Sortiere Buffer in Dekaden-Ordner ein...", flush=True)
    for symbol, ticks in buffer_data.items():
        if not ticks: continue
        df = pd.DataFrame(ticks)
        df['decade'] = (df['t'].str[:4].astype(int) // 10 * 10).astype(str) + "s"

        for decade, decade_df in df.groupby('decade'):
            target_dir = os.path.join(HERITAGE_BASE, decade)
            if not os.path.exists(target_dir): os.makedirs(target_dir)
            
            file_path = os.path.join(target_dir, f"{symbol}.parquet")
            clean_df = decade_df[['t', 'p']].rename(columns={'t': 'Date', 'p': 'Price'})

            if os.path.exists(file_path):
                old_df = pd.read_parquet(file_path)
                clean_df = pd.concat([old_df, clean_df]).drop_duplicates(subset=['Date'], keep='last')
            
            clean_df.to_parquet(file_path, index=False)
    
    # Jetzt der radikale Schnitt:
    purge_flat_files()
    
    # Buffer leeren
    os.remove(BUFFER_FILE)
    print("✨ Reinigung und Archivierung abgeschlossen.", flush=True)

if __name__ == "__main__":
    # Dieser Aufruf sollte in deinem täglichen Archiv-Lauf stehen
    move_buffer_to_heritage_v101_1()
