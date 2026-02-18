import csv
from datetime import datetime

# ... (Rest des vorherigen Scraper-Codes bleibt gleich) ...

if __name__ == "__main__":
    start_time = time.time()
    driver = setup_driver()
    all_results = [] # Für die TXT (Übersicht)
    history_rows = [] # Für die CSV (Historie)
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Core & Markt Scans (wie gehabt)
        combined_wkns = CORE_WKNS + MARKET_POOL
        # Wir machen hier die Batches...
        # Innerhalb deiner Loop, wo du die Daten extrahierst:
        # bid_val, ask_val, news_txt = extrahiere_daten()
        
        # BEISPIEL-LOGIK für die CSV-Speicherung (in der Schleife):
        # history_rows.append([timestamp, wkn, bid_val, ask_val, news_txt])
        # all_results.append(f"📦 {wkn} | B: {bid_val} | A: {ask_val}")
        
    finally:
        driver.quit()

    # --- SPEICHERN DER HISTORIE (APPEND MODUS) ---
    csv_file = 'sentinel_history.csv'
    file_exists = os.path.isfile(csv_file)
    
    with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Header nur schreiben, wenn Datei neu ist
        if not file_exists:
            writer.writerow(['Timestamp', 'WKN', 'Bid', 'Ask', 'News'])
        writer.writerows(history_rows)

    # --- SPEICHERN DER AKTUELLEN ÜBERSICHT (ÜBERSCHREIBEN) ---
    with open("sentinel_data.txt", "w", encoding="utf-8") as f:
        f.write(f"Zuletzt aktualisiert: {timestamp}\n")
        f.write("\n".join(all_results))
