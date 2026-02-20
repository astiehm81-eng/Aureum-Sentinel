import json, os

def sync_pool_with_gemini(symbols):
    """Baut die isin_pool.json sauber auf."""
    pool_data = [{"symbol": s} for s in symbols]
    with open("isin_pool.json", "w") as f:
        json.dump(pool_data, f, indent=4)
    print(f"✅ Pool mit {len(pool_data)} Assets synchronisiert.")

# Hier kommen deine Ticker aus den Blöcken rein
ticker_list = ["SAP.DE", "SIE.DE", "AAPL", "TSLA"] 

if __name__ == "__main__":
    sync_pool_with_gemini(ticker_list)
