edef setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu') 
    # NEU: Erzwinge deutsche Sprache und ein echtes Browser-Profil
    options.add_argument("--lang=de-DE")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def discover_market():
    """Findet automatisch alle relevanten WKNs auf dem Markt"""
    print("🔍 Phase 1: Suche Markt-Assets (Discovery)...")
    driver = setup_driver()
    try:
        # NEU: Wir nutzen die Desktop-Ansicht der deutschen Aktien
        driver.get("https://www.ls-tc.de/de/aktien/deutschland")
        time.sleep(7) # Etwas mehr Zeit für GitHub-Latenz
        html = driver.page_source
        
        # Suche nach WKNs (6-stellige Alphanumerische Codes)
        wkns = list(set(re.findall(r'/de/aktie/([A-Z0-9]{6})', html)))
        
        if not wkns:
            # Fallback: Falls die URL-Struktur auf GitHub anders greift
            wkns = list(set(re.findall(r'WKN:\s*([A-Z0-9]{6})', html)))

        print(f"✨ {len(wkns)} Assets identifiziert.")
        return wkns
    except Exception as e:
        print(f"❌ Discovery Fehler: {e}")
        return []
    finally:
        driver.quit()
