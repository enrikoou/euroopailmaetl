import logging
import sqlite3
import json
from datetime import datetime, timedelta
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException

# Seadistame logimise, et väljund sarnaneks toodangukeskkonna standarditele
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- SEADISTUSED ---
AB_FAIL = "euroopa_ilma_andmeladu.db"
GEO_API_URL = "https://restcountries.com/v3.1/region/europe"
ILMA_ARHIIV_URL = "https://archive-api.open-meteo.com/v1/archive"

# Open-Meteo arhiivil on väike viive. Nihutame kuupäevi 3 päeva tahapoole, et tagada andmete olemasolu.
ARUANDE_LOPP = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
ARUANDE_ALGUS = (datetime.now() - timedelta(days=33)).strftime("%Y-%m-%d")

def seadista_paringute_sessioon() -> Session:
    """Loob päringute sessiooni koos automaatse korduskatsete süsteemiga."""
    sessioon = Session()
    kordus_strateegia = Retry(
        total=3,
        backoff_factor=1,  # Ooteajad korduste vahel: 1s, 2s, 4s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=kordus_strateegia)
    sessioon.mount("https://", adapter)
    return sessioon

def loo_andmebaasi_skeem(kursor):
    """Käivitab andmebaasi skeemi loomise, kasutades idempotentseid käske."""
    
    
    kursor.executescript("""
        CREATE TABLE IF NOT EXISTS stg_toored_geoandmed (
            laadimise_aeg TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            andmed JSON
        );
        
        CREATE TABLE IF NOT EXISTS stg_toores_ilm (
            riigi_kood TEXT PRIMARY KEY,
            laadimise_aeg TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            andmed JSON
        );
        
        CREATE TABLE IF NOT EXISTS dim_riik (
            riigi_kood TEXT PRIMARY KEY,
            riigi_nimi TEXT,
            pealinn TEXT,
            laiuskraad REAL,
            pikkuskraad REAL
        );
        
        CREATE TABLE IF NOT EXISTS fct_paevane_ilm (
            riigi_kood TEXT,
            kuupaev DATE,
            max_temp_c REAL,
            min_temp_c REAL,
            sademete_hulk_mm REAL,
            max_tuulekiirus_kmh REAL,
            paikesepaiste_tundides REAL,
            PRIMARY KEY (riigi_kood, kuupaev),
            FOREIGN KEY(riigi_kood) REFERENCES dim_riik(riigi_kood)
        );
    """)

def laadi_andmed_apidest(yhendus, sessioon):
    """Tõmbab andmed API-dest ja salvestab algkujul vahetabelitesse."""
    kursor = yhendus.cursor()
    logging.info("Euroopa geoandmete allalaadimine...")
    
    try:
        geo_vastus = sessioon.get(GEO_API_URL, timeout=10)
        geo_vastus.raise_for_status()
        riikide_andmed = geo_vastus.json()
        
        # Salvestame toorandmete hetketõmmise silumise ja ajaloo jaoks
        kursor.execute("INSERT INTO stg_toored_geoandmed (andmed) VALUES (?)", (json.dumps(riikide_andmed),))
        yhendus.commit()
    except RequestException as viga:
        logging.error(f"Geoandmete allalaadimine ebaõnnestus. Andmetoru peatatakse. Viga: {viga}")
        return False

    logging.info(f"Geoandmed leitud {len(riikide_andmed)} riigi kohta. Alustame ilmaandmete pärimist...")
    
    for riik in riikide_andmed:
        kood = riik.get('cca3', 'TUNDMATU')
        pealinnad = riik.get('capital', [])
        koordinaadid = riik.get('capitalInfo', {}).get('latlng', [])
        
        # Jätame vahele riigid, millel puuduvad standardsed pealinna koordinaadid (nt Svalbard)
        if not pealinnad or len(koordinaadid) != 2:
            logging.debug(f"Jätan vahele {kood}: Puuduvad kehtivad pealinna koordinaadid.")
            continue
            
        parameetrid = {
            "latitude": koordinaadid[0],
            "longitude": koordinaadid[1],
            "start_date": ARUANDE_ALGUS,
            "end_date": ARUANDE_LOPP,
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "wind_speed_10m_max", "sunshine_duration"],
            "timezone": "auto"
        }
        
        try:
            ilma_vastus = sessioon.get(ILMA_ARHIIV_URL, params=parameetrid, timeout=10)
            ilma_vastus.raise_for_status()
            
            # REPLACE võimaldab skripti ohutult mitu korda käivitada, uuendades vajadusel andmeid
            kursor.execute(
                "INSERT OR REPLACE INTO stg_toores_ilm (riigi_kood, andmed) VALUES (?, ?)", 
                (kood, json.dumps(ilma_vastus.json()))
            )
        except RequestException as viga:
            logging.warning(f"Ilma API päring ebaõnnestus riigile {kood}: {viga}. Jätan vahele.")
            
    yhendus.commit()
    return True

def tootle_ja_salvesta_andmed(yhendus):
    """Töötleb toorandmeid, rakendab puhastusloogikat ja salvestab dimensiooni-/faktitabelitesse."""
    kursor = yhendus.cursor()
    logging.info("Toorandmete töötlemine...")
    
    # --- 1. Dimensioonitabeli täitmine ---
    kursor.execute("SELECT andmed FROM stg_toored_geoandmed ORDER BY laadimise_aeg DESC LIMIT 1")
    geo_rida = kursor.fetchone()
    if not geo_rida:
        logging.error("Vahetabelist ei leitud geoandmeid.")
        return
        
    riigid = json.loads(geo_rida[0])
    dim_kirjed = []
    
    for r in riigid:
        koordinaadid = r.get('capitalInfo', {}).get('latlng', [None, None])
        if koordinaadid[0] is not None:
            dim_kirjed.append((
                r.get('cca3'),
                r.get('name', {}).get('common'),
                r.get('capital', ['Tundmatu'])[0],
                koordinaadid[0],
                koordinaadid[1]
            ))
            
    kursor.executemany("""
        INSERT OR REPLACE INTO dim_riik (riigi_kood, riigi_nimi, pealinn, laiuskraad, pikkuskraad)
        VALUES (?, ?, ?, ?, ?)
    """, dim_kirjed)
    
    # --- 2. Faktitabeli täitmine ---
    kursor.execute("SELECT riigi_kood, andmed FROM stg_toores_ilm")
    ilma_toorandmed = kursor.fetchall()
    
    fct_kirjed = []
    for kood, andmed_json in ilma_toorandmed:
        paevased_mootikud = json.loads(andmed_json).get('daily', {})
        if not paevased_mootikud:
            continue
            
        # Ühendame listid, et itereerida puhtalt päevade kaupa
        uhendatud_andmed = zip(
            paevased_mootikud.get('time', []),
            paevased_mootikud.get('temperature_2m_max', []),
            paevased_mootikud.get('temperature_2m_min', []),
            paevased_mootikud.get('precipitation_sum', []),
            paevased_mootikud.get('wind_speed_10m_max', []),
            paevased_mootikud.get('sunshine_duration', [])
        )
        
        for kuupaev, max_t, min_t, sademed, tuul, paike_sek in uhendatud_andmed:
            # Käsitleme API potentsiaalseid tühjasid väärtusi, määrates vajadusel vaikimisi 0.0
            paike_tundi = round((paike_sek or 0) / 3600.0, 2)
            
            fct_kirjed.append((
                kood, 
                kuupaev, 
                max_t or 0.0, 
                min_t or 0.0, 
                sademed or 0.0, 
                tuul or 0.0, 
                paike_tundi
            ))
            
    kursor.executemany("""
        INSERT OR IGNORE INTO fct_paevane_ilm 
        (riigi_kood, kuupaev, max_temp_c, min_temp_c, sademete_hulk_mm, max_tuulekiirus_kmh, paikesepaiste_tundides)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, fct_kirjed)
    
    yhendus.commit()
    logging.info(f"Edukalt andmelattu laaditud {len(fct_kirjed)} ilmakirjet.")

def loo_aruandluse_vaated(yhendus):
    """Koostab andmete analüüsimiseks vajalikud SQL-vaated."""
    kursor = yhendus.cursor()
    kursor.executescript("""
        CREATE VIEW IF NOT EXISTS vw_keskmise_temp_edetabel AS
        SELECT 
            d.pealinn, 
            d.riigi_nimi,
            ROUND(AVG((f.max_temp_c + f.min_temp_c) / 2.0), 2) AS keskmine_temp_c
        FROM fct_paevane_ilm f
        JOIN dim_riik d ON f.riigi_kood = d.riigi_kood
        GROUP BY d.riigi_kood
        ORDER BY keskmine_temp_c DESC;
        
        CREATE VIEW IF NOT EXISTS vw_suurim_sademete_hulk AS
        SELECT 
            d.riigi_nimi, 
            ROUND(SUM(f.sademete_hulk_mm), 2) AS kogusademed_mm
        FROM fct_paevane_ilm f
        JOIN dim_riik d ON f.riigi_kood = d.riigi_kood
        GROUP BY d.riigi_kood
        ORDER BY kogusademed_mm DESC;
        
        CREATE VIEW IF NOT EXISTS vw_30_paeva_kokkuvote AS
        SELECT 
            d.riigi_nimi,
            MIN(f.kuupaev) as perioodi_algus,
            MAX(f.kuupaev) as perioodi_lopp,
            MAX(f.max_temp_c) AS korgeim_temp,
            MIN(f.min_temp_c) AS madalaim_temp,
            ROUND(SUM(f.sademete_hulk_mm), 2) AS kogusademed,
            MAX(f.max_tuulekiirus_kmh) AS max_tuul,
            ROUND(SUM(f.paikesepaiste_tundides), 2) AS kogu_paikesepaiste
        FROM fct_paevane_ilm f
        JOIN dim_riik d ON f.riigi_kood = d.riigi_kood
        GROUP BY d.riigi_kood;
    """)
    yhendus.commit()

def main():
    algusaeg = datetime.now()
    logging.info("Ilmaandmete ETL-andmetoru käivitamine...")
    
    # Context manager ('with' plokk) tagab, et ühendus suletakse ka siis, kui skript katkeb
    with sqlite3.connect(AB_FAIL) as yhendus:
        loo_andmebaasi_skeem(yhendus)
        
        sessioon = seadista_paringute_sessioon()
        edu = laadi_andmed_apidest(yhendus, sessioon)
        
        if edu:
            tootle_ja_salvesta_andmed(yhendus)
            loo_aruandluse_vaated(yhendus)
            kestus = (datetime.now() - algusaeg).seconds
            logging.info(f"Andmetoru lõpetas edukalt {kestus} sekundiga.")
        else:
            logging.error("Andmetoru töö katkestati andmete eraldamise tõrke tõttu.")

if __name__ == "__main__":
    main()
