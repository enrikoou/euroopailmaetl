-- ==========================================
-- ETL ANDMETORU: ANDMEBAASI STRUKTUUR
-- ==========================================

-- 1. VAHETABELID toorandmete jaoks
CREATE TABLE stg_toored_geoandmed (
    laadimise_aeg TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    andmed JSON
);

CREATE TABLE stg_toores_ilm (
    riigi_kood TEXT PRIMARY KEY,
    laadimise_aeg TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    andmed JSON
);

-- 2. DIMENSIOONITABEL: Riikide info
CREATE TABLE dim_riik (
    riigi_kood TEXT PRIMARY KEY,
    riigi_nimi TEXT,
    pealinn TEXT,
    laiuskraad REAL,
    pikkuskraad REAL
);

-- 3. FAKTITABEL: Puhastatud igapäevane ilmaandmestik
CREATE TABLE fct_paevane_ilm (
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

-- 4. ANALÜÜTILINE VAADE 1: Keskmise temperatuuri edetabel
CREATE VIEW vw_keskmise_temp_edetabel AS
SELECT 
    d.pealinn, 
    d.riigi_nimi,
    ROUND(AVG((f.max_temp_c + f.min_temp_c) / 2.0), 2) AS keskmine_temp_c
FROM fct_paevane_ilm f
JOIN dim_riik d ON f.riigi_kood = d.riigi_kood
GROUP BY d.riigi_kood
ORDER BY keskmine_temp_c DESC;

-- 5. ANALÜÜTILINE VAADE 2: Riigid suurima sademete hulgaga
CREATE VIEW vw_suurim_sademete_hulk AS
SELECT 
    d.riigi_nimi, 
    ROUND(SUM(f.sademete_hulk_mm), 2) AS kogusademed_mm
FROM fct_paevane_ilm f
JOIN dim_riik d ON f.riigi_kood = d.riigi_kood
GROUP BY d.riigi_kood
ORDER BY kogusademed_mm DESC;

-- 6. ANALÜÜTILINE VAADE 3: 30 päeva kokkuvõte riikide kaupa
CREATE VIEW vw_30_paeva_kokkuvote AS
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
