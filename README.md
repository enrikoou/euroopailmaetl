# Euroopa Pealinnade Ilmaandmete ETL Andmetoru

See on Pythonis kirjutatud lihtne, kuid toodangustandardeid järgiv ETL (Extract, Transform, Load) andmetoru. Skript kogub viimase 30 päeva ilmaandmed Euroopa pealinnade kohta ja salvestab need lokaalsesse SQLite andmebaasi.

## Tehnoloogiline Arhitektuur
- **Keel:** Python 3.x
- **Andmebaas:** SQLite3 (lokaalne, failipõhine)
- **Välised teegid:** `requests` (koos automaatse korduskatsete loogikaga)
- **Kasutatud API-d:**
  - **RestCountries API** (`restcountries.com`) - Euroopa riikide, pealinnade ja geograafiliste koordinaatide pärimiseks.
  - **Open-Meteo Archive API** (`archive-api.open-meteo.com`) - Ajalooliste ilmaandmete pärimiseks koordinaatide alusel.

## Andmetoru Sammud (Pipeline)

1. **Extract (Eraldamine):** Skript teeb päringu RestCountries API-sse, et saada nimekiri Euroopa riikidest ja nende pealinnade koordinaatidest. Nende koordinaatide alusel tehakse päringud Open-Meteo arhiivi. Toorandmed (JSON-kujul) salvestatakse otse vahetabelitesse (`stg_toored_geoandmed`, `stg_toores_ilm`), et säilitada algandmete jälgitavus.
2. **Transform (Puhastamine ja Transformeerimine):** JSON-struktuurid parsitakse lahti. Riigid, millel puuduvad standardsed pealinna koordinaadid, filtreeritakse välja. Puuduvad ilmaandmed (NULL väärtused) asendatakse vaikimisi väärtustega. Päikesepaiste kestus teisendatakse sekunditest tundideks (`sunshine_duration`).
3. **Load (Laadimine):** Puhastatud andmed laaditakse relatsioonilisse andmebaasi skeemi, kasutades idempotentseid (korduvkäivitatavaid) operatsioone (`INSERT OR REPLACE`, `INSERT OR IGNORE`). Loodud on dimensioonitabel `dim_riik` ja faktitabel `fct_paevane_ilm`.
4. **Vaated (Views):** Andmebaasi luuakse kolm analüütilist vaadet SQL-i abil:
   - `vw_keskmise_temp_edetabel`: Reastab pealinnad kõrgeima keskmise temperatuuri järgi.
   - `vw_suurim_sademete_hulk`: Reastab riigid kogusademete järgi.
   - `vw_30_paeva_kokkuvote`: Pakub statistilist ülevaadet (miinimumid, maksimumid, summad) iga riigi kohta.

## Kuidas käivitada

1. Klooni see repositoorium või laadi alla `etl_andmetoru.py` fail.
2. Veendu, et sul on installitud vajalik `requests` teek:
   ```bash
   pip install requests
