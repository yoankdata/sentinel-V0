# sentinel-V0

![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![Prefect](https://img.shields.io/badge/Prefect-3.0-blue)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![License](https://img.shields.io/badge/license-MIT-green)

## Objectif du projet
Pipeline de données robuste pour l'ingestion et la transformation de données (ex: météo, agriculture).

## Architecture (Medallion)

```mermaid
graph LR
    API[OpenWeather API] -->|JSON| Prefect[Prefect Orchestrator]
    Prefect -->|Raw JSON| GCS_Bronze[(GCS Bronze)]
    GCS_Bronze -->|Load| BQ_Silver[(BigQuery Silver)]
    BQ_Silver -->|dbt Transform| BQ_Gold[(BigQuery Gold)]
    BQ_Gold -->|Consumption| Dashboard[Dashboards/Analysis]
```

### Détail des couches
- **Bronze**: Données brutes (JSON) stockées sur GCS.
- **Silver**: Données nettoyées et structurées (BigQuery/GCS).
- **Gold**: Données agrégées prêtes pour l'analyse.

## Data Lineage

```mermaid
graph TD
    subgraph Ingestion
        WeatherAPI[Weather API] --> |Python/Prefect| Bronze[GCS Bronze JSON]
    end
    subgraph Warehousing
        Bronze --> |Load| Silver[Silver Observations (BQ)]
        Silver --> |dbt| Gold[Gold Daily Status (BQ)]
    end
    subgraph Quality
        Gold --> Tests{dbt Tests}
        Tests --> |Pass/Fail| Alerting
    end
```

## Stack technique
- **Orchestration**: Prefect
- **Transformation**: dbt
- **Stockage**: Google Cloud Storage (GCS) & BigQuery
- **Langage**: Python 3.10+

## Installation & Configuration

### Pré-requis
- Python 3.10+
- Compte Google Cloud (Service Account avec droits GCS/BigQuery)
- Clé API OpenWeatherMap

### Setup
1. **Cloner le repo**
   ```bash
   git clone https://github.com/yoankdata/sentinel-V0.git
   cd sentinel-v0
   ```

2. **Installer les dépendances**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configuration (Environment)**
   Créer un fichier `.env` à la racine :
   ```ini
   OPENWEATHER_API_KEY=votre_cle_api
   GCS_BRONZE_BUCKET=votre_bucket_gcs
   PREFECT_API_URL=http://127.0.0.1:4200/api
   ```

4. **dbt Setup**
   Configurer `profiles.yml` pour BigQuery.

## Structure du projet
```
sentinel-v0/
├── flows/          # Scripts Prefect
├── dbt/            # Projets dbt (models, seeds, tests)
├── infra/          # Documentation infra
└── requirements.txt
```

## Scénario réel observé (Projet Sentinel)
**Cas testé : API key invalide sur ingestion météo**

Dans le cadre du développement de Sentinel, nous avons simulé une panne d'authentification API pour valider la robustesse du pipeline :

1. **Comportement Automatique** :
    - Le flow Prefect détecte l'erreur 401 (Unauthorized).
    - Il déclenche les **retries** configurés (3 tentatives espacées de 10s, 30s, 90s).
    - Aucune donnée partielle n'est envoyée vers le stockage.

2. **Résultat** :
    - **Arrêt propre** ("Fail Fast") du flow après échec des retries.
    - Notification d'erreur dans les logs.

3. **Impact Business & Technique** :
   - 🛡️ **Bronze (Sécurité)** : Aucun fichier corrompu ou vide n'a été créé (`sentinel-bronze` reste propre).
   - 💎 **Gold (Stabilité)** : Les tableaux de bord et analyses continuent de fonctionner sur les données historiques (J-1), sans risque de régression ou de "trous" dans les données du jour.

## Proof of Reliability

This pipeline is production-oriented and designed to fail safely.

- Daily scheduled ingestion via Prefect
- Automatic retries with exponential backoff
- Safe failure: downstream layers are never corrupted

**Guarantee**  
Daily ingestion with retries, backoff, and safe failure — no silent corruption.

### Evidence
- Prefect UI showing active daily schedule
- Successful completed flow run

![Prefect Deployment UI](assets/img/prefect_deployment_ui.png)



## Commandes de run

### Bronze
```bash
python flows/ingest_weather_to_bronze.py
```

### Silver
```bash
python flows/load_weather_bronze_to_silver.py
```

### Gold + tests
```bash
cd dbt
dbt run --select weather_daily_status
dbt test --select weather_daily_status
```

## Scénario KO (Preuve Sentinel)

**But** : démontrer que Sentinel bloque la donnée aberrante.

### Inject bad data (KO)
```sql
INSERT INTO `spherical-booth-474518-n6.sentinel_silver.weather_observations`
(observed_at_utc, fetched_at_utc, city, country, lat, lon, temp_c, feels_like_c, humidity_pct, pressure_hpa, wind_speed_ms, wind_deg, weather_main, weather_desc)
VALUES
(CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'Abidjan', 'CI', 5.36, -4.01, 999, 999, 150, 1013, 2.0, 90, 'Clear', 'bad_data_demo');
```

### Puis :
```bash
dbt test --select weather_daily_status
# Attendu: FAIL sur accepted_range temp_c & humidity_pct
```

### Rollback :
```sql
DELETE FROM `spherical-booth-474518-n6.sentinel_silver.weather_observations`
WHERE weather_desc = 'bad_data_demo';
```

### Galerie des Preuves
![Visualisation 1](assets/img/sentinel_proof_1.png)
![Visualisation 2](assets/img/sentinel_proof_2.png)
![Visualisation 3](assets/img/sentinel_proof_3.png)
![Visualisation 4](assets/img/sentinel_proof_4.png)
![Visualisation 5](assets/img/sentinel_proof_5.png)
![Visualisation 6](assets/img/sentinel_proof_6.png)


## Interprétation “Sentinel”

**KO** = tests dbt échouent → données suspectes détectées

**Protection** = pas de “corruption silencieuse” (tu vois l’échec)
