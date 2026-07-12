# Guide complet — Football Data Lake (EFREI 2025-2026)

## Résumé des choix techniques

| Zone | Technologie | Justification |
|------|-------------|---------------|
| **Raw** | MinIO (S3-compatible) | Object storage pour CSV + JSON, S3 API standard |
| **Staging** | PostgreSQL | Données structurées + features de forme par équipe |
| **Curated** | PostgreSQL | Prédictions XGBoost H/D/A stockées |
| **Orchestration** | Apache Airflow 2.9 | Scheduling daily API + DAG reproductible |
| **API Gateway** | FastAPI | /raw /staging /curated /health /stats |
| **ML** | XGBoost | Classification H/D/A (adapté de ton pipeline churn) |

---

## Sources de données

### Source fichier — football-data.co.uk (aucun compte requis)
- Premier League, 5 saisons 2019–2024 ≈ 1 900 matchs
- Colonnes clés : `Date, HomeTeam, AwayTeam, FTHG, FTAG, FTR, HS, AS, HST, AST`
- `FTR` = résultat (H=victoire domicile, D=nul, A=victoire extérieur)

URLs directes à utiliser dans le DAG :
```
2023-24 : https://www.football-data.co.uk/mmz4281/2324/E0.csv
2022-23 : https://www.football-data.co.uk/mmz4281/2223/E0.csv
2021-22 : https://www.football-data.co.uk/mmz4281/2122/E0.csv
2020-21 : https://www.football-data.co.uk/mmz4281/2021/E0.csv
2019-20 : https://www.football-data.co.uk/mmz4281/1920/E0.csv
```

### Source API — football-data.org (inscription gratuite)
1. Crée un compte sur https://www.football-data.org/client/register
2. Récupère ta clé API dans le dashboard
3. Tier gratuit : 10 req/min, accès Premier League + Champions League
4. Stocke la clé dans `.env` : `FOOTBALL_API_KEY=ta_clé_ici`

---

## Structure du projet

```
football-datalake/
├── docker-compose.yml
├── .env                        ← JAMAIS sur GitHub (dans .gitignore)
├── .env.example                ← Modèle sans valeurs sensibles
├── .gitignore
├── README.md
├── scripts/
│   └── init_db.sql             ← Création automatique des tables PostgreSQL
├── airflow/
│   ├── Dockerfile              ← Image Airflow custom avec toutes les libs ML
│   ├── requirements.txt
│   └── dags/
│       ├── dag_01_ingest_csv.py    ← Ingestion fichiers CSV → MinIO
│       ├── dag_02_ingest_api.py    ← Ingestion API → MinIO (scheduling quotidien)
│       ├── dag_03_staging.py       ← Transformation Raw → Staging (feature engineering)
│       └── dag_04_curated.py       ← ML XGBoost → Curated (prédictions)
└── api/
    ├── Dockerfile
    ├── requirements.txt
    ├── main.py
    └── routers/
        ├── raw.py
        ├── staging.py
        ├── curated.py
        ├── health.py
        └── stats.py
```

---

## Phase 1 — Setup Docker + Base de données

### `.env.example`
```
FOOTBALL_API_KEY=your_api_key_here
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
PG_USER=postgres
PG_PASSWORD=postgres
PG_DB=football_lake
AIRFLOW_UID=50000
```

### `.gitignore`
```
.env
__pycache__/
*.pyc
*.pkl
airflow/logs/
airflow/plugins/
.ipynb_checkpoints/
```

### `docker-compose.yml`
```yaml
version: '3.8'

# Variables communes aux trois conteneurs Airflow
x-airflow-common: &airflow-common
  build: ./airflow
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://postgres:postgres@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "False"
    AIRFLOW__WEBSERVER__SECRET_KEY: "football_datalake_secret"
    # Variables de notre data lake passées aux DAGs
    FOOTBALL_API_KEY: ${FOOTBALL_API_KEY}
    MINIO_ENDPOINT: minio:9000
    MINIO_ACCESS_KEY: minioadmin
    MINIO_SECRET_KEY: minioadmin
    PG_HOST: postgres
    PG_DB: football_lake
    PG_USER: postgres
    PG_PASSWORD: postgres
  volumes:
    - ./airflow/dags:/opt/airflow/dags
  depends_on:
    postgres:
      condition: service_healthy
    minio:
      condition: service_healthy

services:

  # ─── STOCKAGE OBJET (zone Raw) ────────────────────────────────────────────
  minio:
    image: minio/minio:latest
    container_name: minio
    ports:
      - "9000:9000"    # API S3
      - "9001:9001"    # Console web (http://localhost:9001)
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 10s
      retries: 3

  # ─── BASE DE DONNÉES (Staging + Curated) ──────────────────────────────────
  postgres:
    image: postgres:15
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: football_lake
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init_db.sql:/docker-entrypoint-initdb.d/init_db.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ─── AIRFLOW ───────────────────────────────────────────────────────────────
  airflow-init:
    <<: *airflow-common
    container_name: airflow-init
    command: >
      bash -c "
        airflow db migrate &&
        airflow users create
          --username admin
          --password admin
          --firstname Admin
          --lastname User
          --role Admin
          --email admin@football.com || true
      "
    restart: "no"

  airflow-webserver:
    <<: *airflow-common
    container_name: airflow-webserver
    command: webserver
    ports:
      - "8080:8080"    # Interface Airflow (http://localhost:8080)
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  # ─── API GATEWAY ──────────────────────────────────────────────────────────
  fastapi:
    build: ./api
    container_name: fastapi
    ports:
      - "8000:8000"    # API (http://localhost:8000/docs pour la doc Swagger)
    environment:
      MINIO_ENDPOINT: minio:9000
      MINIO_ACCESS_KEY: minioadmin
      MINIO_SECRET_KEY: minioadmin
      PG_HOST: postgres
      PG_DB: football_lake
      PG_USER: postgres
      PG_PASSWORD: postgres
    depends_on:
      postgres:
        condition: service_healthy
      minio:
        condition: service_healthy

volumes:
  postgres_data:
  minio_data:
```

### `scripts/init_db.sql`
```sql
-- Création de la base Airflow (les métadonnées Airflow sont séparées)
CREATE DATABASE airflow;

-- Les tables suivantes sont créées dans football_lake (POSTGRES_DB par défaut)

-- Journal des fichiers ingérés dans MinIO
CREATE TABLE IF NOT EXISTS raw_files_log (
    id          SERIAL PRIMARY KEY,
    filename    VARCHAR(255)  NOT NULL,
    source      VARCHAR(50)   NOT NULL,   -- 'csv' ou 'api'
    bucket      VARCHAR(100)  NOT NULL,
    object_key  VARCHAR(500)  NOT NULL,
    uploaded_at TIMESTAMP     DEFAULT NOW()
);

-- Zone Staging : matchs nettoyés avec features de forme
CREATE TABLE IF NOT EXISTS staging_matches (
    id                      SERIAL PRIMARY KEY,
    date                    DATE,
    home_team               VARCHAR(100),
    away_team               VARCHAR(100),
    home_goals              INTEGER,
    away_goals              INTEGER,
    result                  CHAR(1),       -- H / D / A
    home_shots              INTEGER,
    away_shots              INTEGER,
    home_shots_on_target    INTEGER,
    away_shots_on_target    INTEGER,
    -- Features de forme calculées dans le DAG staging
    home_form               FLOAT,         -- pts sur 5 derniers matchs (0-15)
    away_form               FLOAT,
    home_avg_goals_scored   FLOAT,
    home_avg_goals_conceded FLOAT,
    away_avg_goals_scored   FLOAT,
    away_avg_goals_conceded FLOAT,
    season                  VARCHAR(10),
    source                  VARCHAR(50),
    processed_at            TIMESTAMP DEFAULT NOW()
);

-- Zone Staging : fixtures ingérées depuis l'API football-data.org
CREATE TABLE IF NOT EXISTS staging_api_fixtures (
    id          SERIAL PRIMARY KEY,
    fixture_id  INTEGER UNIQUE,
    date        TIMESTAMP,
    home_team   VARCHAR(100),
    away_team   VARCHAR(100),
    home_score  INTEGER,
    away_score  INTEGER,
    status      VARCHAR(50),
    competition VARCHAR(100),
    fetched_at  TIMESTAMP DEFAULT NOW()
);

-- Zone Curated : prédictions XGBoost
CREATE TABLE IF NOT EXISTS curated_predictions (
    id               SERIAL PRIMARY KEY,
    date             DATE,
    home_team        VARCHAR(100),
    away_team        VARCHAR(100),
    actual_result    CHAR(1),       -- résultat réel (si connu)
    predicted_result CHAR(1),       -- H / D / A prédit
    prob_home        FLOAT,         -- P(victoire domicile)
    prob_draw        FLOAT,         -- P(nul)
    prob_away        FLOAT,         -- P(victoire extérieur)
    model_version    VARCHAR(50),
    predicted_at     TIMESTAMP DEFAULT NOW()
);
```

### `airflow/Dockerfile`
```dockerfile
FROM apache/airflow:2.9.2

USER root
# libgomp1 requis par XGBoost sur Debian
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

### `airflow/requirements.txt`
```
boto3==1.34.0
requests==2.31.0
pandas==2.2.0
numpy==1.26.4
scikit-learn==1.4.2
xgboost==2.0.3
psycopg2-binary==2.9.9
joblib==1.3.2
```

---

## Phase 2 — Ingestion Raw (DAG 1 + DAG 2)

### `airflow/dags/dag_01_ingest_csv.py`
```python
"""
DAG 01 — Ingestion historique des matchs Premier League (CSV → MinIO Raw)
Source   : football-data.co.uk (pas d'authentification requise)
Schedule : @once (chargement initial des 5 saisons historiques)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import requests
import logging
import os

# ── Configuration ──────────────────────────────────────────────────────────────

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

BUCKET_RAW = 'raw-football'

# 5 saisons Premier League (E0 = England Division 1)
SEASONS = {
    '2023-24': 'https://www.football-data.co.uk/mmz4281/2324/E0.csv',
    '2022-23': 'https://www.football-data.co.uk/mmz4281/2223/E0.csv',
    '2021-22': 'https://www.football-data.co.uk/mmz4281/2122/E0.csv',
    '2020-21': 'https://www.football-data.co.uk/mmz4281/2021/E0.csv',
    '2019-20': 'https://www.football-data.co.uk/mmz4281/1920/E0.csv',
}

# ── Fonctions utilitaires ──────────────────────────────────────────────────────

def get_minio_client():
    """Retourne un client boto3 configuré pour MinIO."""
    return boto3.client(
        's3',
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    )

# ── Tâches ────────────────────────────────────────────────────────────────────

def create_raw_bucket(**kwargs):
    """Crée le bucket MinIO raw-football s'il n'existe pas encore."""
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=BUCKET_RAW)
        logging.info(f"Bucket '{BUCKET_RAW}' existe déjà.")
    except Exception:
        client.create_bucket(Bucket=BUCKET_RAW)
        logging.info(f"Bucket '{BUCKET_RAW}' créé avec succès.")


def download_and_upload_csv(season: str, url: str, **kwargs):
    """
    Télécharge le CSV d'une saison et l'uploade dans MinIO.

    Args:
        season: Identifiant de la saison (ex: '2023-24')
        url   : URL directe du CSV sur football-data.co.uk
    """
    try:
        logging.info(f"Téléchargement saison {season} depuis {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

    except requests.Timeout:
        raise ValueError(f"Timeout lors du téléchargement de la saison {season}")
    except requests.HTTPError as e:
        raise ValueError(f"Erreur HTTP {response.status_code} pour {season}: {e}")
    except requests.ConnectionError as e:
        raise ValueError(f"Impossible de joindre football-data.co.uk: {e}")

    try:
        object_key = f"premier_league/{season}/matches.csv"
        client = get_minio_client()
        client.put_object(
            Bucket=BUCKET_RAW,
            Key=object_key,
            Body=response.content,
            ContentType='text/csv',
        )
        nb_lines = len(response.text.strip().split('\n')) - 1  # -1 pour l'entête
        logging.info(f"Saison {season} uploadée : {nb_lines} matchs → {BUCKET_RAW}/{object_key}")

    except Exception as e:
        raise ValueError(f"Erreur upload MinIO pour la saison {season}: {e}")


# ── Définition du DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id='dag_01_ingest_csv_football',
    default_args=default_args,
    description='Ingestion historique Premier League (CSV) → MinIO Raw',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['raw', 'football', 'csv'],
) as dag:

    task_create_bucket = PythonOperator(
        task_id='create_raw_bucket',
        python_callable=create_raw_bucket,
    )

    upload_tasks = []
    for season, url in SEASONS.items():
        task = PythonOperator(
            task_id=f'upload_{season.replace("-", "_")}',
            python_callable=download_and_upload_csv,
            op_kwargs={'season': season, 'url': url},
        )
        upload_tasks.append(task)

    # Le bucket est créé avant les uploads parallèles
    task_create_bucket >> upload_tasks
```

### `airflow/dags/dag_02_ingest_api.py`
```python
"""
DAG 02 — Ingestion quotidienne des fixtures/standings PL via football-data.org
Source   : football-data.org (clé API gratuite, 10 req/min)
Schedule : @daily — refresh des résultats et classement chaque jour
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import requests
import json
import psycopg2
import psycopg2.extras
import logging
import os
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

BUCKET_RAW  = 'raw-football'
API_BASE    = 'https://api.football-data.org/v4'
COMPETITION = 'PL'   # Premier League

# ── Fonctions utilitaires ──────────────────────────────────────────────────────

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    )

def get_pg_connection():
    return psycopg2.connect(
        host=os.environ['PG_HOST'],
        database=os.environ['PG_DB'],
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD'],
    )

def api_get(endpoint: str) -> dict:
    """
    Effectue un GET sur l'API football-data.org avec gestion des erreurs.
    Respecte automatiquement le rate limit (10 req/min).
    """
    api_key = os.environ.get('FOOTBALL_API_KEY')
    if not api_key:
        raise ValueError("Variable d'environnement FOOTBALL_API_KEY non définie")

    headers = {'X-Auth-Token': api_key}
    url = f"{API_BASE}/{endpoint}"

    try:
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 429:
            # Rate limit atteint — Airflow retentera via retry
            raise ValueError("Rate limit atteint (10 req/min). La tâche sera retentée automatiquement.")

        response.raise_for_status()
        return response.json()

    except requests.ConnectionError:
        raise ValueError("Impossible de joindre api.football-data.org — vérifier la connexion réseau")
    except requests.Timeout:
        raise ValueError(f"Timeout lors de l'appel à {url}")

# ── Tâches ────────────────────────────────────────────────────────────────────

def fetch_and_store_matches(**kwargs):
    """
    Récupère tous les matchs de la saison en cours (PL) et les stocke
    en JSON dans MinIO (zone Raw) + les insère en PostgreSQL Staging.
    """
    data = api_get(f"competitions/{COMPETITION}/matches?season=2024")
    matches = data.get('matches', [])

    if not matches:
        logging.warning("Aucun match retourné par l'API pour la saison 2024")
        return

    # ── 1. Stocker le JSON brut dans MinIO ──
    timestamp  = datetime.now().strftime('%Y%m%d_%H%M%S')
    object_key = f"api/matches/PL_matches_{timestamp}.json"

    client = get_minio_client()
    client.put_object(
        Bucket=BUCKET_RAW,
        Key=object_key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
        ContentType='application/json',
    )
    logging.info(f"{len(matches)} matchs stockés dans {BUCKET_RAW}/{object_key}")

    # ── 2. Insérer les fixtures dans PostgreSQL Staging ──
    conn = get_pg_connection()
    cur  = conn.cursor()
    try:
        insert_sql = """
            INSERT INTO staging_api_fixtures
                (fixture_id, date, home_team, away_team, home_score, away_score, status, competition)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fixture_id) DO UPDATE SET
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                status     = EXCLUDED.status,
                fetched_at = NOW()
        """
        records = []
        for m in matches:
            score = m.get('score', {}).get('fullTime', {})
            records.append((
                m['id'],
                m['utcDate'],
                m['homeTeam']['name'],
                m['awayTeam']['name'],
                score.get('home'),
                score.get('away'),
                m['status'],
                'Premier League',
            ))

        cur.executemany(insert_sql, records)
        conn.commit()
        logging.info(f"{len(records)} fixtures upsertées dans staging_api_fixtures")

    except Exception as e:
        conn.rollback()
        raise ValueError(f"Erreur insertion fixtures API: {e}")
    finally:
        cur.close()
        conn.close()


def fetch_and_store_standings(**kwargs):
    """
    Récupère le classement Premier League et le stocke dans MinIO.
    Attend 6 secondes pour respecter le rate limit (10 req/min).
    """
    time.sleep(6)  # Respecter le rate limit de l'API gratuite

    data = api_get(f"competitions/{COMPETITION}/standings?season=2024")

    timestamp  = datetime.now().strftime('%Y%m%d_%H%M%S')
    object_key = f"api/standings/PL_standings_{timestamp}.json"

    client = get_minio_client()
    client.put_object(
        Bucket=BUCKET_RAW,
        Key=object_key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
        ContentType='application/json',
    )
    logging.info(f"Classement PL stocké dans {BUCKET_RAW}/{object_key}")


# ── Définition du DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id='dag_02_ingest_api_football',
    default_args=default_args,
    description='Ingestion quotidienne fixtures/standings PL via football-data.org',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['raw', 'football', 'api'],
) as dag:

    task_matches = PythonOperator(
        task_id='fetch_pl_matches',
        python_callable=fetch_and_store_matches,
    )

    task_standings = PythonOperator(
        task_id='fetch_pl_standings',
        python_callable=fetch_and_store_standings,
    )

    # Séquence pour respecter le rate limit entre les deux appels API
    task_matches >> task_standings
```

---

## Phase 3 — Staging (transformation + feature engineering)

### `airflow/dags/dag_03_staging.py`
```python
"""
DAG 03 — Transformation Raw → Staging
Lit les CSV depuis MinIO, calcule les features de forme par équipe (rolling window),
puis insère dans PostgreSQL staging_matches.
Schedule : @daily (après l'ingestion Raw)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
import io
import logging
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

BUCKET_RAW = 'raw-football'

# ── Utilitaires ───────────────────────────────────────────────────────────────

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    )

def get_pg_connection():
    return psycopg2.connect(
        host=os.environ['PG_HOST'],
        database=os.environ['PG_DB'],
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD'],
    )

# ── Feature Engineering ───────────────────────────────────────────────────────

def compute_team_form_features(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """
    Calcule les features de forme de chaque équipe de façon vectorisée.

    Pour chaque match, les features sont calculées sur les N matchs PRÉCÉDENTS
    (le .shift(1) garantit qu'il n'y a aucun data leakage vers le futur).

    Features calculées :
        - form       : total de points sur les N derniers matchs (0 à 3N)
        - avg_gf     : moyenne de buts marqués sur les N derniers matchs
        - avg_ga     : moyenne de buts concédés sur les N derniers matchs

    Args:
        df : DataFrame avec colonnes Date, HomeTeam, AwayTeam, FTHG, FTAG, FTR
        n  : Taille de la fenêtre glissante (défaut : 5 matchs)

    Returns:
        DataFrame enrichi avec home_form, away_form, home/away_avg_gf/ga
    """
    df = df.copy().sort_values('date').reset_index(drop=True)

    # Points pour chaque camp selon le résultat
    df['home_match_pts'] = df['FTR'].map({'H': 3, 'D': 1, 'A': 0}).astype(float)
    df['away_match_pts'] = df['FTR'].map({'A': 3, 'D': 1, 'H': 0}).astype(float)

    # ── Vue unifiée "équipe / match" ──
    # On construit un tableau avec une ligne par équipe par match
    home_view = df[['date', 'HomeTeam', 'home_match_pts', 'FTHG', 'FTAG']].copy()
    home_view.columns = ['date', 'team', 'pts', 'gf', 'ga']

    away_view = df[['date', 'AwayTeam', 'away_match_pts', 'FTAG', 'FTHG']].copy()
    away_view.columns = ['date', 'team', 'pts', 'gf', 'ga']

    hist = pd.concat([home_view, away_view]).sort_values(['team', 'date']).reset_index(drop=True)

    # ── Rolling stats vectorisées par équipe ──
    hist['form']   = hist.groupby('team')['pts'].transform(
        lambda x: x.rolling(n, min_periods=1).sum().shift(1)
    ).fillna(0)
    hist['avg_gf'] = hist.groupby('team')['gf'].transform(
        lambda x: x.rolling(n, min_periods=1).mean().shift(1)
    ).fillna(0)
    hist['avg_ga'] = hist.groupby('team')['ga'].transform(
        lambda x: x.rolling(n, min_periods=1).mean().shift(1)
    ).fillna(0)

    # ── Jointure des stats sur les matchs ──
    home_stats = hist[['date', 'team', 'form', 'avg_gf', 'avg_ga']].copy()
    home_stats.columns = ['date', 'HomeTeam', 'home_form', 'home_avg_gf', 'home_avg_ga']

    away_stats = hist[['date', 'team', 'form', 'avg_gf', 'avg_ga']].copy()
    away_stats.columns = ['date', 'AwayTeam', 'away_form', 'away_avg_gf', 'away_avg_ga']

    df = df.merge(home_stats, on=['date', 'HomeTeam'], how='left')
    df = df.merge(away_stats, on=['date', 'AwayTeam'], how='left')

    return df

# ── Tâches ────────────────────────────────────────────────────────────────────

def transform_csv_to_staging(**kwargs):
    """
    Pipeline complet Raw → Staging :
      1. Lire tous les CSV depuis MinIO (bucket raw-football)
      2. Nettoyer et normaliser les données
      3. Calculer les features de forme (rolling window)
      4. Insérer dans PostgreSQL staging_matches
    """
    client = get_minio_client()

    # ── 1. Lister et charger les CSV ──
    response = client.list_objects_v2(Bucket=BUCKET_RAW, Prefix='premier_league/')
    objects  = response.get('Contents', [])

    if not objects:
        raise ValueError(
            "Aucun fichier CSV trouvé dans MinIO (bucket raw-football/premier_league/). "
            "Lancer d'abord dag_01_ingest_csv_football."
        )

    all_dfs = []
    for obj in objects:
        key    = obj['Key']
        season = key.split('/')[1]   # ex: '2023-24'
        try:
            s3_object = client.get_object(Bucket=BUCKET_RAW, Key=key)
            content   = s3_object['Body'].read()
            df        = pd.read_csv(io.BytesIO(content), on_bad_lines='skip')
            df['season'] = season
            all_dfs.append(df)
            logging.info(f"Chargé {len(df)} lignes depuis {key}")
        except Exception as e:
            logging.error(f"Impossible de lire {key} : {e} — fichier ignoré")
            continue

    if not all_dfs:
        raise ValueError("Impossible de charger les données CSV depuis MinIO")

    df_all = pd.concat(all_dfs, ignore_index=True)
    logging.info(f"Total brut : {len(df_all)} lignes sur {len(all_dfs)} fichiers")

    # ── 2. Nettoyage ──
    required = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'season']
    # Garder uniquement les colonnes disponibles dans required
    available = [c for c in required if c in df_all.columns]
    missing   = set(required) - set(available)
    if missing:
        raise ValueError(f"Colonnes manquantes dans les CSV : {missing}")

    df_all = df_all[available].dropna(subset=['Date', 'HomeTeam', 'AwayTeam', 'FTR'])

    # Parsing de la date (football-data.co.uk utilise DD/MM/YYYY ou DD/MM/YY)
    df_all['date'] = pd.to_datetime(df_all['Date'], dayfirst=True, errors='coerce')
    df_all = df_all.dropna(subset=['date'])

    # Conversion des goals en entiers
    for col in ['FTHG', 'FTAG']:
        df_all[col] = pd.to_numeric(df_all[col], errors='coerce').fillna(0).astype(int)

    # Garder uniquement les résultats valides (H / D / A)
    df_all = df_all[df_all['FTR'].isin(['H', 'D', 'A'])]

    logging.info(f"Après nettoyage : {len(df_all)} matchs valides")

    # ── 3. Feature engineering ──
    df_enriched = compute_team_form_features(df_all, n=5)

    # ── 4. Insertion en PostgreSQL ──
    conn = get_pg_connection()
    cur  = conn.cursor()

    try:
        cur.execute("TRUNCATE TABLE staging_matches")  # Reset propre avant rechargement

        records = []
        for _, row in df_enriched.iterrows():
            records.append({
                'date':                    row['date'].date(),
                'home_team':               str(row['HomeTeam']),
                'away_team':               str(row['AwayTeam']),
                'home_goals':              int(row['FTHG']),
                'away_goals':              int(row['FTAG']),
                'result':                  str(row['FTR']),
                'home_form':               float(row.get('home_form', 0)),
                'away_form':               float(row.get('away_form', 0)),
                'home_avg_goals_scored':   float(row.get('home_avg_gf', 0)),
                'home_avg_goals_conceded': float(row.get('home_avg_ga', 0)),
                'away_avg_goals_scored':   float(row.get('away_avg_gf', 0)),
                'away_avg_goals_conceded': float(row.get('away_avg_ga', 0)),
                'season':                  str(row['season']),
                'source':                  'football-data.co.uk',
            })

        insert_sql = """
            INSERT INTO staging_matches (
                date, home_team, away_team, home_goals, away_goals, result,
                home_form, away_form,
                home_avg_goals_scored, home_avg_goals_conceded,
                away_avg_goals_scored, away_avg_goals_conceded,
                season, source
            ) VALUES (
                %(date)s, %(home_team)s, %(away_team)s, %(home_goals)s, %(away_goals)s, %(result)s,
                %(home_form)s, %(away_form)s,
                %(home_avg_goals_scored)s, %(home_avg_goals_conceded)s,
                %(away_avg_goals_scored)s, %(away_avg_goals_conceded)s,
                %(season)s, %(source)s
            )
        """
        psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
        logging.info(f"staging_matches : {len(records)} matchs insérés avec succès")

    except Exception as e:
        conn.rollback()
        raise ValueError(f"Erreur lors de l'insertion dans staging_matches : {e}")
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id='dag_03_staging_football',
    default_args=default_args,
    description='Transformation Raw → Staging (nettoyage + feature engineering forme)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['staging', 'football', 'transformation'],
) as dag:

    task_transform = PythonOperator(
        task_id='transform_csv_to_staging',
        python_callable=transform_csv_to_staging,
    )
```

---

## Phase 4 — Curated + Modèle ML (XGBoost)

### `airflow/dags/dag_04_curated.py`
```python
"""
DAG 04 — Entraînement XGBoost + génération des prédictions (Staging → Curated)
Modèle : XGBoost multi-class (H=0, D=1, A=2)
Features : home_form, away_form, avg_goals_scored/conceded de chaque équipe
Schedule : @weekly (réentraînement hebdomadaire)
XCom    : model_key et model_version sont passés entre les deux tâches
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, classification_report
import joblib
import io
import logging
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BUCKET_RAW = 'raw-football'

# Features utilisées par le modèle (doivent correspondre aux colonnes de staging_matches)
FEATURE_COLS = [
    'home_form',
    'away_form',
    'home_avg_goals_scored',
    'home_avg_goals_conceded',
    'away_avg_goals_scored',
    'away_avg_goals_conceded',
]

# ── Utilitaires ───────────────────────────────────────────────────────────────

def get_pg_connection():
    return psycopg2.connect(
        host=os.environ['PG_HOST'],
        database=os.environ['PG_DB'],
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD'],
    )

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    )

# ── Tâches ────────────────────────────────────────────────────────────────────

def train_xgboost(**kwargs):
    """
    Entraîne un classificateur XGBoost sur les données de staging_matches.

    Pipeline :
      1. Charge les données depuis PostgreSQL Staging
      2. Encode la cible (H→0, D→1, A→2)
      3. Split train/test (80/20, stratifié)
      4. Entraîne XGBoost
      5. Évalue sur le test set et log les métriques
      6. Sauvegarde le modèle (+ encoder + features) dans MinIO
      7. Passe le chemin du modèle à la tâche suivante via XCom
    """
    # ── 1. Chargement des données ──
    conn = get_pg_connection()
    try:
        df = pd.read_sql(
            "SELECT * FROM staging_matches WHERE result IS NOT NULL",
            conn
        )
    except Exception as e:
        raise ValueError(f"Impossible de lire staging_matches : {e}")
    finally:
        conn.close()

    if df.empty:
        raise ValueError("staging_matches est vide — exécuter dag_03_staging d'abord")
    if len(df) < 200:
        raise ValueError(
            f"Pas assez de données : {len(df)} matchs (minimum 200 requis pour l'entraînement)"
        )

    logging.info(f"Données chargées : {len(df)} matchs, {df['season'].nunique()} saisons")

    # ── 2. Encodage de la cible ──
    # H=0, D=1, A=2 (ordre fixé pour garantir la cohérence avec predict_proba)
    le = LabelEncoder()
    le.fit(['H', 'D', 'A'])
    df['target'] = le.transform(df['result'])

    X = df[FEATURE_COLS].fillna(0)
    y = df['target']

    # ── 3. Split train/test ──
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # ── 4. Entraînement XGBoost ──
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective='multi:softprob',
        num_class=3,
        eval_metric='mlogloss',
        random_state=42,
        n_jobs=-1,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # ── 5. Évaluation ──
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    report = classification_report(
        y_test, y_pred,
        target_names=['Victoire dom. (H)', 'Nul (D)', 'Victoire ext. (A)'],
    )
    logging.info(f"\nAccuracy : {accuracy:.3f}\n\n{report}")

    # ── 6. Sauvegarde dans MinIO ──
    model_payload = {
        'model':         model,
        'label_encoder': le,
        'features':      FEATURE_COLS,
        'accuracy':      float(accuracy),
    }
    model_bytes = io.BytesIO()
    joblib.dump(model_payload, model_bytes)
    model_bytes.seek(0)

    version    = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_key  = f"models/xgb_match_predictor_{version}.pkl"
    client     = get_minio_client()

    client.put_object(
        Bucket=BUCKET_RAW,
        Key=model_key,
        Body=model_bytes.getvalue(),
        ContentType='application/octet-stream',
    )
    logging.info(f"Modèle sauvegardé dans MinIO : {model_key} (accuracy={accuracy:.3f})")

    # ── 7. Passage via XCom ──
    kwargs['ti'].xcom_push(key='model_key',     value=model_key)
    kwargs['ti'].xcom_push(key='model_version', value=version)
    kwargs['ti'].xcom_push(key='accuracy',      value=float(accuracy))


def generate_predictions(**kwargs):
    """
    Charge le modèle depuis MinIO et génère les prédictions pour tous les matchs
    du staging → insère dans curated_predictions.

    Utilise XCom pour récupérer le chemin du modèle entraîné par la tâche précédente.
    """
    ti          = kwargs['ti']
    model_key   = ti.xcom_pull(task_ids='train_xgboost', key='model_key')
    model_ver   = ti.xcom_pull(task_ids='train_xgboost', key='model_version')

    if not model_key:
        raise ValueError("model_key absent dans XCom — la tâche train_xgboost a-t-elle réussi ?")

    # ── 1. Chargement du modèle depuis MinIO ──
    client = get_minio_client()
    try:
        obj     = client.get_object(Bucket=BUCKET_RAW, Key=model_key)
        payload = joblib.load(io.BytesIO(obj['Body'].read()))
    except Exception as e:
        raise ValueError(f"Impossible de charger le modèle depuis MinIO ({model_key}): {e}")

    model    = payload['model']
    le       = payload['label_encoder']
    features = payload['features']

    # ── 2. Chargement du staging ──
    conn = get_pg_connection()
    df   = pd.read_sql("SELECT * FROM staging_matches WHERE result IS NOT NULL", conn)
    conn.close()

    if df.empty:
        raise ValueError("staging_matches vide — impossible de générer des prédictions")

    X           = df[features].fillna(0)
    proba       = model.predict_proba(X)       # shape (n, 3) : P(H), P(D), P(A)
    pred_labels = le.inverse_transform(model.predict(X))

    # ── 3. Construction des records à insérer ──
    records = []
    for i, (_, row) in enumerate(df.iterrows()):
        records.append({
            'date':             row['date'],
            'home_team':        row['home_team'],
            'away_team':        row['away_team'],
            'actual_result':    row['result'],
            'predicted_result': str(pred_labels[i]),
            'prob_home':        float(proba[i][0]),
            'prob_draw':        float(proba[i][1]),
            'prob_away':        float(proba[i][2]),
            'model_version':    model_ver,
        })

    # ── 4. Insertion en PostgreSQL Curated ──
    conn = get_pg_connection()
    cur  = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE curated_predictions")

        psycopg2.extras.execute_batch(cur, """
            INSERT INTO curated_predictions
                (date, home_team, away_team, actual_result, predicted_result,
                 prob_home, prob_draw, prob_away, model_version)
            VALUES
                (%(date)s, %(home_team)s, %(away_team)s, %(actual_result)s, %(predicted_result)s,
                 %(prob_home)s, %(prob_draw)s, %(prob_away)s, %(model_version)s)
        """, records, page_size=500)

        conn.commit()
        logging.info(f"curated_predictions : {len(records)} prédictions insérées (version {model_ver})")

    except Exception as e:
        conn.rollback()
        raise ValueError(f"Erreur insertion curated_predictions : {e}")
    finally:
        cur.close()
        conn.close()


# ── Définition du DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id='dag_04_curated_football',
    default_args=default_args,
    description='Entraînement XGBoost + prédictions de résultats → Curated',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['curated', 'football', 'ml', 'xgboost'],
) as dag:

    task_train   = PythonOperator(task_id='train_xgboost',        python_callable=train_xgboost)
    task_predict = PythonOperator(task_id='generate_predictions',  python_callable=generate_predictions)

    task_train >> task_predict
```

---

## Phase 5 — API Gateway (FastAPI)

### `api/Dockerfile`
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### `api/requirements.txt`
```
fastapi==0.110.0
uvicorn==0.29.0
boto3==1.34.0
psycopg2-binary==2.9.9
```

### `api/main.py`
```python
"""
API Gateway — Football Data Lake
Documentation Swagger automatique : http://localhost:8000/docs
"""
from fastapi import FastAPI
from routers import raw, staging, curated, health, stats

app = FastAPI(
    title="Football Data Lake API",
    description="API Gateway pour le data lake Football Premier League (EFREI 2025-2026)",
    version="1.0.0",
)

app.include_router(raw.router,     prefix="/raw",     tags=["Raw Zone"])
app.include_router(staging.router, prefix="/staging", tags=["Staging Zone"])
app.include_router(curated.router, prefix="/curated", tags=["Curated Zone"])
app.include_router(health.router,  prefix="/health",  tags=["Health"])
app.include_router(stats.router,   prefix="/stats",   tags=["Stats"])

@app.get("/")
def root():
    return {
        "service": "Football Data Lake API",
        "docs":    "http://localhost:8000/docs",
        "zones":   ["/raw", "/staging", "/curated", "/health", "/stats"],
    }
```

### `api/routers/raw.py`
```python
"""
Endpoint /raw — Accès aux fichiers bruts dans MinIO (S3).
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import boto3
import json
import os

router = APIRouter()

BUCKET_RAW = 'raw-football'

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    )

@router.get("/", summary="Liste des fichiers bruts dans MinIO")
def list_raw_files(
    prefix: Optional[str] = Query(None, description="Filtre par préfixe (ex: 'api/matches/')"),
    limit:  int           = Query(50, le=500, description="Nombre max de résultats"),
):
    """Retourne la liste des fichiers stockés dans la zone Raw (MinIO)."""
    client = get_minio_client()

    try:
        kwargs = {'Bucket': BUCKET_RAW, 'MaxKeys': limit}
        if prefix:
            kwargs['Prefix'] = prefix

        response = client.list_objects_v2(**kwargs)
        files    = response.get('Contents', [])

        return {
            "zone":    "raw",
            "bucket":  BUCKET_RAW,
            "count":   len(files),
            "files": [
                {
                    "key":           f['Key'],
                    "size_kb":       round(f['Size'] / 1024, 2),
                    "last_modified": f['LastModified'].isoformat(),
                }
                for f in files
            ],
        }

    except client.exceptions.NoSuchBucket:
        raise HTTPException(
            status_code=404,
            detail=f"Bucket '{BUCKET_RAW}' introuvable — le pipeline d'ingestion a-t-il été exécuté ?",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur accès MinIO : {str(e)}")


@router.get("/file", summary="Contenu d'un fichier JSON brut")
def get_raw_file(key: str = Query(..., description="Chemin du fichier dans MinIO (ex: api/matches/PL_matches_xxx.json)")):
    """Retourne le contenu d'un fichier JSON brut depuis MinIO. CSV exclus (trop volumineux)."""
    if not key.endswith('.json'):
        raise HTTPException(
            status_code=400,
            detail="Seuls les fichiers .json sont accessibles via cet endpoint. Pour les CSV, utilisez /stats.",
        )

    client = get_minio_client()
    try:
        obj     = client.get_object(Bucket=BUCKET_RAW, Key=key)
        content = json.loads(obj['Body'].read())
        return {"key": key, "content": content}

    except client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"Fichier '{key}' introuvable dans MinIO")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lecture fichier : {str(e)}")
```

### `api/routers/staging.py`
```python
"""
Endpoint /staging — Accès aux matchs nettoyés avec features de forme.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import psycopg2
import psycopg2.extras
import os

router = APIRouter()

def get_pg_connection():
    return psycopg2.connect(
        host=os.environ['PG_HOST'], database=os.environ['PG_DB'],
        user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'],
    )

@router.get("/", summary="Matchs transformés avec features de forme")
def get_staging_matches(
    team:   Optional[str] = Query(None, description="Filtrer par équipe (recherche partielle)"),
    season: Optional[str] = Query(None, description="Filtrer par saison (ex: '2023-24')"),
    limit:  int           = Query(100, le=1000),
    offset: int           = Query(0, ge=0),
):
    """Accès aux données de la zone Staging : matchs nettoyés avec features de forme XGBoost."""
    conn = get_pg_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        where_clauses, params = [], []

        if team:
            where_clauses.append("(home_team ILIKE %s OR away_team ILIKE %s)")
            params.extend([f"%{team}%", f"%{team}%"])
        if season:
            where_clauses.append("season = %s")
            params.append(season)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        cur.execute(f"""
            SELECT * FROM staging_matches {where_sql}
            ORDER BY date DESC LIMIT %s OFFSET %s
        """, params + [limit, offset])

        rows = cur.fetchall()

        return {
            "zone":    "staging",
            "count":   len(rows),
            "filters": {"team": team, "season": season},
            "matches": [dict(row) for row in rows],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur accès staging : {str(e)}")
    finally:
        cur.close()
        conn.close()
```

### `api/routers/curated.py`
```python
"""
Endpoint /curated — Prédictions XGBoost de résultats de matchs.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import psycopg2
import psycopg2.extras
import os

router = APIRouter()

def get_pg_connection():
    return psycopg2.connect(
        host=os.environ['PG_HOST'], database=os.environ['PG_DB'],
        user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'],
    )

@router.get("/", summary="Prédictions XGBoost de résultats")
def get_predictions(
    home_team: Optional[str] = Query(None, description="Filtrer par équipe à domicile"),
    away_team: Optional[str] = Query(None, description="Filtrer par équipe à l'extérieur"),
    limit:     int           = Query(100, le=1000),
    offset:    int           = Query(0, ge=0),
):
    """
    Accès aux données de la zone Curated : prédictions XGBoost (H/D/A)
    avec probabilités et précision globale du modèle.
    """
    conn = get_pg_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        where_clauses, params = [], []
        if home_team:
            where_clauses.append("home_team ILIKE %s")
            params.append(f"%{home_team}%")
        if away_team:
            where_clauses.append("away_team ILIKE %s")
            params.append(f"%{away_team}%")

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        cur.execute(f"""
            SELECT * FROM curated_predictions {where_sql}
            ORDER BY date DESC LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        # Accuracy globale du modèle
        cur.execute("""
            SELECT
                COUNT(*)  AS total,
                SUM(CASE WHEN actual_result = predicted_result THEN 1 ELSE 0 END) AS correct
            FROM curated_predictions
            WHERE actual_result IS NOT NULL
        """)
        stats   = cur.fetchone()
        accuracy = round(stats['correct'] / stats['total'], 4) if stats['total'] > 0 else None

        return {
            "zone":            "curated",
            "model":           "XGBoost — match outcome predictor (H/D/A)",
            "global_accuracy": accuracy,
            "count":           len(rows),
            "predictions":     [dict(row) for row in rows],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur accès curated : {str(e)}")
    finally:
        cur.close()
        conn.close()
```

### `api/routers/health.py`
```python
"""
Endpoint /health — Vérification de l'état de santé de tous les services.
"""
from fastapi import APIRouter
import psycopg2
import boto3
import os

router = APIRouter()

@router.get("/", summary="État de santé des services")
def health_check():
    """Vérifie la connectivité à MinIO (zone Raw) et PostgreSQL (Staging + Curated)."""
    status = {}

    # ── PostgreSQL ──
    try:
        conn = psycopg2.connect(
            host=os.environ['PG_HOST'], database=os.environ['PG_DB'],
            user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'],
            connect_timeout=5,
        )
        conn.close()
        status['postgresql'] = {"status": "healthy", "database": os.environ['PG_DB']}
    except Exception as e:
        status['postgresql'] = {"status": "unhealthy", "error": str(e)}

    # ── MinIO (S3) ──
    try:
        client = boto3.client(
            's3',
            endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
            aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
            aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
        )
        client.list_buckets()
        status['minio'] = {"status": "healthy", "endpoint": os.environ['MINIO_ENDPOINT']}
    except Exception as e:
        status['minio'] = {"status": "unhealthy", "error": str(e)}

    all_healthy = all(s.get("status") == "healthy" for s in status.values())

    return {
        "overall":  "healthy" if all_healthy else "degraded",
        "services": status,
    }
```

### `api/routers/stats.py`
```python
"""
Endpoint /stats — Métriques de remplissage du data lake (zones Raw + Staging + Curated).
"""
from fastapi import APIRouter, HTTPException
import psycopg2
import psycopg2.extras
import boto3
import os

router = APIRouter()

BUCKET_RAW = 'raw-football'

@router.get("/", summary="Métriques de remplissage du data lake")
def get_stats():
    """
    Retourne les métriques quantitatives de chaque zone :
    - Raw    : nombre et taille des fichiers dans MinIO
    - Staging: nombre de matchs, saisons, équipes
    - Curated: nombre de prédictions, accuracy du modèle
    """
    result = {}

    # ── Stats zone Raw (MinIO) ──
    try:
        client   = boto3.client(
            's3',
            endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
            aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
            aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
        )
        paginator = client.get_paginator('list_objects_v2')
        objects   = []
        for page in paginator.paginate(Bucket=BUCKET_RAW):
            objects.extend(page.get('Contents', []))

        total_size = sum(o['Size'] for o in objects)
        result['raw'] = {
            "bucket":        BUCKET_RAW,
            "nb_files":      len(objects),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "csv_files":     sum(1 for o in objects if o['Key'].endswith('.csv')),
            "json_files":    sum(1 for o in objects if o['Key'].endswith('.json')),
            "model_files":   sum(1 for o in objects if o['Key'].endswith('.pkl')),
        }
    except Exception as e:
        result['raw'] = {"error": f"MinIO inaccessible : {str(e)}"}

    # ── Stats zones Staging + Curated (PostgreSQL) ──
    try:
        conn = psycopg2.connect(
            host=os.environ['PG_HOST'], database=os.environ['PG_DB'],
            user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'],
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT
                COUNT(*)                    AS nb_matches,
                COUNT(DISTINCT season)      AS nb_seasons,
                COUNT(DISTINCT home_team)   AS nb_teams
            FROM staging_matches
        """)
        result['staging'] = dict(cur.fetchone())

        cur.execute("""
            SELECT
                COUNT(*) AS nb_predictions,
                ROUND(
                    SUM(CASE WHEN actual_result = predicted_result THEN 1 ELSE 0 END)::numeric
                    / NULLIF(COUNT(*), 0), 4
                ) AS model_accuracy,
                MAX(model_version) AS latest_model_version
            FROM curated_predictions
        """)
        result['curated'] = dict(cur.fetchone())

        cur.close()
        conn.close()

    except Exception as e:
        result['staging'] = {"error": str(e)}
        result['curated'] = {"error": str(e)}

    return {"data_lake": "Football Premier League", "zones": result}
```

---

## Commandes de lancement

```bash
# 1. Copier et compléter le fichier .env
cp .env.example .env
# Editer .env et mettre ta clé football-data.org dans FOOTBALL_API_KEY

# 2. Lancer tous les services
docker-compose up --build -d

# 3. Vérifier que tout est lancé (attendre ~2 min le 1er démarrage)
docker-compose ps

# 4. Accès aux interfaces
# MinIO Console  : http://localhost:9001  (minioadmin / minioadmin)
# Airflow        : http://localhost:8080  (admin / admin)
# API Swagger    : http://localhost:8000/docs

# 5. Ordre d'exécution des DAGs dans Airflow
# dag_01_ingest_csv_football   → Run (unique)
# dag_02_ingest_api_football   → Run (s'exécutera ensuite quotidiennement)
# dag_03_staging_football      → Run (après dag_01)
# dag_04_curated_football      → Run (après dag_03)

# 6. Tester l'API
curl http://localhost:8000/health
curl http://localhost:8000/stats
curl "http://localhost:8000/staging/?limit=5"
curl "http://localhost:8000/curated/?limit=5"
```

---

## Timeline 2 semaines

| Jours | Tâche | Durée estimée |
|-------|-------|---------------|
| J1–J2 | Setup Docker Compose + init_db.sql + MinIO up | ~3h |
| J3–J4 | DAG 01 (CSV ingestion) + DAG 02 (API ingestion) | ~4h |
| J5–J6 | DAG 03 (Staging + feature engineering) | ~4h |
| J7    | DAG 04 (XGBoost training + curated) | ~3h |
| J8–J9 | FastAPI (5 endpoints + tests Swagger) | ~4h |
| J10–J11 | Gestion des exceptions + code comments | ~3h |
| J12–J13 | README exhaustif + push GitHub | ~3h |
| J14   | Buffer relecture finale | — |

---

## Template README.md

```markdown
# Football Data Lake — EFREI 2025-2026

## Architecture

| Zone | Technologie | Description |
|------|-------------|-------------|
| Raw | MinIO (S3) | Fichiers CSV historiques + JSON API football-data.org |
| Staging | PostgreSQL | Matchs nettoyés + features de forme (rolling window 5 matchs) |
| Curated | PostgreSQL | Prédictions XGBoost (H/D/A) avec probabilités |
| Orchestration | Apache Airflow 2.9 | 4 DAGs + scheduling quotidien API |
| API Gateway | FastAPI | 5 endpoints : /raw /staging /curated /health /stats |

## Sources de données

- **Fichier** : football-data.co.uk — Premier League, 5 saisons (2019–2024), ~1 900 matchs, format CSV
- **API** : football-data.org — Fixtures et standings saison en cours, refresh quotidien

## Prérequis

- Docker + Docker Compose installés
- Clé API gratuite sur https://www.football-data.org/client/register

## Installation et lancement

\```bash
git clone <repo_url>
cd football-datalake
cp .env.example .env
# Éditer .env et renseigner FOOTBALL_API_KEY
docker-compose up --build -d
\```

## Utilisation

Attendre ~2 minutes le premier démarrage, puis :

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Airflow | http://localhost:8080 | admin / admin |
| API Swagger | http://localhost:8000/docs | — |

### Ordre d'exécution des DAGs (Airflow)

1. `dag_01_ingest_csv_football` — Ingestion initiale des 5 saisons CSV
2. `dag_02_ingest_api_football` — Ingestion API (s'exécutera ensuite @daily automatiquement)
3. `dag_03_staging_football` — Transformation + feature engineering
4. `dag_04_curated_football` — Entraînement XGBoost + prédictions

## Description des endpoints API

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/raw` | GET | Liste les fichiers bruts dans MinIO (filtre par prefix) |
| `/raw/file?key=...` | GET | Retourne le contenu d'un fichier JSON brut |
| `/staging` | GET | Matchs transformés avec features de forme (filtre team/season) |
| `/curated` | GET | Prédictions XGBoost avec probabilités et accuracy globale |
| `/health` | GET | État de santé de MinIO + PostgreSQL |
| `/stats` | GET | Métriques quantitatives des 3 zones du data lake |

## Modèle ML — XGBoost

**Objectif** : classifier le résultat d'un match (H = victoire domicile, D = nul, A = victoire extérieur)

**Features** : forme sur 5 matchs (pts), buts marqués/concédés moyens sur 5 matchs, pour chaque équipe

**Résultats** : voir `/stats` ou `/curated` pour l'accuracy courante du modèle

Le modèle est sauvegardé dans MinIO (`models/`) et réentraîné automatiquement chaque semaine.
\```
```
