# Football Data Lake - EFREI 2025-2026

Projet final du cours **Data Lakes & Data Integration** (Yvann Vincent, EFREI Paris).

Data lake complet sur la Premier League anglaise, de l'ingestion brute jusqu'à l'exposition via API, avec un modèle de Machine Learning (XGBoost) intégré au pipeline pour prédire l'issue des matchs (victoire domicile / nul / victoire extérieur).

**Auteur** : Stéphane Sando ([GitHub](https://github.com/StephaneSando) · [LinkedIn](https://linkedin.com/in/stephane-sando))

---

## Sommaire

1. [Architecture](#architecture)
2. [Stack technique](#stack-technique)
3. [Sources de données](#sources-de-données)
4. [Structure du projet](#structure-du-projet)
5. [Prérequis](#prérequis)
6. [Installation](#installation)
7. [Lancer le pipeline (ordre des DAGs)](#lancer-le-pipeline-ordre-des-dags)
8. [Vérifier que tout fonctionne](#vérifier-que-tout-fonctionne)
9. [API Gateway - endpoints](#api-gateway---endpoints)
10. [Dashboard de visualisation](#dashboard-de-visualisation)
11. [Modèle de Machine Learning](#modèle-de-machine-learning)
12. [Dépannage (troubleshooting)](#dépannage-troubleshooting)
13. [Checklist des critères d'évaluation](#checklist-des-critères-dévaluation)

---

## Architecture

Le data lake est structuré en 3 zones classiques, orchestrées par Apache Airflow et exposées par une API FastAPI.

```
football-data.co.uk (CSV)     football-data.org (API REST)
        |                              |
        v                              v
   +---------------------------------------------+
   |         ZONE RAW  -  MinIO (S3)              |
   |   CSV bruts (5 saisons) + JSON fixtures/      |
   |   standings + modèles ML sérialisés (.pkl)    |
   +---------------------------------------------+
                       |
                       v
   +---------------------------------------------+
   |     ZONE STAGING  -  PostgreSQL               |
   |   Matchs nettoyés + features de forme         |
   |   (rolling window 5 derniers matchs)          |
   +---------------------------------------------+
                       |
                       v
   +---------------------------------------------+
   |     ZONE CURATED  -  PostgreSQL               |
   |   Prédictions XGBoost (H / D / A)             |
   |   + probabilités + accuracy du modèle         |
   +---------------------------------------------+
                       |
                       v
   +---------------------------------------------+
   |          API GATEWAY  -  FastAPI              |
   |   /raw  /staging  /curated  /health  /stats   |
   |   + /dashboard (visualisation web)            |
   +---------------------------------------------+
```

Toute la chaîne (ingestion -> staging -> curated) est orchestrée par **Apache Airflow**, avec 4 DAGs indépendants et un scheduling adapté à chaque source.

---

## Stack technique

| Composant | Technologie | Rôle |
|---|---|---|
| Zone Raw | MinIO (compatible S3) | Stockage objet des fichiers bruts |
| Zone Staging | PostgreSQL | Données nettoyées + feature engineering |
| Zone Curated | PostgreSQL | Prédictions du modèle ML |
| Orchestration | Apache Airflow 2.9 | 4 DAGs, scheduling automatisé |
| API Gateway | FastAPI | Exposition REST des 3 zones + monitoring |
| Machine Learning | XGBoost (scikit-learn) | Classification multi-classe H/D/A |
| Conteneurisation | Docker Compose | 6 services orchestrés |

---

## Sources de données

### Source fichier - football-data.co.uk

Aucune inscription requise. 5 saisons de Premier League (2019-2024), environ 1 900 matchs au format CSV, téléchargées directement par le DAG d'ingestion.

### Source API - football-data.org

Nécessite une clé API gratuite.

1. Créer un compte sur https://www.football-data.org/client/register
2. Récupérer la clé API dans le dashboard du compte
3. La renseigner dans le fichier `.env` (voir section Installation)

Tier gratuit : 10 requêtes/minute, accès Premier League + Champions League. Le DAG respecte automatiquement cette limite (délai de 6 secondes entre les deux appels).

---

## Structure du projet

```
football-datalake/
├── docker-compose.yml
├── .env                        # A créer (non versionné)
├── .env.example                # Modèle sans valeurs sensibles
├── .gitignore
├── README.md
├── scripts/
│   └── init_db.sql             # Création automatique des tables PostgreSQL
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── dag_01_ingest_csv.py     # CSV -> MinIO Raw (@once)
│       ├── dag_02_ingest_api.py     # API -> MinIO Raw (@daily)
│       ├── dag_03_staging.py        # Raw -> Staging (@daily)
│       └── dag_04_curated.py        # Staging -> Curated + ML (@weekly)
└── api/
    ├── Dockerfile
    ├── requirements.txt
    ├── main.py
    └── routers/
        ├── raw.py
        ├── staging.py
        ├── curated.py
        ├── health.py
        ├── stats.py
        └── dashboard.py
```

---

## Prérequis

- **Docker** et **Docker Compose** installés ([guide d'installation](https://docs.docker.com/get-docker/))
- Une clé API gratuite sur https://www.football-data.org/client/register
- Ports **8080**, **8000**, **9000**, **9001**, **5432** disponibles sur la machine hôte

---

## Installation

```bash
# 1. Cloner le dépôt
git clone https://github.com/StephaneSando/DataLake_Football.git
cd DataLake_Football

# 2. Créer le fichier .env à partir du modèle
cp .env.example .env
```

Ouvrir `.env` et renseigner la clé API football-data.org :

```
FOOTBALL_API_KEY=votre_clé_ici
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
PG_USER=postgres
PG_PASSWORD=postgres
PG_DB=football_lake
AIRFLOW_UID=50000
```

> Sur Linux/Mac, remplacer `AIRFLOW_UID=50000` par le résultat de `id -u` pour éviter des problèmes de permissions sur les volumes Airflow.

```bash
# 3. Construire et lancer tous les services
docker-compose up --build -d

# 4. Vérifier que tous les conteneurs sont "healthy" ou "running"
# (patienter environ 2 minutes le premier démarrage)
docker-compose ps
```

Six services doivent apparaître : `minio`, `postgres`, `airflow-webserver`, `airflow-scheduler`, `fastapi`, et `airflow-init` (qui s'arrête après son travail, c'est normal).

### Accès aux interfaces

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| API Swagger | http://localhost:8000/docs | - |
| Dashboard | http://localhost:8000/dashboard | - |

Si la connexion Airflow échoue (`Invalid login`), recréer l'utilisateur manuellement :

```bash
docker exec -it airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@football.com
```

---

## Lancer le pipeline (ordre des DAGs)

Dans l'interface Airflow (http://localhost:8080), déclencher les DAGs **dans cet ordre** en cliquant sur le bouton "Play" (▶) de chacun :

| Ordre | DAG | Schedule | Ce qu'il fait |
|---|---|---|---|
| 1 | `dag_01_ingest_csv_football` | `@once` | Télécharge les 5 CSV historiques -> MinIO |
| 2 | `dag_02_ingest_api_football` | `@daily` | Récupère fixtures + standings via l'API -> MinIO + PostgreSQL |
| 3 | `dag_03_staging_football` | `@daily` | Nettoie les CSV et calcule les features de forme -> PostgreSQL Staging |
| 4 | `dag_04_curated_football` | `@weekly` | Entraîne XGBoost et génère les prédictions -> PostgreSQL Curated |

Chaque DAG doit être **entièrement vert** (statut "success") avant de lancer le suivant, en particulier `dag_01` avant `dag_03` (qui a besoin des CSV dans MinIO) et `dag_03` avant `dag_04` (qui a besoin du staging rempli).

Temps d'exécution total du pipeline complet : environ 3 à 5 minutes.

---

## Vérifier que tout fonctionne

### 1. Dans MinIO (bucket `raw-football`)

```
raw-football/
├── premier_league/
│   ├── 2019-20/matches.csv
│   ├── 2020-21/matches.csv
│   ├── 2021-22/matches.csv
│   ├── 2022-23/matches.csv
│   └── 2023-24/matches.csv
├── api/
│   ├── matches/PL_matches_<timestamp>.json
│   └── standings/PL_standings_<timestamp>.json
└── models/
    └── xgb_match_predictor_<timestamp>.pkl
```

### 2. Dans PostgreSQL

```bash
docker exec -it postgres psql -U postgres -d football_lake -c "SELECT COUNT(*) FROM staging_matches;"
docker exec -it postgres psql -U postgres -d football_lake -c "SELECT COUNT(*) FROM curated_predictions;"
```

Les deux requêtes doivent retourner environ 1 900 lignes.

### 3. Via l'API

```bash
curl http://localhost:8000/health
curl http://localhost:8000/stats
```

`/health` doit retourner `"overall": "healthy"`. `/stats` doit afficher un nombre de fichiers, de matchs et une accuracy non nulle.

---

## API Gateway - endpoints

Documentation interactive Swagger complète : **http://localhost:8000/docs**

| Endpoint | Méthode | Description |
|---|---|---|
| `/raw` | GET | Liste les fichiers bruts dans MinIO (paramètre `prefix` optionnel) |
| `/raw/file?key=...` | GET | Contenu d'un fichier JSON brut |
| `/staging` | GET | Matchs nettoyés avec features de forme (filtres `team`, `season`) |
| `/curated` | GET | Prédictions XGBoost avec probabilités et accuracy globale |
| `/health` | GET | État de santé de MinIO et PostgreSQL |
| `/stats` | GET | Métriques de remplissage des 3 zones |
| `/dashboard` | GET | Interface web de visualisation (voir section suivante) |

### Exemples de requêtes

```bash
# Liste des fichiers bruts contenant "api"
curl "http://localhost:8000/raw?prefix=api/"

# Matchs d'Arsenal en staging
curl "http://localhost:8000/staging?team=Arsenal&limit=10"

# Prédictions pour les matchs à domicile de Liverpool
curl "http://localhost:8000/curated?home_team=Liverpool&limit=10"

# Métriques globales du data lake
curl "http://localhost:8000/stats"
```

---

## Dashboard de visualisation

Une interface web est disponible sur **http://localhost:8000/dashboard** pour explorer visuellement les 3 couches sans écrire de requêtes :

- **Cartes de synthèse** : nombre de fichiers Raw, matchs en Staging, prédictions en Curated, accuracy du modèle
- **Onglet Staging** : table paginée et filtrable par équipe, avec forme et moyennes de buts
- **Onglet Curated** : prédictions avec indicateur visuel correct/incorrect et barres de probabilité H/D/A
- **Onglet Raw** : liste de tous les fichiers stockés dans MinIO

---

## Modèle de Machine Learning

**Objectif** : classifier l'issue d'un match de football (H = victoire domicile, D = match nul, A = victoire extérieur).

**Algorithme** : XGBoost (`XGBClassifier`, objectif `multi:softprob`)

**Features utilisées** (calculées sur les 5 matchs précédents de chaque équipe, sans fuite de données grâce à un décalage temporel) :

- Forme de l'équipe à domicile et à l'extérieur (points cumulés sur 5 matchs)
- Moyenne de buts marqués sur 5 matchs (domicile / extérieur)
- Moyenne de buts concédés sur 5 matchs (domicile / extérieur)

**Entraînement** : split 80/20 stratifié, réentraînement automatique chaque semaine (`dag_04_curated_football`).

**Suivi de la performance** : l'accuracy du modèle est recalculée à chaque entraînement et consultable via `/stats` ou `/curated`.

Le modèle sérialisé (`joblib`) est versionné par timestamp et stocké dans MinIO (`models/`), ce qui permet de conserver un historique des modèles entraînés.

---

## Dépannage (troubleshooting)

### `airflow-init` échoue avec `PendingRollbackError` / `UniqueViolation serialized_dag_pkey`

Se produit si l'initialisation est relancée sur des volumes déjà partiellement migrés.

```bash
docker-compose stop airflow-init
docker-compose rm -f airflow-init
docker-compose up airflow-init
```

Si l'erreur persiste, réinitialiser complètement les volumes (perte des données MinIO/PostgreSQL) :

```bash
docker-compose down -v
docker-compose up --build -d
```

### `Invalid login` sur l'interface Airflow

L'utilisateur admin n'a pas été créé correctement. Le recréer manuellement :

```bash
docker exec -it airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@football.com
```

### `dag_03_staging_football` échoue avec "Aucun fichier CSV trouvé"

`dag_01_ingest_csv_football` n'a pas encore été exécuté avec succès. Le lancer d'abord et attendre son statut "success" avant de relancer `dag_03`.

### `dag_04_curated_football` échoue avec "Pas assez de données"

`dag_03_staging_football` n'a pas terminé ou a échoué silencieusement. Vérifier le contenu de `staging_matches` :

```bash
docker exec -it postgres psql -U postgres -d football_lake -c "SELECT COUNT(*) FROM staging_matches;"
```

### `dag_02_ingest_api_football` échoue avec "Rate limit atteint"

Le tier gratuit de football-data.org est limité à 10 requêtes/minute. Airflow retentera automatiquement la tâche (3 tentatives configurées, 10 minutes d'intervalle) - aucune action nécessaire, patienter.

### Port déjà utilisé (`port is already allocated`)

Un autre service utilise déjà l'un des ports (8080, 8000, 9000, 9001, 5432). Arrêter le service concurrent ou modifier le port exposé dans `docker-compose.yml` (partie gauche du mapping `"HOST:CONTAINER"`).

---

## Checklist des critères d'évaluation

| Exigence du sujet | Statut | Où le vérifier |
|---|---|---|
| Zone Raw en S3 ou Elasticsearch | Fait (MinIO, compatible S3) | Console MinIO, port 9001 |
| Deux sources (fichier + API) | Fait | `dag_01` (CSV) et `dag_02` (API) |
| Pipeline d'intégration Airflow/DVC | Fait (Airflow) | Interface Airflow, port 8080 |
| Endpoint `/raw` | Fait | `/docs` Swagger |
| Endpoint `/staging` | Fait | `/docs` Swagger |
| Endpoint `/curated` | Fait | `/docs` Swagger |
| Endpoint `/health` | Fait | `/docs` Swagger |
| Endpoint `/stats` | Fait | `/docs` Swagger |
| Modèle ML intégré au pipeline | Fait (XGBoost, `dag_04`) | `/curated`, `/stats` |
| Gestion des exceptions | Fait (try/except sur chaque tâche et endpoint) | Code source |
| Documentation technique | Ce README | - |

---

## Arrêter le projet

```bash
# Arrêter tous les services (conserve les données)
docker-compose stop

# Arrêter et supprimer les conteneurs (conserve les volumes/données)
docker-compose down

# Tout supprimer y compris les données MinIO/PostgreSQL
docker-compose down -v
```