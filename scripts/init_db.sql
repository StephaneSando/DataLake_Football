CREATE DATABASE airflow;

CREATE TABLE IF NOT EXISTS raw_files_log (
    id          SERIAL PRIMARY KEY,
    filename    VARCHAR(255) NOT NULL,
    source      VARCHAR(50)  NOT NULL,
    bucket      VARCHAR(100) NOT NULL,
    object_key  VARCHAR(500) NOT NULL,
    uploaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging_matches (
    id                      SERIAL PRIMARY KEY,
    date                    DATE,
    home_team               VARCHAR(100),
    away_team               VARCHAR(100),
    home_goals              INTEGER,
    away_goals              INTEGER,
    result                  CHAR(1),
    home_shots              INTEGER,
    away_shots              INTEGER,
    home_shots_on_target    INTEGER,
    away_shots_on_target    INTEGER,
    home_form               FLOAT,
    away_form               FLOAT,
    home_avg_goals_scored   FLOAT,
    home_avg_goals_conceded FLOAT,
    away_avg_goals_scored   FLOAT,
    away_avg_goals_conceded FLOAT,
    season                  VARCHAR(10),
    source                  VARCHAR(50),
    processed_at            TIMESTAMP DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS curated_predictions (
    id               SERIAL PRIMARY KEY,
    date             DATE,
    home_team        VARCHAR(100),
    away_team        VARCHAR(100),
    actual_result    CHAR(1),
    predicted_result CHAR(1),
    prob_home        FLOAT,
    prob_draw        FLOAT,
    prob_away        FLOAT,
    model_version    VARCHAR(50),
    predicted_at     TIMESTAMP DEFAULT NOW()
);
