# Automated Weather Data Pipeline
### Apache Airflow · Snowflake · Python · Open-Meteo API

An end-to-end automated data pipeline that fetches live weather data for 5 US cities daily and loads it into Snowflake — orchestrated by Apache Airflow running locally via Astro CLI.

---

## Architecture

```
Open-Meteo API (free, no key needed)
        ↓
Apache Airflow DAG
  ├── fetch_nyc    ─┐
  ├── fetch_la     ─┤ (run in parallel)
  └── fetch_chicago─┘
        ↓
load_to_snowflake (SnowflakeHook)
        ↓
Snowflake — WEATHER_DB.RAW.DAILY_WEATHER
```

---

## Tech Stack

- **Orchestration:** Apache Airflow 3.x (via Astro CLI)
- **Language:** Python 3.13
- **Data Warehouse:** Snowflake
- **API:** Open-Meteo (free, no signup required)
- **Local Environment:** Docker Desktop + Astro CLI

---

## Airflow Concepts Used

| Concept | Where Used |
|---|---|
| PythonOperator | fetch and load tasks |
| XComs (xcom_push / xcom_pull) | passing weather data between tasks |
| Parallel task execution | 3 city fetches run simultaneously |
| SnowflakeHook | loading data into Snowflake |
| Airflow Connections | storing Snowflake credentials securely |
| Airflow Variables | storing city coordinates |
| Cron scheduling | runs every day at 8am |
| catchup=False | no backfilling past runs |
| retries | automatic retry on failure |

---

## DAG Structure

```python
fetch_nyc ─┐
fetch_la   ─┤ >> load_to_snowflake
fetch_chicago─┘
```

All 3 fetch tasks run in parallel. The load task waits for all 3 to finish before inserting into Snowflake.

---

## Snowflake Table

```sql
CREATE TABLE WEATHER_DB.RAW.DAILY_WEATHER (
    city          VARCHAR(50),
    fetch_date    DATE,
    temp_max_c    FLOAT,
    temp_min_c    FLOAT,
    loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Sample Data

| CITY | FETCH_DATE | TEMP_MAX_C | TEMP_MIN_C | LOADED_AT |
|---|---|---|---|---|
| NYC | 2026-04-17 | 26.2 | 16.9 | 2026-04-16 21:39:56 |
| LA | 2026-04-17 | 25.6 | 11.7 | 2026-04-16 21:39:59 |
| CHICAGO | 2026-04-17 | 28.6 | 8.5 | 2026-04-16 21:40:02 |

---

## Project Setup

### Prerequisites
- Docker Desktop installed and running
- Astro CLI installed
- Snowflake account (free trial works)

### Step 1 — Clone the repo
```bash
git clone https://github.com/ushasreedindi/weather-pipeline
cd weather-pipeline
```

### Step 2 — Add Snowflake provider
Add to `requirements.txt`:
```
apache-airflow-providers-snowflake==5.3.0
```

### Step 3 — Start Airflow
```bash
astro dev start
```
Open `http://localhost:8080` — login with `admin/admin`

### Step 4 — Set up Snowflake Connection
In Airflow UI → Admin → Connections → Add:
```
Connection ID:   snowflake_default
Connection Type: Snowflake
Account:         your_account_id
Login:           your_username
Password:        your_password
Database:        WEATHER_DB
Schema:          RAW
Warehouse:       COMPUTE_WH
```

### Step 5 — Create Snowflake table
Run in your Snowflake worksheet:
```sql
CREATE DATABASE IF NOT EXISTS WEATHER_DB;
CREATE SCHEMA IF NOT EXISTS WEATHER_DB.RAW;

CREATE TABLE IF NOT EXISTS WEATHER_DB.RAW.DAILY_WEATHER (
    city          VARCHAR(50),
    fetch_date    DATE,
    temp_max_c    FLOAT,
    temp_min_c    FLOAT,
    loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Step 6 — Trigger the DAG
In Airflow UI → find `weather_pipeline` → click ▶ Trigger

---

## DAG Code Highlights

### Fetching weather data with XCom push
```python
def fetch_weather(city, lat, lon, **context):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min",
        "timezone": "America/New_York", "forecast_days": 1
    }
    r = requests.get(url, params=params)
    data = r.json()
    context['ti'].xcom_push(key=city, value=data)
```

### Loading to Snowflake with XCom pull
```python
def load_to_snowflake(**context):
    ti = context['ti']
    cities = {"nyc": [40.71, -74.00], "la": [34.05, -118.24], "chicago": [41.85, -87.65]}
    rows = []
    for city in cities:
        raw = ti.xcom_pull(task_ids=f'fetch_{city}', key=city)
        if raw and 'daily' in raw:
            rows.append((city.upper(), raw['daily']['time'][0],
                raw['daily']['temperature_2m_max'][0],
                raw['daily']['temperature_2m_min'][0]))
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    for row in rows:
        hook.run(
            f"INSERT INTO WEATHER_DB.RAW.DAILY_WEATHER "
            f"(city, fetch_date, temp_max_c, temp_min_c) "
            f"VALUES ('{row[0]}', '{row[1]}', {row[2]}, {row[3]})")
```

---

## What I Learned

- Setting up Apache Airflow locally using Astro CLI and Docker
- Writing DAGs with parallel task execution and task dependencies
- Passing data between tasks using XComs
- Connecting Airflow to Snowflake using SnowflakeHook and Airflow Connections
- Debugging real Airflow errors in task logs
- Scheduling automated pipelines with cron expressions

---

## Future Improvements

- Add HttpSensor to check API availability before fetching
- Add BranchPythonOperator to alert when temperature exceeds 35°C
- Expand to 5 cities (Houston, Miami)
- Add dbt transformation layer on top of Snowflake raw table
- Deploy to Astronomer Cloud for 24/7 scheduling

---

## Author

**Usha Sree Dindi**
Data Engineer | M.S. Business Analytics, UConn 2025
[LinkedIn](https://linkedin.com/in/ushasreedindi) · [GitHub](https://github.com/ushasreedindi)
