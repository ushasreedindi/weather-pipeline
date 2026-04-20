from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import json

def fetch_weather(city, lat, lon, **context):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min",
        "timezone": "America/New_York",
        "forecast_days": 1
    }
    r = requests.get(url, params=params)
    data = r.json()
    context['ti'].xcom_push(key=city, value=data)
    print(f"Fetched {city}: {data['daily']}")

def load_to_snowflake(**context):
    ti = context['ti']
    cities = {"nyc": [40.71, -74.00], "la": [34.05, -118.24], "chicago": [41.85, -87.65]}
    rows = []
    for city in cities:
        raw = ti.xcom_pull(task_ids=f'fetch_{city}', key=city)
        print(f"XCom data for {city}: {raw}")
        if raw and 'daily' in raw:
            rows.append((
                city.upper(),
                raw['daily']['time'][0],
                raw['daily']['temperature_2m_max'][0],
                raw['daily']['temperature_2m_min'][0],
            ))
    print(f"Total rows to insert: {len(rows)}")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    for row in rows:
        hook.run(
            f"INSERT INTO WEATHER_DB.RAW.DAILY_WEATHER "
            f"(city, fetch_date, temp_max_c, temp_min_c) "
            f"VALUES ('{row[0]}', '{row[1]}', {row[2]}, {row[3]})"
        )
    print(f"Loaded {len(rows)} rows into Snowflake!")

default_args = {
    'owner': 'usha',
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

cities_map = {"nyc": [40.71, -74.00], "la": [34.05, -118.24], "chicago": [41.85, -87.65]}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    schedule='0 8 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['weather', 'portfolio', 'snowflake'],
) as dag:

    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{city}',
            python_callable=fetch_weather,
            op_kwargs={'city': city, 'lat': coords[0], 'lon': coords[1]},
        )
        for city, coords in cities_map.items()
    ]

    load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )

    fetch_tasks >> load