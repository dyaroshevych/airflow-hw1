from datetime import datetime
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


COORDINATES = {
    "Lviv": ("49.8397", "24.0297"),
    "Kyiv": ("50.4501", "30.5234"),
    "Kharkiv": ("49.9935", "36.2304"),
    "Odesa": ("46.4825", "30.7233"),
    "Zhmerynka": ("49.039051", "28.108594"),
}


def fetch_weather_data(city, execution_date):
    """
    Fetch weather data for a given city and date using the requests library.
    """
    api_key = "a30e6f3554083cc6bf9be68d9e6484cc"
    execution_date = datetime.fromisoformat(execution_date)
    dt = int(execution_date.timestamp())
    lat, lon = COORDINATES[city]

    url = f"http://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={dt}&appid={api_key}"
    response = requests.get(url)
    return json.loads(response.text)


def process_and_store_weather_data(ti, city):
    """
    Process and store the fetched weather data.
    """
    data = ti.xcom_pull(task_ids=f'fetch_weather_{city}')
    daily_data = data.get('data', [])[0]

    timestamp = datetime.utcfromtimestamp(int(daily_data["dt"])).strftime('%Y-%m-%d')
    temp = daily_data["temp"]
    humidity = daily_data["humidity"]
    cloudiness = daily_data["clouds"]
    wind_speed = daily_data["wind_speed"]

    insert_query = """
    INSERT INTO measurements (city, timestamp, temperature, humidity, cloudiness, wind_speed)
    VALUES (?, ?, ?, ?, ?, ?);
    """
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    sqlite_hook.run(insert_query, parameters=(city, timestamp, temp, humidity, cloudiness, wind_speed))

def main():
    with DAG('weather_data_pipeline', start_date=datetime(2023, 11, 23), schedule_interval="@daily", catchup=True):
        create_table = SqliteOperator(
            task_id='create_table',
            sqlite_conn_id='sqlite_default',
            sql="""
            CREATE TABLE IF NOT EXISTS measurements (
                city TEXT,
                timestamp TEXT,
                temperature REAL,
                humidity REAL,
                cloudiness REAL,
                wind_speed REAL
            );
            """
        )

        for city in COORDINATES.keys():
            fetch_weather = PythonOperator(
                task_id=f'fetch_weather_{city}',
                python_callable=fetch_weather_data,
                op_kwargs={'city': city, 'execution_date': '{{ ds }}'},
            )

            process_store_weather = PythonOperator(
                task_id=f'process_store_weather_{city}',
                python_callable=process_and_store_weather_data,
                op_kwargs={'city': city},
            )

            create_table >> fetch_weather >> process_store_weather


if __name__ == "__main__":
    main()
