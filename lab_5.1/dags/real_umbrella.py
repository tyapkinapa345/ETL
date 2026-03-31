import json
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# ========== НАСТРОЙКИ ==========
API_KEY = "8c436c2106bf40599dd104558262803"   # Ваш API-ключ
CITY = "Dubai"
DAYS = 3
TEMPERATURE_THRESHOLD = 30

DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

RAW_DATA_PATH = f"{DATA_DIR}/dubai_forecast.csv"
HOT_DAYS_FILE = f"{DATA_DIR}/hot_days_list.txt"   # Файл со списком жарких дней
ERROR_LOG_FILE = f"{DATA_DIR}/error_log.txt"

# ========== ФУНКЦИИ ==========
def check_api_availability():
    """Проверяет доступность WeatherAPI (HEAD-запрос)."""
    url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={CITY}&days=1"
    try:
        resp = requests.head(url, timeout=10)
        resp.raise_for_status()
        print("API доступен")
    except Exception as e:
        with open(ERROR_LOG_FILE, "a") as f:
            f.write(f"{datetime.now()} - API недоступен: {str(e)}\n")
        raise

def fetch_weather_forecast(**kwargs):
    """Загружает прогноз, фильтрует дни >30°C и сохраняет список дат."""
    url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={CITY}&days={DAYS}&aqi=no&alerts=no"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        with open(ERROR_LOG_FILE, "a") as f:
            f.write(f"{datetime.now()} - Ошибка загрузки прогноза: {str(e)}\n")
        raise

    forecast_list = []
    for day in data['forecast']['forecastday']:
        temp = day['day']['maxtemp_c']
        date = day['date']
        forecast_list.append({
            'date': date,
            'temp_c': temp,
            'condition': day['day']['condition']['text']
        })
    
    df = pd.DataFrame(forecast_list)
    df.to_csv(RAW_DATA_PATH, index=False)
    
    # Фильтруем жаркие дни
    hot_days_df = df[df['temp_c'] > TEMPERATURE_THRESHOLD]
    
    # Сохраняем список дат в текстовый файл (по заданию)
    with open(HOT_DAYS_FILE, 'w') as f:
        f.write(f"Даты, когда температура > {TEMPERATURE_THRESHOLD}°C в {CITY}:\n")
        if hot_days_df.empty:
            f.write("Нет таких дней в прогнозе.\n")
        else:
            for _, row in hot_days_df.iterrows():
                f.write(f"{row['date']}: {row['temp_c']}°C, {row['condition']}\n")
    
    # Также сохраняем количество (на всякий случай)
    count_hot = len(hot_days_df)
    with open(f"{DATA_DIR}/hot_days_count.txt", 'w') as f:
        f.write(str(count_hot))
    
    print(f"Найдено жарких дней: {count_hot}. Список сохранён в {HOT_DAYS_FILE}")
    kwargs['ti'].xcom_push(key='hot_days_count', value=count_hot)

# ========== ОПРЕДЕЛЕНИЕ DAG ==========
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='variant_16_dubai_hot_days',
    default_args=default_args,
    description='Находит дни с температурой >30°C в Дубае по прогнозу',
    schedule_interval=None,
    catchup=False,
    tags=['weather', 'hot_days'],
) as dag:
    
    start = DummyOperator(task_id='start')
    check_api = PythonOperator(
        task_id='check_api_availability',
        python_callable=check_api_availability,
    )
    fetch = PythonOperator(
        task_id='fetch_weather_forecast',
        python_callable=fetch_weather_forecast,
        provide_context=True,
    )
    end = DummyOperator(task_id='end')
    
    start >> check_api >> fetch >> end
