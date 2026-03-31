import json
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from sklearn.linear_model import LinearRegression
import joblib
import dill

# ========== НАСТРОЙКИ ==========
API_KEY = "8c436c2106bf40599dd104558262803"   # Ваш API-ключ
CITY = "Dubai"
TARGET_DATE = "2024-07-15"   # Интересующая дата (историческая)
TEMPERATURE_THRESHOLD = 30

# Пути для сохранения файлов
DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

RAW_DATA_PATH = f"{DATA_DIR}/dubai_history_{TARGET_DATE}.csv"
HOT_DAYS_COUNT_PATH = f"{DATA_DIR}/dubai_hot_days_count.txt"
HOT_DAYS_LIST_PATH = f"{DATA_DIR}/hot_days_list.txt"
AVG_TEMP_PATH = f"{DATA_DIR}/avg_temp.txt"
MODEL_PATH = f"{DATA_DIR}/ml_model.pkl"

# ========== ФУНКЦИИ ==========
def fetch_weather_history(**kwargs):
    """Загрузка исторических погодных данных для заданной даты."""
    url = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={CITY}&dt={TARGET_DATE}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Извлечение данных за день
    day_data = data['forecast']['forecastday'][0]['day']
    forecast_list = [{
        'date': TARGET_DATE,
        'temp_c': day_data['maxtemp_c'],
        'condition': day_data['condition']['text']
    }]
    df = pd.DataFrame(forecast_list)
    
    # Сохраняем сырые данные
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"Данные за {TARGET_DATE} сохранены в {RAW_DATA_PATH}")
    
    # Фильтрация по температуре >30°C
    hot_days = df[df['temp_c'] > TEMPERATURE_THRESHOLD]
    count_hot = len(hot_days)
    print(f"Температура {df['temp_c'].values[0]}°C -> {'>' if count_hot else '<='} {TEMPERATURE_THRESHOLD}°C")
    
    # Сохраняем количество жарких дней
    with open(HOT_DAYS_COUNT_PATH, 'w') as f:
        f.write(str(count_hot))
    
    # Сохраняем список дат и температур
    with open(HOT_DAYS_LIST_PATH, 'w') as f:
        if count_hot == 0:
            f.write(f"Нет дней с температурой > {TEMPERATURE_THRESHOLD}°C\n")
        else:
            for _, row in hot_days.iterrows():
                f.write(f"{row['date']}: {row['temp_c']}°C\n")
    print(f"Список жарких дней сохранён в {HOT_DAYS_LIST_PATH}")
    
    # Сохраняем среднюю температуру (она же равна температуре дня)
    avg_temp = df['temp_c'].mean()
    with open(AVG_TEMP_PATH, 'w') as f:
        f.write(str(avg_temp))
    
    # XCom
    kwargs['ti'].xcom_push(key='hot_days_count', value=count_hot)
    kwargs['ti'].xcom_push(key='avg_temp', value=avg_temp)
    
    return df.to_dict('records')

def train_model(**kwargs):
    """Обучение модели на синтетических данных."""
    import numpy as np
    np.random.seed(42)
    temperatures = np.linspace(15, 35, 100)
    sales = 100 - 2 * (temperatures - 15) + np.random.normal(0, 5, size=100)
    X = temperatures.reshape(-1, 1)
    y = sales
    model = LinearRegression()
    model.fit(X, y)
    joblib.dump(model, MODEL_PATH)
    print(f"Модель сохранена в {MODEL_PATH}")

def save_model_info(**kwargs):
    with open(f"{DATA_DIR}/model_info.txt", 'w') as f:
        f.write(f"Модель обучена {datetime.now()}\n")
        f.write(f"Использованы синтетические данные\n")
    print("Информация о модели сохранена")

# ========== DAG ==========
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
    dag_id='variant_16_dubai_history',
    default_args=default_args,
    description='Исторические данные для Дубай за конкретную дату, фильтрация >30°C',
    schedule_interval=None,
    catchup=False,
    tags=['umbrella', 'dubai', 'history'],
) as dag:
    
    start = DummyOperator(task_id='start')
    fetch = PythonOperator(
        task_id='fetch_weather_history',
        python_callable=fetch_weather_history,
        provide_context=True,
    )
    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        provide_context=True,
    )
    save_info = PythonOperator(
        task_id='save_model_info',
        python_callable=save_model_info,
        provide_context=True,
    )
    end = DummyOperator(task_id='end')
    
    start >> fetch >> train >> save_info >> end
