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
API_KEY = "8c436c2106bf40599dd104558262803"   # Вставьте ваш API-ключ от WeatherAPI.com
CITY = "Dubai"
DAYS = 3
TEMPERATURE_THRESHOLD = 30   # для фильтрации >30°C

# Пути для сохранения файлов (внутри контейнера)
DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

RAW_DATA_PATH = f"{DATA_DIR}/dubai_forecast.csv"
HOT_DAYS_COUNT_PATH = f"{DATA_DIR}/dubai_hot_days_count.txt"
AVG_TEMP_PATH = f"{DATA_DIR}/avg_temp.txt"
MODEL_PATH = f"{DATA_DIR}/ml_model.pkl"

# ========== ФУНКЦИИ ==========
def fetch_weather_forecast(**kwargs):
    """Загрузка прогноза погоды для Дубай на 3 дня."""
    url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={CITY}&days={DAYS}&aqi=no&alerts=no"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Извлечение данных
    forecast_list = []
    for day in data['forecast']['forecastday']:
        forecast_list.append({
            'date': day['date'],
            'temp_c': day['day']['maxtemp_c'],
            'condition': day['day']['condition']['text']
        })
    df = pd.DataFrame(forecast_list)
    
    # Сохраняем сырые данные
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"Сырые данные сохранены в {RAW_DATA_PATH}")
    
    # Фильтрация по температуре >30°C
    hot_days = df[df['temp_c'] > TEMPERATURE_THRESHOLD]
    count_hot = len(hot_days)
    print(f"Количество дней с температурой > {TEMPERATURE_THRESHOLD}°C: {count_hot}")
    
    # Сохраняем количество жарких дней
    with open(HOT_DAYS_COUNT_PATH, 'w') as f:
        f.write(str(count_hot))
    
    # Сохраняем среднюю температуру для прогнозирования продаж
    avg_temp = df['temp_c'].mean()
    with open(AVG_TEMP_PATH, 'w') as f:
        f.write(str(avg_temp))
    
    # Передаём данные в XCom для последующих задач
    kwargs['ti'].xcom_push(key='hot_days_count', value=count_hot)
    kwargs['ti'].xcom_push(key='avg_temp', value=avg_temp)
    
    return df.to_dict('records')  # возвращаем для возможного использования

def train_model(**kwargs):
    """Обучение простой линейной регрессии на исторических данных (если есть)."""
    # Здесь можно реализовать загрузку исторических данных, но для примера
    # создадим синтетические данные: температура vs продажи зонтов.
    # В реальном проекте данные брались бы из базы или CSV.
    # Для демонстрации мы используем фиктивные данные.
    # Если у вас есть реальные исторические данные, замените этот блок.
    
    # Синтетические данные: температура от 15 до 35, продажи от 50 до 10 (чем жарче, тем меньше продаж)
    import numpy as np
    np.random.seed(42)
    temperatures = np.linspace(15, 35, 100)
    sales = 100 - 2 * (temperatures - 15) + np.random.normal(0, 5, size=100)
    
    X = temperatures.reshape(-1, 1)
    y = sales
    
    model = LinearRegression()
    model.fit(X, y)
    
    # Сохраняем модель с помощью joblib
    joblib.dump(model, MODEL_PATH)
    print(f"Модель сохранена в {MODEL_PATH}")

def save_model_info(**kwargs):
    """Сохранение дополнительной информации о модели (опционально)."""
    # Можем сохранить метрики или просто подтверждение
    with open(f"{DATA_DIR}/model_info.txt", 'w') as f:
        f.write(f"Модель обучена {datetime.now()}\n")
        f.write(f"Использованы синтетические данные\n")
    print("Информация о модели сохранена")

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
    dag_id='variant_16_dubai',
    default_args=default_args,
    description='ETL для прогноза погоды в Дубай и подсчёта жарких дней',
    schedule_interval=None,  # запуск только по триггеру
    catchup=False,
    tags=['umbrella', 'dubai', 'variant_16_dubai'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    fetch = PythonOperator(
        task_id='fetch_weather_forecast',
        python_callable=fetch_weather_forecast,
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
    
    # Определяем зависимости
    start >> fetch >> train >> save_info >> end
