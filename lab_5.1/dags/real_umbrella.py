import json
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from sklearn.linear_model import LinearRegression
import joblib

# ========== НАСТРОЙКИ ==========
API_KEY = Variable.get("weather_api_key", default_var="8c436c2106bf40599dd104558262803")
CITY = "Dubai"
# Дату можно задать через Variable или захардкодить для теста
START_DATE = Variable.get("weather_start_date", default_var="2024-07-15")  # Формат: YYYY-MM-DD
DAYS_COUNT = 3  # Сколько дней подряд запрашиваем
TEMPERATURE_THRESHOLD = 30

DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_weather_3days(**kwargs):
    """Загрузка погодных данных за 3 дня, начиная с START_DATE."""
    from datetime import datetime, timedelta
    
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    all_records = []
    
    for i in range(DAYS_COUNT):
        current_date = (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
        url = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={CITY}&dt={current_date}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Извлекаем данные за день (max temperature)
            day_data = data['forecast']['forecastday'][0]['day']
            record = {
                'date': current_date,
                'temp_c': day_data['maxtemp_c'],
                'temp_f': day_data['maxtemp_f'],
                'condition': day_data['condition']['text'],
                'avgtemp_c': day_data['avgtemp_c'],
                'mintemp_c': day_data['mintemp_c'],
                'maxtemp_c': day_data['maxtemp_c'],
                'avghumidity': day_data['avghumidity'],
                'daily_chance_of_rain': day_data['daily_chance_of_rain']
            }
            all_records.append(record)
            print(f"✓ Получены данные за {current_date}: {day_data['maxtemp_c']}°C")
            
        except Exception as e:
            print(f"✗ Ошибка при запросе за {current_date}: {e}")
            continue
    
    if not all_records:
        raise ValueError("Не удалось получить данные ни за один день")
    
    df = pd.DataFrame(all_records)
    
    # Сохраняем сырые данные
    raw_path = f"{DATA_DIR}/dubai_3days_{START_DATE}.csv"
    df.to_csv(raw_path, index=False)
    print(f"📁 Данные сохранены в {raw_path}")
    
    # Фильтрация: дни с температурой >30°C
    hot_days = df[df['temp_c'] > TEMPERATURE_THRESHOLD].copy()
    count_hot = len(hot_days)
    
    # Сохраняем результаты фильтрации
    with open(f"{DATA_DIR}/hot_days_count.txt", 'w') as f:
        f.write(str(count_hot))
    
    with open(f"{DATA_DIR}/hot_days_list.txt", 'w', encoding='utf-8') as f:
        if count_hot == 0:
            f.write(f"Нет дней с температурой > {TEMPERATURE_THRESHOLD}°C\n")
        else:
            for _, row in hot_days.iterrows():
                f.write(f"{row['date']}: {row['temp_c']}°C — {row['condition']}\n")
    
    # XCom для передачи в следующие задачи
    ti = kwargs['ti']
    ti.xcom_push(key='hot_days_count', value=count_hot)
    ti.xcom_push(key='data_path', value=raw_path)
    
    print(f"🔥 Дней с температурой >{TEMPERATURE_THRESHOLD}°C: {count_hot}")
    return df.to_dict('records')


def train_model(**kwargs):
    """Обучение простой модели на синтетических данных."""
    import numpy as np
    np.random.seed(42)
    
    # Синтетические данные: температура → продажи зонтов
    temperatures = np.linspace(15, 40, 100)
    sales = 100 - 2.5 * (temperatures - 15) + np.random.normal(0, 3, size=100)
    
    X = temperatures.reshape(-1, 1)
    y = sales
    
    model = LinearRegression()
    model.fit(X, y)
    
    model_path = f"{DATA_DIR}/umbrella_model.pkl"
    joblib.dump(model, model_path)
    print(f"🤖 Модель сохранена в {model_path}")
    
    # Сохраняем метаданные модели
    with open(f"{DATA_DIR}/model_info.txt", 'w') as f:
        f.write(f"Model trained: {datetime.now()}\n")
        f.write(f"R² score: {model.score(X, y):.4f}\n")
        f.write(f"Coefficient: {model.coef_[0]:.2f}\n")


def save_summary(**kwargs):
    """Сохранение краткой сводки."""
    ti = kwargs['ti']
    count = ti.xcom_pull(key='hot_days_count', task_ids='fetch_weather_3days')
    data_path = ti.xcom_pull(key='data_path', task_ids='fetch_weather_3days')
    
    summary = {
        'start_date': START_DATE,
        'days_requested': DAYS_COUNT,
        'hot_days_count': count,
        'threshold': TEMPERATURE_THRESHOLD,
        'data_file': data_path
    }
    
    with open(f"{DATA_DIR}/summary.json", 'w') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"📋 Сводка: {summary}")


# ========== DAG ==========
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='variant_16_dubai_3days',
    default_args=default_args,
    description='Погода в Дубае: 3 дня начиная с заданной даты, фильтрация >30°C',
    schedule_interval=None,  # Запуск вручную
    catchup=False,
    tags=['weather', 'dubai', 'etl'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    fetch_task = PythonOperator(
        task_id='fetch_weather_3days',
        python_callable=fetch_weather_3days,
        provide_context=True,
    )
    
    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        provide_context=True,
    )
    
    summary_task = PythonOperator(
        task_id='save_summary',
        python_callable=save_summary,
        provide_context=True,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> fetch_task >> train_task >> summary_task >> end
