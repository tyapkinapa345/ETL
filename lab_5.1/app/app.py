import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os
import joblib

st.set_page_config(page_title="Прогноз погоды в Дубае | Вариант 16", layout="wide")
st.title("Анализ погоды в Дубае (прогноз на 3 дня)")

# Пути к файлам (в контейнере)
DATA_DIR = "/opt/airflow/data"
FORECAST_FILE = os.path.join(DATA_DIR, "dubai_forecast.csv")
HOT_COUNT_FILE = os.path.join(DATA_DIR, "dubai_hot_days_count.txt")
AVG_TEMP_FILE = os.path.join(DATA_DIR, "avg_temp.txt")
MODEL_FILE = os.path.join(DATA_DIR, "ml_model.pkl")

# Проверяем наличие основного файла с прогнозом
if not os.path.exists(FORECAST_FILE):
    st.warning("Данные прогноза еще не сгенерированы. Пожалуйста, запустите DAG 'variant_16' в Airflow.")
    st.stop()

# Загружаем данные
df = pd.read_csv(FORECAST_FILE)

# Отображаем таблицу
st.write("### Прогноз погоды на 3 дня")
st.dataframe(df)

# Выводим количество жарких дней (>30°C)
if os.path.exists(HOT_COUNT_FILE):
    with open(HOT_COUNT_FILE, 'r') as f:
        hot_days = int(f.read().strip())
    st.success(f"Количество дней с температурой >30°C: **{hot_days}**")
else:
    st.warning("Файл с количеством жарких дней не найден.")

# График температуры
st.write("### График температуры по датам")
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(df['date'], df['temp_c'], marker='o', color='orange', linewidth=2)
ax.set_xlabel('Дата')
ax.set_ylabel('Температура (°C)')
ax.grid(True, linestyle='--', alpha=0.7)
plt.xticks(rotation=45)
st.pyplot(fig)

# Прогноз продаж (если есть модель и средняя температура)
if os.path.exists(MODEL_FILE) and os.path.exists(AVG_TEMP_FILE):
    st.write("### Прогноз продаж зонтов")
    try:
        model = joblib.load(MODEL_FILE)
        with open(AVG_TEMP_FILE, 'r') as f:
            avg_temp = float(f.read().strip())
        input_data = pd.DataFrame({'temperature': [avg_temp]})
        prediction = model.predict(input_data)[0]
        st.info(f"Средняя температура за 3 дня: **{avg_temp:.1f}°C**")
        st.success(f"Прогнозируемый объём продаж: **{prediction:.2f}**")
    except Exception as e:
        st.error(f"Ошибка при загрузке модели или прогнозировании: {e}")
else:
    st.write("### Прогноз продаж")
    if not os.path.exists(MODEL_FILE):
        st.warning("Модель не найдена. Запустите DAG для обучения модели.")
    if not os.path.exists(AVG_TEMP_FILE):
        st.warning("Средняя температура не найдена. Запустите DAG для получения данных.")
