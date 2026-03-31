import streamlit as st
import pandas as pd
import json
import os
from PIL import Image
from datetime import datetime

st.set_page_config(page_title="Rocket Launch Analytics", layout="wide")

DATA_DIR = "/opt/airflow/data"
JSON_FILE = f"{DATA_DIR}/launches.json"
PREDICTIONS_FILE = f"{DATA_DIR}/ml_predictions.csv"
IMAGES_DIR = f"{DATA_DIR}/images"
DOMAIN_REPORT = f"{DATA_DIR}/domain_counts_report.txt"
ERROR_LOG = "/opt/airflow/logs/error_log.txt"   # путь внутри контейнера (проброшен в ./logs)

st.title("🚀 Аналитика космических запусков и ML распознавание")

# ========== Секция 1: Анализ расписания (из JSON) ==========
st.header("1. Ближайшие запуски (ETL Data)")
if os.path.exists(JSON_FILE):
    with open(JSON_FILE, "r") as f:
        launches = json.load(f).get("results", [])
    
    if launches:
        df_launches = pd.DataFrame([{
            "Имя миссии": l.get("name"),
            "Статус": l.get("status", {}).get("name"),
            "Окно старта": l.get("window_start"),
            "Провайдер": l.get("launch_service_provider", {}).get("name")
        } for l in launches])
        st.dataframe(df_launches)
        
        st.subheader("Запуски по провайдерам")
        provider_counts = df_launches["Провайдер"].value_counts()
        if not provider_counts.empty:
            st.bar_chart(provider_counts)
        else:
            st.info("Нет данных о провайдерах (поле launch_service_provider отсутствует в ответе API).")
    else:
        st.warning("Нет предстоящих запусков.")
else:
    st.warning("Файл launches.json еще не загружен. Запустите DAG в Airflow.")

st.markdown("---")

# ========== Секция 2: Результаты ML ==========
st.header("2. Распознавание типов ракет (ML Data)")
if os.path.exists(PREDICTIONS_FILE):
    df_preds = pd.read_csv(PREDICTIONS_FILE)
    st.dataframe(df_preds)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Статистика по типам ракет")
        rocket_counts = df_preds["predicted_rocket"].value_counts()
        if not rocket_counts.empty:
            st.bar_chart(rocket_counts)
        else:
            st.info("Нет предсказаний.")
        
    # Галерея
    st.subheader("Галерея распознанных ракет")
    cols = st.columns(3)
    for idx, row in df_preds.iterrows():
        img_path = os.path.join(IMAGES_DIR, row['image_name'])
        if os.path.exists(img_path):
            with cols[idx % 3]:
                img = Image.open(img_path)
                st.image(img, caption=f"{row['predicted_rocket']} ({row['confidence']}%)", use_column_width=True)
else:
    st.info("Результаты ML еще не готовы. Выполните ноутбук ml.ipynb для генерации прогнозов.")

st.markdown("---")

# ========== Секция 3: Отчёт по доменам (Задание 1) ==========
st.header("3. Отчёт по доменам (задание 1)")
if os.path.exists(DOMAIN_REPORT):
    with open(DOMAIN_REPORT, "r") as f:
        report_content = f.read()
    st.text(report_content)
else:
    st.warning("Отчёт по доменам ещё не сформирован. Запустите DAG.")

st.markdown("---")

# ========== Секция 4: Проверка доступности сервера (Задание 2) ==========
st.header("4. Проверка доступности сервера (задание 2)")
# Для этой секции можно отобразить результат последней проверки из лога Airflow,
# но проще показать статус на основе наличия ошибок в error_log.txt, если там была ошибка доступности.
# Альтернативно: добавить в DAG запись в отдельный файл о результате проверки.

# Создадим отдельный файл status.txt в data/, если он не существует.
# В DAG мы не сохраняли статус, поэтому покажем информацию из error_log.txt.

if os.path.exists(ERROR_LOG):
    with open(ERROR_LOG, "r") as f:
        error_content = f.read()
    if "Ошибка доступности" in error_content:
        st.error("❌ Последняя проверка: сервер недоступен (ошибка в логе).")
        with st.expander("Показать подробности"):
            st.text(error_content)
    else:
        st.success("✅ Сервер доступен (ошибок доступности не зафиксировано).")
else:
    st.info("Лог ошибок отсутствует. Возможно, DAG ещё не запускался.")

st.markdown("---")

# ========== Секция 5: Логирование ошибок (Задание 3) ==========
st.header("5. Логирование ошибок (задание 3)")
if os.path.exists(ERROR_LOG):
    with open(ERROR_LOG, "r") as f:
        errors = f.read()
    if errors.strip():
        st.warning("⚠️ Обнаружены ошибки в процессе работы:")
        st.text(errors)
    else:
        st.success("✅ Ошибок не зафиксировано.")
else:
    st.info("Файл error_log.txt отсутствует. Запустите DAG для генерации логов.")
