# app/app.py
import streamlit as st
import pandas as pd
import json
import os
from PIL import Image
from datetime import datetime
from urllib.parse import urlparse

st.set_page_config(page_title="Rocket Launch Analytics - Variant 16", layout="wide")

# Пути к данным
DATA_DIR = "/opt/airflow/data"
LOGS_DIR = "/opt/airflow/logs"
JSON_FILE = f"{DATA_DIR}/launches.json"
PREDICTIONS_FILE = f"{DATA_DIR}/ml_predictions.csv"
IMAGES_DIR = f"{DATA_DIR}/images"
DOMAIN_REPORT = f"{DATA_DIR}/domain_counts_report.txt"
SERVER_STATUS_FILE = f"{DATA_DIR}/server_status.txt"
ERROR_LOG_JSON = f"{LOGS_DIR}/error_log.json"

st.title("🚀 Rocket Launch Analytics | Вариант 16")
st.markdown("*ETL + Domain Analysis + Server Health + Error Logging*")

# === Секция 1: Статистика по доменам (Задание 1) ===
st.header("📊 1. Статистика скачивания по доменам")
if os.path.exists(DOMAIN_REPORT):
    with open(DOMAIN_REPORT, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Парсинг для визуализации
    lines = content.split("\n")
    domain_data = []
    for line in lines:
        if ":" in line and "image(s)" in line:
            parts = line.split(":")
            if len(parts) == 2:
                domain = parts[0].strip()
                count = int(parts[1].strip().split()[0])
                domain_data.append({"Domain": domain, "Count": count})
    
    if domain_data:
        df_domains = pd.DataFrame(domain_data)
        col1, col2 = st.columns([2, 1])
        with col1:
            st.bar_chart(df_domains.set_index("Domain"))
        with col2:
            st.metric("Всего доменов", len(df_domains))
            st.metric("Всего изображений", df_domains["Count"].sum())
        st.dataframe(df_domains, use_container_width=True)
    st.code(content, language="text")
else:
    st.warning("⚠️ Отчёт по доменам ещё не сгенерирован. Запустите DAG в Airflow.")

st.markdown("---")

# === Секция 2: Статус серверов (Задание 2) ===
st.header("🔍 2. Проверка доступности серверов")
if os.path.exists(SERVER_STATUS_FILE):
    with open(SERVER_STATUS_FILE, "r", encoding="utf-8") as f:
        status_content = f.read()
    
    # Парсинг для таблицы
    servers = []
    current = {}
    for line in status_content.split("\n"):
        if line.strip().startswith("=== ") or not line.strip():
            continue
        if ": ✅" in line or ": ❌" in line:
            if current:
                servers.append(current)
            parts = line.split(":")
            domain = parts[0].strip()
            status = "✅ OK" if "✅" in line else "❌ FAILED"
            current = {"Domain": domain, "Status": status}
        elif "Status Code:" in line:
            current["Status Code"] = line.split(":")[1].strip()
        elif "Error:" in line:
            current["Error"] = line.split(":", 1)[1].strip()
        elif "Checked:" in line:
            current["Checked"] = line.split(":", 1)[1].strip()
    if current:
        servers.append(current)
    
    if servers:
        df_servers = pd.DataFrame(servers)
        # Цветовое кодирование статуса
        def color_status(val):
            return 'color: green' if val == '✅ OK' else 'color: red'
        
        st.dataframe(
            df_servers.style.map(color_status, subset=['Status']),
            use_container_width=True
        )
        
        # Метрики
        ok_count = len(df_servers[df_servers["Status"] == "✅ OK"])
        col1, col2, col3 = st.columns(3)
        col1.metric("Доступных серверов", ok_count)
        col2.metric("Недоступных", len(df_servers) - ok_count)
        col3.metric("Общий охват", f"{ok_count/len(df_servers)*100:.1f}%")
    
    with st.expander("📄 Исходный отчёт"):
        st.code(status_content, language="text")
else:
    st.warning("⚠️ Проверка серверов ещё не выполнена.")

st.markdown("---")

# === Секция 3: Лог ошибок (Задание 3) ===
st.header("🐛 3. Лог ошибок для анализа")
if os.path.exists(ERROR_LOG_JSON):
    with open(ERROR_LOG_JSON, "r", encoding="utf-8") as f:
        error_data = json.load(f)
    
    errors = error_data.get("errors", [])
    
    if errors:
        st.error(f"⚠️ Обнаружено ошибок: {len(errors)}")
        df_errors = pd.DataFrame(errors)
        
        # Фильтры
        col1, col2 = st.columns(2)
        with col1:
            task_filter = st.multiselect("Фильтр по задаче", df_errors["task"].unique(), default=df_errors["task"].unique())
        with col2:
            search = st.text_input("🔍 Поиск по ошибке")
        
        filtered = df_errors[
            df_errors["task"].isin(task_filter) & 
            (df_errors["error"].str.contains(search, case=False, na=True) if search else True)
        ]
        
        st.dataframe(filtered, use_container_width=True)
        
        # Статистика по типам ошибок
        if not filtered.empty:
            st.subheader("📈 Распределение ошибок по задачам")
            error_counts = filtered["task"].value_counts()
            st.bar_chart(error_counts)
    else:
        st.success("✅ Ошибок не обнаружено! Все задачи выполнены успешно.")
    
    with st.expander("📋 JSON лог"):
        st.json(error_data)
else:
    st.info("ℹ️ Файл лога ошибок не найден. Ошибки появятся здесь после выполнения DAG.")

st.markdown("---")

# === Секция 4: Основные данные запусков ===
st.header("🛰️ 4. Ближайшие запуски (основные данные)")
if os.path.exists(JSON_FILE):
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        launches = json.load(f).get("results", [])
    
    if launches:
        df_launches = pd.DataFrame([{
            "Миссия": l.get("name"),
            "Статус": l.get("status", {}).get("name"),
            "Старт": l.get("window_start", "")[:19] if l.get("window_start") else "",
            "Провайдер": l.get("launch_service_provider", {}).get("name"),
            "Ракета": l.get("rocket", {}).get("name") if isinstance(l.get("rocket"), dict) else ""
        } for l in launches])
        st.dataframe(df_launches, use_container_width=True)
    else:
        st.warning("Нет данных о запусках.")
else:
    st.warning("⚠️ Запустите DAG для загрузки данных.")

st.markdown("---")

# === Секция 5: Результаты ML + Галерея ===
st.header("🤖 5. ML-распознавание ракет")
if os.path.exists(PREDICTIONS_FILE):
    df_preds = pd.read_csv(PREDICTIONS_FILE)
    st.dataframe(df_preds, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Типы ракет")
        if "predicted_rocket" in df_preds.columns:
            st.bar_chart(df_preds["predicted_rocket"].value_counts())
    
    with col2:
        st.subheader("Точность предсказаний")
        if "confidence" in df_preds.columns:
            st.metric("Средняя уверенность", f"{df_preds['confidence'].mean():.1f}%")
            st.metric("Минимальная", f"{df_preds['confidence'].min():.1f}%")
            st.metric("Максимальная", f"{df_preds['confidence'].max():.1f}%")
    
    # Галерея
    st.subheader("🖼️ Галерея распознанных изображений")
    if os.path.exists(IMAGES_DIR):
        cols = st.columns(3)
        for idx, row in df_preds.iterrows():
            img_name = row.get("image_name", "")
            if img_name:
                img_path = os.path.join(IMAGES_DIR, img_name)
                if os.path.exists(img_path):
                    with cols[idx % 3]:
                        try:
                            img = Image.open(img_path)
                            pred = row.get("predicted_rocket", "N/A")
                            conf = row.get("confidence", 0)
                            st.image(img, caption=f"{pred} ({conf:.1f}%)", use_container_width=True)
                        except Exception as e:
                            st.error(f"Ошибка загрузки: {img_name}")
else:
    st.info("ℹ️ Запустите ml.ipynb для генерации ML-предсказаний.")

# === Футер ===
st.markdown("---")
st.caption(f"Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Вариант 16 | Автор: Tyapkina P.A.")
