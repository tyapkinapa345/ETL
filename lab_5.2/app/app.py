# app/app.py
# Streamlit Dashboard для Варианта 16
# "Всё что есть — всё на показ"

import streamlit as st
import pandas as pd
import json
import os
from PIL import Image
from datetime import datetime

# ============================================
# 1. НАСТРОЙКИ СТРАНИЦЫ
# ============================================
st.set_page_config(
    page_title="Rocket Analytics | Variant 16",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# 2. ПУТИ К ФАЙЛАМ
# ============================================
DATA_DIR = "/opt/airflow/data"
LOGS_DIR = "/opt/airflow/logs"

FILES = {
    "launches": f"{DATA_DIR}/launches.json",
    "predictions": f"{DATA_DIR}/ml_predictions.csv",
    "images_dir": f"{DATA_DIR}/images",
    "domain_report": f"{DATA_DIR}/domain_counts_report.txt",
    "server_status": f"{DATA_DIR}/server_status.txt",
    "server_ping": f"{DATA_DIR}/server_ping_detailed.txt",
    "error_log_json": f"{LOGS_DIR}/error_log.json",
    "error_log_txt": f"{LOGS_DIR}/error_log.txt"
}

# ============================================
# 3. ЗАГОЛОВОК И САЙДБАР
# ============================================
st.title("🚀 Rocket Launch Analytics")
st.markdown("### Вариант 16: Domain Analysis + Server Ping/Head + Error Logging")
st.caption(f"Последнее обновление: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

st.markdown("---")

# Сайдбар с навигацией
with st.sidebar:
    st.header("📂 Доступные файлы")
    
    file_status = {}
    for name, path in FILES.items():
        if name == "images_dir":
            exists = os.path.isdir(path) and len(os.listdir(path)) > 0 if os.path.isdir(path) else False
        else:
            exists = os.path.exists(path)
        file_status[name] = exists
        status_icon = "✅" if exists else "❌"
        st.write(f"{status_icon} {name}")
    
    st.markdown("---")
    st.info("💡 Запустите DAG в Airflow для генерации данных")
    st.markdown("**Порядок действий:**")
    st.markdown("1. Airflow: http://localhost:8080")
    st.markdown("2. Запустить DAG `listing_TyapkinaPA_Rocket`")
    st.markdown("3. Jupyter: http://localhost:8888 → ml.ipynb")
    st.markdown("4. Обновить эту страницу")

st.markdown("---")


# ============================================
# 4. СЕКЦИЯ 1: Статистика по доменам (Задание 1)
# ============================================
st.header("📊 1. Статистика скачивания по доменам")
st.markdown("*Задание 1: Отчёт — число скачанных с конкретных доменов*")

if file_status["domain_report"]:
    # Переключатель режима просмотра
    view_mode = st.radio(
        "Режим отображения:",
        ["📊 Визуализация", "📄 Исходный файл"],
        horizontal=True,
        key="domain_view_mode"
    )
    
    # Чтение файла
    with open(FILES["domain_report"], "r", encoding="utf-8") as f:
        domain_content = f.read()
    
    if view_mode == "📊 Визуализация":
        # Парсинг данных для таблицы
        domain_data = []
        for line in domain_content.split("\n"):
            line = line.strip()
            if line and not line.startswith("=") and not line.startswith("-") and not line.startswith("Generated") and not line.startswith("Pipeline") and not line.startswith("Total"):
                parts = line.split()
                if len(parts) >= 2 and parts[-1].isdigit():
                    count = int(parts[-1])
                    domain = " ".join(parts[:-1])
                    if domain and domain != "Domain" and count > 0:
                        domain_data.append({"Domain": domain, "Count": count})
        
        if domain_data:
            df_domains = pd.DataFrame(domain_data)
            
            # Метрики
            col1, col2, col3 = st.columns(3)
            col1.metric("Всего доменов", len(df_domains))
            col2.metric("Всего запросов", df_domains["Count"].sum())
            col3.metric("Среднее на домен", f"{df_domains['Count'].mean():.1f}")
            
            # График
            st.subheader("📈 Распределение по доменам")
            st.bar_chart(df_domains.set_index("Domain")["Count"])
            
            # Таблица
            st.subheader("📋 Таблица")
            st.dataframe(df_domains, use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ Не удалось распарсить данные из файла")
    
    # Исходный файл
    with st.expander("📂 Открыть исходный файл domain_counts_report.txt", expanded=False):
        st.code(domain_content, language="text", line_numbers=True)
        st.download_button(
            label="⬇️ Скачать файл",
            data=domain_content,
            file_name="domain_counts_report.txt",
            mime="text/plain"
        )
else:
    st.warning("⚠️ Файл не найден. Запустите DAG в Airflow для генерации отчёта.")
    st.code(f"Ожидаемый путь: {FILES['domain_report']}", language="text")

st.markdown("---")


# ============================================
# 5. СЕКЦИЯ 2: Статус серверов (Задание 2 - Кратко)
# ============================================
st.header("🔍 2. Проверка доступности серверов (Краткий статус)")
st.markdown("*Задание 2: Проверка доступности серверов (Ping/Head)*")

if file_status["server_status"]:
    view_mode = st.radio(
        "Режим отображения:",
        ["📊 Таблица", "📄 Исходный файл"],
        horizontal=True,
        key="server_view_mode"
    )
    
    with open(FILES["server_status"], "r", encoding="utf-8") as f:
        server_content = f.read()
    
    if view_mode == "📊 Таблица":
        # Парсинг для таблицы
        servers = []
        for line in server_content.split("\n"):
            line = line.strip()
            if line.startswith("[✅") or line.startswith("[❌"):
                status = "✅ ONLINE" if "[✅" in line else "❌ OFFLINE"
                domain = line.split("]")[1].strip() if "]" in line else "unknown"
                servers.append({"Domain": domain, "Status": status})
        
        if servers:
            df_servers = pd.DataFrame(servers)
            
            # Цветовое кодирование
            def color_status(val):
                if pd.isna(val):
                    return ""
                return 'color: #2e7d32; font-weight: bold; background-color: #e8f5e9' if '✅' in str(val) else 'color: #c62828; font-weight: bold; background-color: #ffebee'
            
            st.dataframe(
                df_servers.style.map(color_status, subset=['Status']),
                use_container_width=True,
                hide_index=True
            )
            
            # Метрики
            online = len(df_servers[df_servers["Status"].str.contains("✅", na=False)])
            total = len(df_servers)
            col1, col2, col3 = st.columns(3)
            col1.metric("✅ Онлайн", online)
            col2.metric("❌ Офлайн", total - online)
            col3.metric("Uptime", f"{online/total*100:.1f}%" if total > 0 else "N/A")
        else:
            st.info("ℹ️ Данные в файле не распознаны")
    
    # Исходный файл
    with st.expander("📂 Открыть исходный файл server_status.txt", expanded=False):
        st.code(server_content, language="text", line_numbers=True)
        st.download_button(
            label="⬇️ Скачать файл",
            data=server_content,
            file_name="server_status.txt",
            mime="text/plain"
        )
else:
    st.warning("⚠️ Файл не найден. Запустите DAG в Airflow.")
    st.code(f"Ожидаемый путь: {FILES['server_status']}", language="text")

st.markdown("---")


# ============================================
# 6. СЕКЦИЯ 3: Детальные данные Ping/Head (Задание 2 - Полные)
# ============================================
st.header("📶 3. Детальные данные Ping/Head проверок")
st.markdown("*ПОЛНАЯ информация: DNS, Ping, HTTP Headers, Redirects, Download Status*")

if file_status["server_ping"]:
    with open(FILES["server_ping"], "r", encoding="utf-8") as f:
        ping_content = f.read()
    
    # Информационный блок
    st.info("📋 Этот файл содержит максимальную диагностическую информацию по каждой проверке сервера")
    
    # Переключатели отображения
    col1, col2, col3 = st.columns(3)
    with col1:
        show_summary = st.checkbox("📊 Сводка", value=True, key="ping_summary")
    with col2:
        show_details = st.checkbox("🔍 По проверкам", value=True, key="ping_details")
    with col3:
        show_raw = st.checkbox("📄 Весь файл", value=False, key="ping_raw")
    
    # Сводка (из конца файла)
    if show_summary and "SUMMARY STATISTICS" in ping_content:
        st.subheader("📈 Сводная статистика")
        summary_part = ping_content.split("SUMMARY STATISTICS")[1] if "SUMMARY STATISTICS" in ping_content else ""
        if summary_part:
            st.code(summary_part[:1500], language="text", line_numbers=False)
    
    # Детали по проверкам
    if show_details:
        st.subheader("🔍 Детали отдельных проверок")
        
        # Разбиваем на проверки
        checks = ping_content.split("CHECK #")[1:]
        
        # Фильтры
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            show_test = st.checkbox("Показывать тестовые сценарии [TEST]", value=True, key="show_test_checks")
        with col_f2:
            max_checks = st.slider("Максимум проверок для отображения", 1, max(len(checks), 1), min(5, len(checks)) if checks else 1, key="max_checks_slider")
        
        if checks:
            displayed = 0
            for i, check in enumerate(checks):
                if displayed >= max_checks:
                    break
                
                is_test = "[TEST]" in check
                
                if not show_test and is_test:
                    continue
                
                displayed += 1
                
                # Извлекаем заголовок проверки
                first_line = check.split("\n")[0] if check.split("\n") else "Check"
                task_info = first_line.split("|")[1].strip() if "|" in first_line else ""
                
                # Expander для каждой проверки
                with st.expander(f"{'🧪 TEST' if is_test else '✅ PROD'} | {task_info}", expanded=False):
                    # Показываем первые 40 строк детали
                    lines = check.split("\n")[:40]
                    for line in lines:
                        if line.strip():
                            st.text(line)
                    if len(check.split("\n")) > 40:
                        st.info(f"... ещё {len(check.split('\n')) - 40} строк (см. полный файл)")
            
            if displayed == 0:
                st.info("ℹ️ Нет проверок для отображения с текущими фильтрами")
        else:
            st.warning("⚠️ Не удалось распарсить проверки из файла")
    
    # Полный файл
    if show_raw:
        with st.expander("📂 Исходный файл server_ping_detailed.txt", expanded=True):
            st.code(ping_content, language="text", line_numbers=True)
            st.download_button(
                label="⬇️ Скачать файл",
                data=ping_content,
                file_name="server_ping_detailed.txt",
                mime="text/plain"
            )
else:
    st.warning("⚠️ Файл не найден. Запустите DAG в Airflow для генерации полных данных.")
    st.code(f"Ожидаемый путь: {FILES['server_ping']}", language="text")

st.markdown("---")


# ============================================
# 7. СЕКЦИЯ 4: Лог ошибок (Задание 3)
# ============================================
st.header("🐛 4. Лог ошибок для анализа")
st.markdown("*Задание 3: Логирование ошибок для будущего анализа*")

if file_status["error_log_json"]:
    # Загрузка JSON
    with open(FILES["error_log_json"], "r", encoding="utf-8") as f:
        try:
            error_data = json.load(f)
        except json.JSONDecodeError:
            st.error("❌ Ошибка чтения JSON файла")
            error_data = {"errors": [], "summary": {}}
    
    errors = error_data.get("errors", [])
    
    # Преобразуем в DataFrame, если есть ошибки
    if errors:
        df_errors = pd.DataFrame(errors)
        
        # Нормализуем колонки: создаём error_type из error или error_message
        if 'error_type' not in df_errors.columns:
            if 'error' in df_errors.columns:
                df_errors['error_type'] = df_errors['error'].apply(
                    lambda x: x.split(':')[0] if isinstance(x, str) else 'Unknown'
                )
            elif 'error_message' in df_errors.columns:
                df_errors['error_type'] = df_errors['error_message'].apply(
                    lambda x: x.split(':')[0] if isinstance(x, str) else 'Unknown'
                )
            else:
                df_errors['error_type'] = 'Unknown'
        
        # Убедимся, что есть error_message
        if 'error_message' not in df_errors.columns and 'error' in df_errors.columns:
            df_errors['error_message'] = df_errors['error']
        
        # Заполним недостающие колонки значениями по умолчанию
        for col in ['task_id', 'url', 'is_test_scenario']:
            if col not in df_errors.columns:
                df_errors[col] = 'unknown' if col != 'is_test_scenario' else False
    else:
        df_errors = pd.DataFrame()
    
    # Сводные метрики
    st.subheader("📈 Сводка по ошибкам")
    col1, col2, col3, col4 = st.columns(4)
    total_errors = len(df_errors)
    test_errors = df_errors['is_test_scenario'].sum() if not df_errors.empty else 0
    col1.metric("Всего записей", total_errors)
    col2.metric("🧪 Тестовые", test_errors)
    col3.metric("⚠️ Продакшен", total_errors - test_errors)
    
    if not df_errors.empty:
        by_type = df_errors['error_type'].value_counts().to_dict()
        col4.metric("Типов ошибок", len(by_type))
    else:
        col4.metric("Типов ошибок", 0)
    
    # Таблица ошибок
    if not df_errors.empty:
        st.subheader("📋 Таблица ошибок")
        
        # Добавляем визуальный маркер
        df_errors["Тип"] = df_errors.apply(
            lambda x: "🧪 TEST" if x.get("is_test_scenario") else "⚠️ PROD",
            axis=1
        )
        
        # Фильтры
        with st.expander("🔍 Фильтры и поиск", expanded=False):
            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                filter_show_test = st.checkbox("Показывать тестовые ошибки", value=True, key="filter_test_errors")
            with col_f2:
                error_types = df_errors["error_type"].unique().tolist()
                filter_type = st.multiselect("Тип ошибки", error_types, default=error_types)
            with col_f3:
                filter_search = st.text_input("🔍 Поиск по сообщению об ошибке")
        
        # Применение фильтров
        filtered_df = df_errors.copy()
        if not filter_show_test:
            filtered_df = filtered_df[~filtered_df["is_test_scenario"]]
        if filter_type:
            filtered_df = filtered_df[filtered_df["error_type"].isin(filter_type)]
        if filter_search:
            filtered_df = filtered_df[filtered_df["error_message"].str.contains(filter_search, case=False, na=True)]
        
        # Отображение таблицы
        display_cols = ["Тип", "timestamp", "task_id", "error_type", "error_message", "url"]
        available_cols = [c for c in display_cols if c in filtered_df.columns]
        if available_cols:
            st.dataframe(
                filtered_df[available_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "url": st.column_config.LinkColumn("URL", max_width=200),
                    "error_message": st.column_config.TextColumn("Сообщение", width="large")
                }
            )
        
        # График
        if not filtered_df.empty:
            st.subheader("📊 Распределение по типам ошибок")
            st.bar_chart(filtered_df["error_type"].value_counts())
    else:
        st.success("✅ Ошибок не обнаружено в этом запуске!")
    
    # Исходные файлы
    st.subheader("📂 Исходные файлы логов")
    with st.expander("📄 error_log.json", expanded=False):
        with open(FILES["error_log_json"], "r", encoding="utf-8") as f:
            json_content = f.read()
        try:
            st.json(json.loads(json_content), expanded=False)
        except:
            st.code(json_content, language="json")
        st.download_button(label="⬇️ Скачать JSON", data=json_content, file_name="error_log.json", mime="application/json")
    
    if file_status["error_log_txt"]:
        with st.expander("📝 error_log.txt", expanded=False):
            with open(FILES["error_log_txt"], "r", encoding="utf-8") as f:
                txt_content = f.read()
            st.code(txt_content, language="text", line_numbers=True)
            st.download_button(label="⬇️ Скачать TXT", data=txt_content, file_name="error_log.txt", mime="text/plain")
else:
    st.warning("⚠️ Файл не найден. Запустите DAG в Airflow для генерации логов.")
    st.code(f"Ожидаемый путь: {FILES['error_log_json']}", language="text")

st.markdown("---")


# ============================================
# 8. СЕКЦИЯ 5: Основные данные запусков
# ============================================
st.header("🛰️ 5. Данные о запусках (из API)")

if file_status["launches"]:
    with open(FILES["launches"], "r", encoding="utf-8") as f:
        try:
            launches_data = json.load(f)
            launches = launches_data.get("results", [])
        except json.JSONDecodeError:
            launches = []
    
    if launches:
        # Формирование таблицы
        launch_rows = []
        for launch in launches:
            row = {
                "Миссия": launch.get("name", "N/A"),
                "Статус": launch.get("status", {}).get("name", "N/A") if isinstance(launch.get("status"), dict) else "N/A",
                "Дата запуска": launch.get("window_start", "")[:19] if launch.get("window_start") else "N/A",
                "Провайдер": launch.get("launch_service_provider", {}).get("name", "N/A") if isinstance(launch.get("launch_service_provider"), dict) else "N/A",
                "Ракета": launch.get("rocket", {}).get("name", "N/A") if isinstance(launch.get("rocket"), dict) else "N/A"
            }
            launch_rows.append(row)
        
        df_launches = pd.DataFrame(launch_rows)
        st.dataframe(df_launches, use_container_width=True, hide_index=True)
        
        st.info(f"📊 Всего запусков: {len(launches)}")
    else:
        st.warning("⚠️ Нет данных о запусках в файле")
else:
    st.warning("⚠️ Файл не найден. Запустите DAG в Airflow.")
    st.code(f"Ожидаемый путь: {FILES['launches']}", language="text")

st.markdown("---")


# ============================================
# 9. СЕКЦИЯ 6: ML-результаты и галерея
# ============================================
st.header("🤖 6. ML-распознавание ракет")
st.markdown("*Результаты классификации изображений моделью CLIP*")

if file_status["predictions"]:
    try:
        df_predictions = pd.read_csv(FILES["predictions"])
        
        if not df_predictions.empty:
            # Таблица предсказаний
            st.subheader("📋 Таблица предсказаний")
            st.dataframe(df_predictions, use_container_width=True, hide_index=True)
            
            # Статистика
            col1, col2 = st.columns(2)
            
            if "predicted_rocket" in df_predictions.columns:
                with col1:
                    st.subheader("📊 Распределение по типам ракет")
                    rocket_counts = df_predictions["predicted_rocket"].value_counts()
                    st.bar_chart(rocket_counts)
            
            if "confidence" in df_predictions.columns:
                with col2:
                    st.subheader("📈 Статистика уверенности")
                    st.metric("Средняя уверенность", f"{df_predictions['confidence'].mean():.1f}%")
                    st.metric("Минимальная", f"{df_predictions['confidence'].min():.1f}%")
                    st.metric("Максимальная", f"{df_predictions['confidence'].max():.1f}%")
            
            # Галерея изображений
            if file_status["images_dir"]:
                st.subheader("🖼️ Галерея распознанных изображений")
                
                images = os.listdir(FILES["images_dir"])
                images = [img for img in images if img.lower().endswith(('.jpg', '.png', '.jpeg'))]
                
                if images:
                    cols = st.columns(3)
                    for idx, img_name in enumerate(images[:12]):  # Максимум 12 изображений
                        img_path = os.path.join(FILES["images_dir"], img_name)
                        
                        # Находим предсказание для этого изображения
                        pred_row = df_predictions[df_predictions["image_name"] == img_name]
                        pred_text = ""
                        if not pred_row.empty:
                            pred_text = f"{pred_row.iloc[0].get('predicted_rocket', 'N/A')} ({pred_row.iloc[0].get('confidence', 0):.1f}%)"
                        
                        with cols[idx % 3]:
                            try:
                                img = Image.open(img_path)
                                st.image(img, caption=pred_text if pred_text else img_name, use_container_width=True)
                            except Exception as e:
                                st.error(f"❌ Ошибка загрузки: {img_name}")
                else:
                    st.info("ℹ️ Изображения не найдены в папке")
        else:
            st.warning("⚠️ Файл предсказаний пуст")
    except Exception as e:
        st.error(f"❌ Ошибка чтения файла предсказаний: {str(e)}")
else:
    st.warning("⚠️ Файл не найден. Запустите ml.ipynb в Jupyter для генерации предсказаний.")
    st.code(f"Ожидаемый путь: {FILES['predictions']}", language="text")

st.markdown("---")


# ============================================
# 10. ФУТЕР
# ============================================
st.markdown("---")

col_f1, col_f2, col_f3 = st.columns(3)

with col_f1:
    st.markdown("**👤 Автор**")
    st.caption("Tyapkina P.A.")

with col_f2:
    st.markdown("**📚 Курс**")
    st.caption("ETL Workshop 2026")

with col_f3:
    st.markdown("**🎯 Вариант**")
    st.caption("Вариант 16")

st.markdown("---")
st.caption("🚀 Всё что есть — всё на показ | Данные обновляются автоматически при изменении файлов")
