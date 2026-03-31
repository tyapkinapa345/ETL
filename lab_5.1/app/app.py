import streamlit as st
import pandas as pd
import os
from datetime import datetime

# Настройки страницы
st.set_page_config(page_title="Weather ETL Dashboard", layout="wide")
st.title("🌤️ Погода в Дубае: 3-дневный прогноз/история")

DATA_DIR = "/opt/airflow/data"  # Путь внутри контейнера
# Для локального запуска укажите актуальный путь:
# DATA_DIR = "./data"

# Боковая панель
st.sidebar.header("⚙️ Настройки")
selected_file = st.sidebar.selectbox(
    "Выберите файл с данными:",
    [f for f in os.listdir(DATA_DIR) if f.startswith("dubai_3days_") and f.endswith(".csv")]
    if os.path.exists(DATA_DIR) else ["Нет данных"]
)

if selected_file != "Нет данных" and os.path.exists(os.path.join(DATA_DIR, selected_file)):
    # Загрузка данных
    df = pd.read_csv(os.path.join(DATA_DIR, selected_file))
    
    # Отображение таблицы
    st.subheader("📊 Данные за период")
    st.dataframe(df.style.format({
        'temp_c': '{:.1f}°C',
        'avgtemp_c': '{:.1f}°C',
        'mintemp_c': '{:.1f}°C',
        'maxtemp_c': '{:.1f}°C'
    }), use_container_width=True)
    
    # Метрики
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Средняя температура", f"{df['temp_c'].mean():.1f}°C")
    with col2:
        st.metric("Макс. температура", f"{df['temp_c'].max():.1f}°C")
    with col3:
        hot_count = len(df[df['temp_c'] > 30])
        st.metric("Дней >30°C", f"{hot_count} из {len(df)}")
    
    # График
    st.subheader("📈 График температуры")
    chart_df = df[['date', 'temp_c', 'avgtemp_c', 'mintemp_c', 'maxtemp_c']].copy()
    chart_df['date'] = pd.to_datetime(chart_df['date'])
    st.line_chart(chart_df.set_index('date')[['temp_c', 'avgtemp_c']])
    
    # Фильтр жарких дней
    st.subheader(f"🔥 Дни с температурой >30°C")
    hot_days = df[df['temp_c'] > 30]
    if not hot_days.empty:
        st.dataframe(hot_days[['date', 'temp_c', 'condition']].style.format({'temp_c': '{:.1f}°C'}))
    else:
        st.info("Нет дней с температурой выше 30°C в выбранном периоде")
    
    # Информация о модели
    model_info_path = os.path.join(DATA_DIR, "model_info.txt")
    if os.path.exists(model_info_path):
        with st.expander("🤖 Информация о модели"):
            with open(model_info_path, 'r') as f:
                st.text(f.read())
else:
    st.warning("⚠️ Файлы с данными не найдены. Запустите DAG `variant_16_dubai_3days` в Airflow.")

# Футер
st.markdown("---")
st.caption(f"Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M')} | ETL Pipeline with Airflow + Streamlit")
