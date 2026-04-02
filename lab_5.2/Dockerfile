# ============================================
# Dockerfile для ETL Workshop 2026 (Упрощённый)
# Вариант 16: Rocket Launch Analytics
# ============================================

FROM apache/airflow:2.8.1-python3.11

# ============================================
# МЕТАДАННЫЕ
# ============================================
LABEL maintainer="Tyapkina P.A."
LABEL version="1.0"
LABEL description="ETL Pipeline with ML and Streamlit for Variant 16"

# ============================================
# ПЕРЕКЛЮЧЕНИЕ НА ROOT
# ============================================
USER root

# ============================================
# ИСПРАВЛЕНИЕ ВРЕМЕНИ И РЕПОЗИТОРИЕВ
# ============================================
# Синхронизация времени (решает проблему "not valid yet")
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ntpdate \
    ca-certificates \
    && ntpdate -u pool.ntp.org || true \
    && rm -rf /var/lib/apt/lists/*

# Удаление проблемных источников пакетов из базового образа
RUN rm -f /etc/apt/sources.list.d/microsoft-prod.list \
    /etc/apt/sources.list.d/pgdg.list \
    /etc/apt/sources.list.d/mariadb.list 2>/dev/null || true

# ============================================
# МИНИМАЛЬНЫЕ СИСТЕМНЫЕ ПАКЕТЫ
# ============================================
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    # Для компиляции
    build-essential \
    gcc \
    # Для изображений (Pillow)
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    # Для сетевых проверок
    iputils-ping \
    dnsutils \
    # Утилиты
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# ============================================
# ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# ============================================
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# ============================================
# УСТАНОВКА PYTHON ПАКЕТОВ
# ============================================
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Установка зависимостей (без точных версий для гибкости)
RUN pip install --no-cache-dir \
    apache-airflow-providers-http \
    apache-airflow-providers-postgres \
    requests \
    pandas \
    numpy \
    pillow \
    torch \
    torchvision \
    transformers \
    streamlit \
    jupyter \
    notebook \
    dnspython \
    python-dotenv \
    pyyaml \
    matplotlib

# ============================================
# НАСТРОЙКА ПРАВ И ДИРЕКТОРИЙ
# ============================================
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app /opt/airflow/ml \
    && chown -R airflow:airflow /opt/airflow \
    && chmod -R 775 /opt/airflow/data \
    && chmod -R 775 /opt/airflow/logs \
    && chmod -R 775 /opt/airflow/app

# ============================================
# ПЕРЕКЛЮЧЕНИЕ НА AIRFLOW USER
# ============================================
USER airflow

# ============================================
# РАБОЧАЯ ДИРЕКТОРИЯ
# ============================================
WORKDIR /opt/airflow

# ============================================
# КОПИРОВАНИЕ ФАЙЛОВ ПРОЕКТА
# ============================================
COPY --chown=airflow:airflow app/ /opt/airflow/app/
COPY --chown=airflow:airflow ml.ipynb /opt/airflow/ml/

# ============================================
# ПОРТЫ
# ============================================
EXPOSE 8080 8501 8888

# ============================================
# HEALTH CHECK
# ============================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import requests, pandas, torch; print('OK')" || exit 1

# ============================================
# ENTRYPOINT
# ============================================
ENTRYPOINT ["airflow"]
CMD ["webserver"]
