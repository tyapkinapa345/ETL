# ============================================
# Dockerfile для ETL Workshop 2026
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
# ПЕРЕКЛЮЧЕНИЕ НА ROOT ДЛЯ СИСТЕМНЫХ ПАКЕТОВ
# ============================================
USER root

# ============================================
# ИСПРАВЛЕНИЕ ВРЕМЕНИ
# ============================================
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ntpdate ca-certificates \
    && ntpdate -u pool.ntp.org || true \
    && rm -rf /var/lib/apt/lists/*

# Удаление проблемных репозиториев
RUN rm -f /etc/apt/sources.list.d/microsoft-prod.list \
    /etc/apt/sources.list.d/pgdg.list \
    /etc/apt/sources.list.d/mariadb.list 2>/dev/null || true

# ============================================
# СИСТЕМНЫЕ ЗАВИСИМОСТИ
# ============================================
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential gcc \
    libgl1-mesa-glx libglib2.0-0 libsm6 libxext6 libxrender-dev \
    iputils-ping dnsutils curl jq \
    && rm -rf /var/lib/apt/lists/* && apt-get clean

# ============================================
# ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# ============================================
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# ============================================
# ПЕРЕКЛЮЧЕНИЕ НА AIRFLOW USER (UID 50000)
# ============================================
USER 50000

# ============================================
# УСТАНОВКА PYTHON ПАКЕТОВ (как пользователь 50000)
# ============================================
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

RUN pip install --no-cache-dir \
    apache-airflow-providers-http \
    apache-airflow-providers-postgres \
    requests pandas numpy pillow \
    torch torchvision transformers \
    streamlit jupyter notebook \
    dnspython python-dotenv pyyaml matplotlib

# ============================================
# СОЗДАНИЕ ДИРЕКТОРИЙ (возвращаемся к root)
# ============================================
USER root

# Используем числовые ID: 50000 = airflow user, 0 = root group
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app /opt/airflow/ml \
    && chown -R 50000:0 /opt/airflow \
    && chmod -R 775 /opt/airflow/data \
    && chmod -R 775 /opt/airflow/logs \
    && chmod -R 775 /opt/airflow/app

# ============================================
# ФИНАЛЬНЫЙ USER И WORKDIR
# ============================================
USER 50000
WORKDIR /opt/airflow

# ============================================
# КОПИРОВАНИЕ ФАЙЛОВ ПРОЕКТА
# ============================================
# --chown=50000:0 использует числовые ID
COPY --chown=50000:0 app/ /opt/airflow/app/
COPY --chown=50000:0 ml.ipynb /opt/airflow/ml/

# ============================================
# ПОРТЫ
# ============================================
EXPOSE 8080 8501 8888

# ============================================
# HEALTH CHECK
# ============================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import requests, pandas; print('OK')" || exit 1

# ============================================
# ENTRYPOINT
# ============================================
ENTRYPOINT ["airflow"]
CMD ["webserver"]
