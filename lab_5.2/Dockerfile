# ============================================
# Dockerfile для ETL Workshop 2026
# Вариант 16: Rocket Launch Analytics
# ============================================

# Базовый образ Apache Airflow 2.8.1 с Python 3.11
FROM apache/airflow:2.8.1-python3.11

# ============================================
# 1. МЕТАДАННЫЕ
# ============================================
LABEL maintainer="Tyapkina P.A."
LABEL version="1.0"
LABEL description="ETL Pipeline with ML and Streamlit for Variant 16"

# ============================================
# 2. ПЕРЕКЛЮЧЕНИЕ НА ROOT ДЛЯ УСТАНОВКИ ПАКЕТОВ
# ============================================
USER root

# ============================================
# 3. СИСТЕМНЫЕ ЗАВИСИМОСТИ
# ============================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Для компиляции Python пакетов
    build-essential \
    gcc \
    g++ \
    # Для работы с изображениями
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    # Для сетевых проверок (ping, DNS)
    iputils-ping \
    dnsutils \
    # Для загрузки данных
    curl \
    wget \
    # Для работы с файлами
    jq \
    vim \
    # Для Jupyter
    libzmq3-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# ============================================
# 4. ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# ============================================
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Пути для данных
ENV DATA_DIR=/opt/airflow/data
ENV LOGS_DIR=/opt/airflow/logs
ENV IMAGES_DIR=/opt/airflow/data/images

# ============================================
# 5. СОЗДАНИЕ ДИРЕКТОРИЙ
# ============================================
RUN mkdir -p ${DATA_DIR} ${LOGS_DIR} ${IMAGES_DIR} /opt/airflow/app /opt/airflow/ml

# ============================================
# 6. УСТАНОВКА PYTHON ПАКЕТОВ
# ============================================
RUN pip install --upgrade pip setuptools wheel

# Установка основных зависимостей
RUN pip install --no-cache-dir \
    # Airflow дополнительные пакеты
    apache-airflow-providers-http \
    apache-airflow-providers-postgres \
    # Запросы к API
    requests==2.31.0 \
    urllib3==2.1.0 \
    # Обработка данных
    pandas==2.1.4 \
    numpy==1.26.3 \
    # Изображения
    pillow==10.2.0 \
    # ML и нейросети
    torch==2.1.2 \
    torchvision==0.16.2 \
    transformers==4.36.2 \
    # Streamlit
    streamlit==1.30.0 \
    # Jupyter
    jupyter==1.0.0 \
    notebook==7.0.6 \
    jupyterlab==4.0.10 \
    # DNS для проверок
    dnspython==2.5.0 \
    # Утилиты
    python-dotenv==1.0.0 \
    pyyaml==6.0.1

# ============================================
# 7. НАСТРОЙКА ПРАВ ДОСТУПА
# ============================================
# Airflow работает под пользователем с UID 50000
# Важно: все папки должны быть доступны этому пользователю
RUN chown -R airflow:airflow /opt/airflow \
    && chmod -R 775 /opt/airflow/data \
    && chmod -R 775 /opt/airflow/logs \
    && chmod -R 775 /opt/airflow/app

# ============================================
# 8. ПЕРЕКЛЮЧЕНИЕ ОБРАТНО НА AIRFLOW USER
# ============================================
USER airflow

# ============================================
# 9. РАБОЧАЯ ДИРЕКТОРИЯ
# ============================================
WORKDIR /opt/airflow

# ============================================
# 10. КОПИРОВАНИЕ ФАЙЛОВ ПРОЕКТА
# ============================================
# Копируем файлы приложения (Streamlit)
COPY --chown=airflow:airflow app/ /opt/airflow/app/

# Копируем ML ноутбук
COPY --chown=airflow:airflow ml.ipynb /opt/airflow/ml/

# Копируем requirements (если есть)
# COPY --chown=airflow:airflow requirements.txt /opt/airflow/

# ============================================
# 11. ЭКСПОЗИЦИЯ ПОРТОВ
# ============================================
# Airflow Webserver
EXPOSE 8080
# Streamlit
EXPOSE 8501
# Jupyter
EXPOSE 8888

# ============================================
# 12. ПРОВЕРКА УСТАНОВКИ (HEALTH CHECK)
# ============================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import requests; import pandas; import torch; print('OK')" || exit 1

# ============================================
# 13. ТОЧКА ВХОДА (ENTRYPOINT)
# ============================================
# По умолчанию запускаем Airflow Webserver
# В docker-compose переопределяется для каждого сервиса

ENTRYPOINT ["airflow"]
CMD ["webserver"]
