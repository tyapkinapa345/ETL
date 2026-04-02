dev@dev-vm:~/Downloads/lab_etl/data_for_labs/lab_airflow/workshop-on-ETL/practice/business_case_rocket_26$ sudo docker compose up -d
[sudo] password for dev: 
WARN[0000] /home/dev/Downloads/lab_etl/data_for_labs/lab_airflow/workshop-on-ETL/practice/business_case_rocket_26/docker-compose.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
[+] up 16/16
 ✔ Image postgres:13            Pulled                                                                 29.9s
 ✔ Network etl-workshop-network Created                                                                0.2s
[+] up 22/22ound orphan containers ([business_case_rocket_26-scheduler-1 business_case_rocket_26-webserver-1
 ✔ Image postgres:13            Pulled                                                                 29.9s
 ✔ Network etl-workshop-network Created                                                                0.2s
 ✔ Volume etl-postgres-data     Created                                                                0.0s
 ✔ Container postgres-airflow   Healthy                                                                6.7s
 ✔ Container airflow-webserver  Created                                                                0.1s
 ✔ Container airflow-scheduler  Created                                                                0.1s
 ✔ Container jupyter-ml         Created                                                                0.1s
 ✔ Container streamlit-app      Created                                                                0.1s
Error response from daemon: failed to set up container networking: driver failed programming external connectivity on endpoint airflow-webserver (c187dba2e9ef8d99c8fca248e399f64a205ee2df90aa4cd8a54368acedf410aa): Bind for 0.0.0.0:8080 failed: port is already allocated
dev@dev-vm:~/Downloads/lab_etl/data_for_labs/lab_airflow/workshop-on-ETL/practice/business_case_rocket_26$ sudo docker compose up -d
WARN[0000] /home/dev/Downloads/lab_etl/data_for_labs/lab_airflow/workshop-on-ETL/practice/business_case_rocket_26/docker-compose.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
WARN[0000] Found orphan containers ([business_case_rocket_26-scheduler-1 business_case_rocket_26-webserver-1]) for this project. If you removed or renamed this service in your compose file, you can run this command with the --remove-orphans flag to clean it up. 
[+] up 2/2
 ✔ Container postgres-airflow  Healthy                                                                  0.5s
 ✔ Container airflow-scheduler Running    
 
 
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
