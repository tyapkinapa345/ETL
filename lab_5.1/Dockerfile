FROM apache/airflow:slim-2.8.1-python3.11

USER airflow

# Устанавливаем необходимые Python-библиотеки
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    joblib \
    requests \
    azure-storage-blob==12.8.1 \
    psycopg2-binary \
    streamlit \
    matplotlib \
    "connexion[swagger-ui]"

USER root

# Создаём директории и назначаем владельца
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs /opt/airflow/app

USER airflow