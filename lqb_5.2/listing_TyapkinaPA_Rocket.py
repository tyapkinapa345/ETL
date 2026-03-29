import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from collections import Counter
from urllib.parse import urlparse
from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# --- Конфигурация ---
DATA_DIR = "/opt/airflow/data"
IMAGES_DIR = f"{DATA_DIR}/images"
TMP_JSON_FILE = "/tmp/launches.json"
MAX_IMAGES = 10
API_URL = "https://ll.thespacedevs.com/2.3.0/launches/upcoming/?format=json&mode=list&limit={MAX_IMAGES}"
ERROR_LOG_FILE = "/opt/airflow/logs/error_log.txt"   # сохраняется на хосте в ./logs/

# --- Задание 2: Проверка доступности сервера ---
def check_server_availability():
    """Выполняет HEAD-запрос к API, при ошибке пишет в error_log.txt и падает."""
    try:
        resp = requests.head(API_URL, timeout=10)
        resp.raise_for_status()
        logging.info(f"Сервер доступен, статус: {resp.status_code}")
    except Exception as e:
        error_msg = f"{datetime.now()} - Ошибка доступности {API_URL}: {str(e)}"
        logging.error(error_msg)
        # Логируем в файл на хосте
        pathlib.Path(ERROR_LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
        with open(ERROR_LOG_FILE, "a") as f:
            f.write(error_msg + "\n")
        raise

# --- Загрузка JSON (как в исходном DAG, но с обработкой ошибок) ---
def download_launches(**context):
    """Скачивает JSON, при ошибке пишет в лог."""
    try:
        resp = requests.get(API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        with open(TMP_JSON_FILE, "w") as f:
            json.dump(data, f)
        logging.info("JSON успешно сохранён")
    except Exception as e:
        error_msg = f"{datetime.now()} - Ошибка загрузки JSON: {str(e)}"
        logging.error(error_msg)
        with open(ERROR_LOG_FILE, "a") as f:
            f.write(error_msg + "\n")
        raise

# --- Загрузка картинок + сбор URL для подсчёта доменов ---
def get_pictures(**context):
    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)
    with open(TMP_JSON_FILE, encoding="utf-8") as f:
        try:
            launches = json.load(f)
        except json.JSONDecodeError as e:
            f.seek(0)
            preview = f.read(500)
            error_msg = f"{datetime.now()} - JSON decode error: {e}. Preview: {preview}"
            logging.error(error_msg)
            with open(ERROR_LOG_FILE, "a") as log_f:
                log_f.write(error_msg + "\n")
            raise RuntimeError(error_msg)

        image_urls = []
        for launch in launches.get("results", []):
            image = launch.get("image")
            if isinstance(image, dict):
                url = image.get("image_url")
                if url:
                    image_urls.append(url)
            elif isinstance(image, str) and image:
                image_urls.append(image)

        image_urls = list(dict.fromkeys(image_urls))[:MAX_IMAGES]

        # Сохраняем список URL в XCom для следующей задачи
        context['task_instance'].xcom_push(key='image_urls', value=image_urls)

        # Скачиваем картинки, логируя ошибки
        for idx, img_url in enumerate(image_urls, 1):
            logging.info(f"[{idx}/{len(image_urls)}] Загрузка: {img_url}")
            try:
                resp = requests.get(img_url, timeout=30)
                resp.raise_for_status()
                filename = img_url.split("/")[-1]
                if not filename:
                    filename = f"image_{idx}.jpg"
                target = f"{IMAGES_DIR}/{filename}"
                with open(target, "wb") as f:
                    f.write(resp.content)
                logging.info(f"Сохранено: {target}")
            except Exception as e:
                error_msg = f"{datetime.now()} - Ошибка загрузки {img_url}: {str(e)}"
                logging.error(error_msg)
                with open(ERROR_LOG_FILE, "a") as log_f:
                    log_f.write(error_msg + "\n")

# --- Задание 1: Отчёт по доменам ---
def report_domain_counts(**context):
    """Извлекает домены из URL, считает количество, сохраняет отчёт в DATA_DIR."""
    image_urls = context['task_instance'].xcom_pull(task_ids='get_pictures', key='image_urls')
    if not image_urls:
        logging.warning("Нет URL для анализа")
        return

    domains = [urlparse(url).netloc for url in image_urls if urlparse(url).netloc]
    domain_counts = Counter(domains)

    report_path = f"{DATA_DIR}/domain_counts_report.txt"
    with open(report_path, "w") as f:
        f.write(f"Отчёт по доменам (вариант 16)\n")
        f.write(f"Сформирован: {datetime.now()}\n\n")
        for domain, count in domain_counts.items():
            f.write(f"{domain}: {count}\n")
            logging.info(f"Домен {domain}: {count} изображений")

    logging.info(f"Отчёт сохранён: {report_path}")

# --- Определение DAG ---
dag = DAG(
    dag_id="listing_TyapkinaPA_Rocket",   # замените ivanov на свою фамилию
    description="Вариант 16: проверка доступности, подсчёт по доменам, логирование ошибок",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        'owner': 'ivanov',            # ваше имя
        'retries': 1,
    }
)

# --- Задачи ---
clean_data = BashOperator(
    task_id="clean_data_directory",
    bash_command=f"mkdir -p {DATA_DIR} && rm -rf {DATA_DIR}/*",
    dag=dag,
)

check_server = PythonOperator(
    task_id="check_server_availability",
    python_callable=check_server_availability,
    dag=dag,
)

download_json = PythonOperator(
    task_id="download_launches",
    python_callable=download_launches,
    dag=dag,
)

download_images = PythonOperator(
    task_id="get_pictures",
    python_callable=get_pictures,
    provide_context=True,
    dag=dag,
)

report_domains = PythonOperator(
    task_id="report_domain_counts",
    python_callable=report_domain_counts,
    provide_context=True,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "Готово. Изображений: $(ls {IMAGES_DIR}/ | wc -l). Отчёт: {DATA_DIR}/domain_counts_report.txt"',
    dag=dag,
)

# --- Порядок выполнения (учитываем проверку доступности) ---
clean_data >> check_server >> download_json >> download_images >> report_domains >> notify
