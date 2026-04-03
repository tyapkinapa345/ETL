
"""
DAG для варианта 16:
- Скачивание JSON с API запусков
- Извлечение URL изображений
- Подсчёт доменов (задание 1)
- Проверка доступности серверов (ping + HEAD) (задание 2)
- Логирование ошибок (задание 3)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import json
import os
import subprocess
import requests
from urllib.parse import urlparse
import logging
from pathlib import Path

# ========== КОНФИГУРАЦИЯ ==========
DATA_DIR = "/opt/airflow/data"
IMAGES_DIR = f"{DATA_DIR}/images"
LOGS_DIR = "/opt/airflow/logs"

API_URL = "https://ll.thespacedevs.com/2.3.0/launches/upcoming/?format=json&mode=list&limit=10"
MAX_IMAGES = 10

# Файлы отчётов
DOMAIN_REPORT = f"{DATA_DIR}/domain_counts_report.txt"
SERVER_STATUS_FILE = f"{DATA_DIR}/server_status.txt"
ERROR_LOG_JSON = f"{LOGS_DIR}/error_log.json"
ERROR_LOG_TXT = f"{LOGS_DIR}/error_log.txt"

# Настройка логирования
logger = logging.getLogger(__name__)

# ========== ФУНКЦИИ ДЛЯ ЗАДАНИЙ ==========

def setup_directories():
    """Создаёт нужные папки"""
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

def download_json():
    """Скачивает JSON с API, возвращает список URL изображений"""
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        image_urls = []
        for launch in data.get("results", []):
            img = launch.get("image")
            if isinstance(img, dict):
                url = img.get("image_url")
            elif isinstance(img, str):
                url = img
            else:
                url = None
            if url:
                image_urls.append(url)
        
        # Уникальные, не более MAX_IMAGES
        image_urls = list(dict.fromkeys(image_urls))[:MAX_IMAGES]
        
        # Сохраняем JSON для отладки
        with open(f"{DATA_DIR}/launches.json", "w") as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Загружено {len(image_urls)} уникальных URL изображений")
        return image_urls
    except Exception as e:
        logger.error(f"Ошибка загрузки JSON: {e}")
        raise

def check_server(url):
    """
    Проверка сервера: DNS, ping, HEAD-запрос.
    Возвращает dict с результатами.
    """
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path.split('/')[0]
    result = {
        "url": url,
        "domain": domain,
        "dns_ok": False,
        "ping_ok": False,
        "http_ok": False,
        "status_code": None,
        "response_time_ms": None,
        "error": None
    }
    
    # 1. DNS (проверяем, что домен резолвится)
    try:
        import socket
        socket.gethostbyname(domain)
        result["dns_ok"] = True
    except Exception as e:
        result["error"] = f"DNS: {str(e)[:50]}"
        return result
    
    # 2. Ping (одна попытка, таймаут 2 сек)
    try:
        ping_cmd = ["ping", "-c", "1", "-W", "2", domain]
        subprocess.run(ping_cmd, capture_output=True, timeout=5)
        result["ping_ok"] = True
    except Exception:
        result["ping_ok"] = False
    
    # 3. HEAD-запрос
    try:
        start = datetime.now()
        resp = requests.head(url, timeout=10, allow_redirects=True)
        elapsed = (datetime.now() - start).total_seconds() * 1000
        result["http_ok"] = resp.status_code < 400
        result["status_code"] = resp.status_code
        result["response_time_ms"] = round(elapsed, 2)
        if not result["http_ok"]:
            result["error"] = f"HTTP {resp.status_code}"
    except Exception as e:
        result["error"] = f"HTTP: {str(e)[:50]}"
    
    return result

def download_image(url, task_id):
    """Скачивает изображение, возвращает имя файла или None"""
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        # Имя файла из URL
        fname = url.split("/")[-1].split("?")[0]
        if not fname or "." not in fname:
            fname = f"image_{task_id}.jpg"
        # Очистка имени
        fname = "".join(c for c in fname if c.isalnum() or c in "._-")
        fpath = os.path.join(IMAGES_DIR, fname)
        with open(fpath, "wb") as f:
            f.write(resp.content)
        logger.info(f"Скачано: {fname} ({len(resp.content)} байт)")
        return fname
    except Exception as e:
        logger.warning(f"Не удалось скачать {url}: {e}")
        return None

def main_pipeline(**context):
    """Главная функция, выполняет все три задания"""
    setup_directories()
    
    # Получаем URL изображений
    image_urls = download_json()
    
    # Статистика для заданий
    domain_counts = {}
    server_status_list = []   # для подробного отчёта
    errors = []               # для лога ошибок
    
    # Обрабатываем каждый URL
    for idx, url in enumerate(image_urls, 1):
        domain = urlparse(url).netloc or urlparse(url).path.split('/')[0]
        # Задание 1: счётчик по доменам
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
        
        # Задание 2: проверка сервера
        server_check = check_server(url)
        server_status_list.append(server_check)
        
        # Задание 3: логируем ошибки, если есть
        if server_check.get("error"):
            errors.append({
                "timestamp": datetime.now().isoformat(),
                "url": url,
                "domain": domain,
                "error": server_check["error"]
            })
        
        # Скачиваем изображение (только если HEAD прошёл успешно)
        if server_check.get("http_ok"):
            download_image(url, idx)
        else:
            logger.info(f"Пропускаем скачивание {url} из-за ошибки проверки")
    
    # --- Сохраняем отчёты ---
    
    # 1. Отчёт по доменам
    with open(DOMAIN_REPORT, "w") as f:
        f.write("=" * 50 + "\n")
        f.write("Отчёт по доменам (Вариант 16)\n")
        f.write(f"Сгенерировано: {datetime.now()}\n\n")
        f.write(f"{'Домен':<40} {'Кол-во':>8}\n")
        f.write("-" * 50 + "\n")
        for dom, cnt in sorted(domain_counts.items(), key=lambda x: -x[1]):
            f.write(f"{dom:<40} {cnt:>8}\n")
        f.write("-" * 50 + "\n")
        f.write(f"Всего доменов: {len(domain_counts)}\n")
        f.write(f"Всего запросов: {sum(domain_counts.values())}\n")
    
    # 2. Статус серверов (краткий)
    with open(SERVER_STATUS_FILE, "w") as f:
        f.write("=" * 60 + "\n")
        f.write("Статус доступности серверов (Ping/HEAD)\n")
        f.write(f"Дата: {datetime.now()}\n\n")
        for check in server_status_list:
            status = "✅ ДОСТУПЕН" if check.get("http_ok") else "❌ НЕДОСТУПЕН"
            f.write(f"[{status}] {check['domain']}\n")
            f.write(f"   URL: {check['url']}\n")
            f.write(f"   DNS: {'OK' if check['dns_ok'] else 'FAIL'}\n")
            f.write(f"   Ping: {'OK' if check['ping_ok'] else 'FAIL'}\n")
            f.write(f"   HTTP: {check['status_code'] or 'N/A'} ({check['response_time_ms']} ms)\n")
            if check.get("error"):
                f.write(f"   Ошибка: {check['error']}\n")
            f.write("\n")
        online = sum(1 for c in server_status_list if c.get("http_ok"))
        f.write(f"\nИтого: {online}/{len(server_status_list)} серверов доступны\n")
    
    # 3. Лог ошибок (JSON + TXT)
    with open(ERROR_LOG_JSON, "w") as f:
        json.dump({
            "metadata": {"generated": datetime.now().isoformat(), "variant": 16},
            "errors": errors
        }, f, indent=2)
    
    with open(ERROR_LOG_TXT, "w") as f:
        f.write("Лог ошибок (Вариант 16)\n")
        f.write(f"Сгенерировано: {datetime.now()}\n\n")
        if not errors:
            f.write("Ошибок не зафиксировано.\n")
        else:
            for i, err in enumerate(errors, 1):
                f.write(f"{i}. {err['timestamp']}\n")
                f.write(f"   Домен: {err['domain']}\n")
                f.write(f"   URL: {err['url']}\n")
                f.write(f"   Ошибка: {err['error']}\n\n")
    
    logger.info(f"✅ Pipeline завершён. Доменов: {len(domain_counts)}, ошибок: {len(errors)}")
    return "OK"

# ========== ОПРЕДЕЛЕНИЕ DAG ==========
default_args = {
    "owner": "TyapkinaPA",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 1),
    "retries": 1,
    "retry_delay": 300,
}

dag = DAG(
    dag_id="listing_TyapkinaPA_Rocket",
    default_args=default_args,
    description="ETL: Domain stats + Server checks + Error logging (Variant 16)",
    schedule_interval="@daily",
    catchup=False,
    tags=["rocket", "variant16"],
)

# Задачи
clean = BashOperator(
    task_id="clean_old_files",
    bash_command=f"rm -f {DOMAIN_REPORT} {SERVER_STATUS_FILE} {ERROR_LOG_JSON} {ERROR_LOG_TXT} && echo 'Очищено'",
    dag=dag,
)

process = PythonOperator(
    task_id="run_etl_pipeline",
    python_callable=main_pipeline,
    dag=dag,
)

notify = BashOperator(
    task_id="notify_completion",
    bash_command='echo "✅ Pipeline completed successfully!" && echo "Files generated:" && ls -la /opt/airflow/data/*.txt && echo "Images count:" && ls /opt/airflow/data/images/ | wc -l',
    dag=dag,
)

clean >> process >> notify
