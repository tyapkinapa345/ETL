# dags/listing_TyapkinaPA_Rocket.py
# ETL Pipeline для Варианта 16
# Задание 1: Анализ доменов
# Задание 2: Проверка серверов (Ping/Head)
# Задание 3: Логирование ошибок

import json
import pathlib
import logging
import socket
import subprocess
import re
from datetime import datetime
from urllib.parse import urlparse
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import requests.exceptions as requests_exceptions

# ============================================
# 1. КОНФИГУРАЦИЯ И ПУТИ
# ============================================

# Пути внутри контейнера Airflow
DATA_DIR = "/opt/airflow/data"
LOGS_DIR = "/opt/airflow/logs"
IMAGES_DIR = f"{DATA_DIR}/images"
TMP_JSON_FILE = "/tmp/launches.json"

# Файлы отчётов (Вариант 16)
DOMAIN_COUNTS_FILE = f"{DATA_DIR}/domain_counts_report.txt"
SERVER_STATUS_FILE = f"{DATA_DIR}/server_status.txt"
SERVER_PING_FILE = f"{DATA_DIR}/server_ping_detailed.txt"
ERROR_LOG_JSON = f"{LOGS_DIR}/error_log.json"
ERROR_LOG_TXT = f"{LOGS_DIR}/error_log.txt"

# API Launch Library 2
API_URL = "https://ll.thespacedevs.com/2.3.0/launches/upcoming/?format=json&mode=list&limit=10"
MAX_IMAGES = 10

# Тестовые сценарии для демонстрации ошибок (Задание 3)
TEST_SCENARIOS = [
    {"url": "https://httpstat.us/404", "description": "HTTP_404"},
    {"url": "https://httpstat.us/503", "description": "HTTP_503"},
    {"url": "https://httpstat.us/408?sleep=30000", "description": "Timeout"},
    {"url": "https://nonexistent-domain-xyz123456789.com/image.jpg", "description": "DNS_Failure"},
    {"url": "not-a-valid-url-at-all", "description": "Invalid_Schema"},
]

# Флаг: включать ли тестовые сценарии
# True = для демонстрации преподавателю (будут ошибки)
# False = для продакшена (только реальные данные)
ENABLE_TEST_ERRORS = True

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(ERROR_LOG_TXT, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# 2. ОПРЕДЕЛЕНИЕ DAG
# ============================================

dag = DAG(
    dag_id="listing_TyapkinaPA_Rocket",
    description="ETL: Domain Analysis + Server Ping/Head + Error Logging (Variant 16)",
    start_date=datetime(2026, 2, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["rocket", "variant16", "etl", "server-monitoring"],
    default_args={
        "owner": "Tyapkina P.A.",
        "retries": 1,
        "retry_delay": 300,
    }
)


# ============================================
# 3. ЗАДАЧИ DAG
# ============================================

# Task 0: Очистка и подготовка директорий
clean_data = BashOperator(
    task_id="clean_data_directory",
    bash_command=f"mkdir -p {DATA_DIR} {IMAGES_DIR} {LOGS_DIR} && echo 'Directories prepared'",
    dag=dag,
)

# Task 1: Скачивание JSON с API
download_json = BashOperator(
    task_id="download_launches_json",
    bash_command=(
        f"curl -fSL --connect-timeout 15 --max-time 120 --progress-bar "
        f"-H 'Accept: application/json' -o {TMP_JSON_FILE} '{API_URL}' && "
        f"echo 'JSON downloaded: $(wc -c < {TMP_JSON_FILE}) bytes'"
    ),
    dag=dag,
)


# ============================================
# 4. ФУНКЦИИ ПРОВЕРКИ СЕРВЕРОВ
# ============================================

def resolve_domain_ip(domain):
    """
    Получить IP адреса домена через DNS
    Возвращает: dict с ipv4, ipv6, error
    """
    result = {"ipv4": [], "ipv6": [], "error": None}
    
    try:
        # IPv4 (A запись)
        answers = socket.getaddrinfo(domain, None, socket.AF_INET, socket.SOCK_STREAM)
        ips = list(set([addr[4][0] for addr in answers]))
        result["ipv4"] = ips[:5]  # Максимум 5 IP
    except Exception as e:
        result["error"] = f"DNS A: {str(e)[:100]}"
    
    try:
        # IPv6 (AAAA запись) - опционально
        answers = socket.getaddrinfo(domain, None, socket.AF_INET6, socket.SOCK_STREAM)
        ips = list(set([addr[4][0] for addr in answers]))
        result["ipv6"] = ips[:5]
    except:
        pass  # IPv6 не обязателен
    
    return result


def ping_server(domain, count=3, timeout=2):
    """
    Выполнить ping до сервера (системная команда)
    Возвращает: dict с результатами ping
    """
    result = {
        "success": False,
        "packets_sent": count,
        "packets_received": 0,
        "packet_loss_percent": 100.0,
        "min_rtt_ms": None,
        "avg_rtt_ms": None,
        "max_rtt_ms": None,
        "error": None,
        "raw_output": ""
    }
    
    try:
        # ping команда для Linux
        cmd = ["ping", "-c", str(count), "-W", str(timeout), domain]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=count*timeout+5)
        
        result["raw_output"] = proc.stdout + proc.stderr
        
        # Парсинг вывода ping
        output = result["raw_output"]
        
        if "packet loss" in output:
            result["success"] = proc.returncode == 0
            for line in output.split("\n"):
                if "packet loss" in line:
                    try:
                        # Формат: "X packets transmitted, Y received, Z% packet loss"
                        match = re.search(r'(\d+)%\s*packet\s*loss', line)
                        if match:
                            result["packet_loss_percent"] = float(match.group(1))
                        
                        match = re.search(r'(\d+)\s*received', line)
                        if match:
                            result["packets_received"] = int(match.group(1))
                    except:
                        pass
                
                # RTT статистика: "rtt min/avg/max/mdev = X/Y/Z/W ms"
                if "rtt" in line.lower() or "round-trip" in line.lower():
                    try:
                        match = re.search(r'=?\s*([\d.]+)/\s*([\d.]+)/\s*([\d.]+)/\s*([\d.]+)', line)
                        if match:
                            result["min_rtt_ms"] = float(match.group(1))
                            result["avg_rtt_ms"] = float(match.group(2))
                            result["max_rtt_ms"] = float(match.group(3))
                    except:
                        pass
    except subprocess.TimeoutExpired:
        result["error"] = f"Ping timeout after {count*timeout+5}s"
    except FileNotFoundError:
        result["error"] = "ping command not found"
    except Exception as e:
        result["error"] = f"Ping failed: {str(e)[:100]}"
    
    return result


def check_server_head(url, timeout=10):
    """
    HEAD запрос с полной информацией
    Возвращает: dict со всеми данными HTTP запроса
    """
    result = {
        "url": url,
        "success": False,
        "status_code": None,
        "response_time_ms": None,
        "headers": {},
        "error": None,
        "redirect_chain": [],
        "final_url": url,
        "content_type": None,
        "content_length": None,
        "server_header": None
    }
    
    start_time = datetime.now()
    
    try:
        session = requests.Session()
        response = session.head(url, timeout=timeout, allow_redirects=True)
        
        result["response_time_ms"] = round((datetime.now() - start_time).total_seconds() * 1000, 2)
        result["status_code"] = response.status_code
        result["success"] = response.status_code < 400
        result["final_url"] = response.url
        
        # Сохраняем все заголовки
        for header, value in response.headers.items():
            result["headers"][header.lower()] = value
        
        # Ключевые заголовки отдельно
        result["content_type"] = response.headers.get("content-type")
        result["content_length"] = response.headers.get("content-length")
        result["server_header"] = response.headers.get("server")
        
        # Цепочка редиректов
        if response.history:
            result["redirect_chain"] = [str(r.url) for r in response.history]
            
    except requests_exceptions.Timeout:
        result["error"] = f"Timeout after {timeout}s"
        result["response_time_ms"] = round((datetime.now() - start_time).total_seconds() * 1000, 2)
    except requests_exceptions.ConnectionError as e:
        result["error"] = f"Connection error: {str(e)[:150]}"
    except requests_exceptions.HTTPError as e:
        result["status_code"] = e.response.status_code if e.response else None
        result["error"] = f"HTTP error: {str(e)[:150]}"
    except requests_exceptions.RequestException as e:
        result["error"] = f"Request failed: {str(e)[:150]}"
    except Exception as e:
        result["error"] = f"Unexpected: {type(e).__name__}: {str(e)[:150]}"
    
    return result


def download_image(url, task_id):
    """
    Скачать изображение
    Возвращает: dict со статусом скачивания
    """
    result = {
        "success": False,
        "filename": None,
        "filepath": None,
        "size_bytes": 0,
        "error": None
    }
    
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        
        # Извлекаем имя файла из URL
        filename = url.split("/")[-1].split("?")[0]
        filename = "".join(c for c in filename if c.isalnum() or c in "._-")
        filename = filename[:50] or f"image_{task_id}.jpg"
        
        filepath = f"{IMAGES_DIR}/{filename}"
        
        with open(filepath, "wb") as f:
            f.write(response.content)
        
        result["success"] = True
        result["filename"] = filename
        result["filepath"] = filepath
        result["size_bytes"] = len(response.content)
        
        logger.info(f"[{task_id}] ✅ Downloaded: {filename} ({result['size_bytes']} bytes)")
        
    except requests_exceptions.RequestException as e:
        result["error"] = f"Download failed: {str(e)[:150]}"
    except Exception as e:
        result["error"] = f"Unexpected: {type(e).__name__}: {str(e)[:150]}"
    
    return result


def log_error(errors, task_id, url, error_type, error_msg, is_test):
    """
    Задание 3: Структурированное логирование ошибки
    """
    error_entry = {
        "timestamp": datetime.now().isoformat(),
        "task_id": task_id,
        "url": url,
        "error_type": error_type,
        "error_message": error_msg,
        "is_test_scenario": is_test,
        "severity": "info" if is_test else "error"
    }
    
    logger.error(f"[{task_id}] {error_type}: {error_msg[:200]}")
    errors.append(error_entry)


def process_single_url(url, domain_counts, server_status, server_ping_data, errors, task_id, is_test=False):
    """
    Обработка одного URL со сбором ВСЕХ данных
    """
    prefix = "[TEST] " if is_test else ""
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path.split('/')[0] or "unknown"
    
    logger.info(f"{prefix}Processing: {url}")
    
    # === ЗАДАНИЕ 1: Подсчёт доменов ===
    domain_counts[domain] = domain_counts.get(domain, 0) + 1
    
    # === Сбор данных для проверки сервера ===
    check_data = {
        "task_id": task_id,
        "url": url,
        "domain": domain,
        "checked_at": datetime.now().isoformat(),
        "is_test": is_test,
        "dns": {},
        "ping": {},
        "http_head": {},
        "download": {}
    }
    
    # 1. DNS разрешение
    logger.debug(f"{prefix}Resolving DNS for {domain}")
    check_data["dns"] = resolve_domain_ip(domain)
    
    # 2. Ping
    logger.debug(f"{prefix}Pinging {domain}")
    check_data["ping"] = ping_server(domain)
    
    # 3. HEAD запрос
    logger.debug(f"{prefix}Sending HEAD request to {url}")
    check_data["http_head"] = check_server_head(url)
    
    # Сохраняем краткий статус по домену (для server_status.txt)
    if domain not in server_status:
        server_status[domain] = check_data["http_head"].copy()
    
    # 4. Скачивание (только если HEAD успешен)
    if check_data["http_head"].get("success"):
        logger.debug(f"{prefix}Downloading image from {url}")
        check_data["download"] = download_image(url, task_id)
        if not check_data["download"]["success"]:
            log_error(errors, task_id, url, "DownloadError", check_data["download"]["error"], is_test)
    else:
        check_data["download"] = {"success": False, "error": "Skipped (HEAD failed)"}
        if check_data["http_head"].get("error"):
            log_error(errors, task_id, url, "HeadRequestError", check_data["http_head"]["error"], is_test)
    
    # Сохраняем полные данные проверки
    server_ping_data.append(check_data)


# ============================================
# 5. ФУНКЦИИ СОХРАНЕНИЯ ОТЧЁТОВ
# ============================================

def save_domain_report(domain_counts):
    """
    Задание 1: Сохранение отчёта по доменам
    """
    with open(DOMAIN_COUNTS_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("DOMAIN DOWNLOAD STATISTICS | Variant 16\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Pipeline: listing_TyapkinaPA_Rocket\n\n")
        
        f.write(f"{'Domain':<45} {'Count':>8}\n")
        f.write("-" * 55 + "\n")
        for domain, count in sorted(domain_counts.items(), key=lambda x: -x[1]):
            f.write(f"{domain:<45} {count:>8}\n")
        
        f.write("\n" + "=" * 60 + "\n")
        f.write(f"Total domains: {len(domain_counts)}\n")
        f.write(f"Total requests: {sum(domain_counts.values())}\n")
    
    logger.info(f"Domain report saved to {DOMAIN_COUNTS_FILE}")


def save_server_status_brief(server_status):
    """
    Задание 2: Краткий статус серверов
    """
    with open(SERVER_STATUS_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("SERVER AVAILABILITY STATUS (BRIEF) | Variant 16\n")
        f.write("=" * 70 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")
        
        for domain, data in server_status.items():
            status = "✅ ONLINE" if data.get("success") else "❌ OFFLINE"
            f.write(f"[{status}] {domain}\n")
            f.write(f"    Status Code: {data.get('status_code', 'N/A')}\n")
            f.write(f"    Response Time: {data.get('response_time_ms', 'N/A')} ms\n")
            if data.get("error"):
                f.write(f"    Error: {data['error']}\n")
            f.write("\n")
        
        online = sum(1 for d in server_status.values() if d.get("success"))
        total = len(server_status)
        f.write(f"\n{'=' * 70}\n")
        f.write(f"SUMMARY: {online}/{total} servers online ({online/total*100:.1f}%)\n")
    
    logger.info(f"Server status saved to {SERVER_STATUS_FILE}")


def save_server_ping_detailed(server_ping_data):
    """
    Задание 2: ПОЛНЫЕ данные ping/HEAD для каждого запроса
    """
    with open(SERVER_PING_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("DETAILED SERVER PING & HEAD CHECK REPORT | Variant 16\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Pipeline: listing_TyapkinaPA_Rocket\n")
        f.write(f"Total checks: {len(server_ping_data)}\n\n")
        
        for i, check in enumerate(server_ping_data, 1):
            test_tag = "[TEST] " if check.get("is_test") else ""
            f.write(f"\n{'#' * 80}\n")
            f.write(f"CHECK #{i} {test_tag}| Task: {check.get('task_id')}\n")
            f.write(f"{'#' * 80}\n")
            
            f.write(f"\n📌 URL: {check.get('url', 'N/A')}\n")
            f.write(f"🌐 Domain: {check.get('domain', 'N/A')}\n")
            f.write(f"⏰ Checked: {check.get('checked_at', 'N/A')}\n")
            
            # DNS информация
            dns = check.get("dns", {})
            f.write(f"\n🔍 DNS RESOLUTION:\n")
            f.write(f"    IPv4: {', '.join(dns.get('ipv4', [])) or 'N/A'}\n")
            f.write(f"    IPv6: {', '.join(dns.get('ipv6', [])) or 'N/A'}\n")
            if dns.get("error"):
                f.write(f"    ⚠️ DNS Error: {dns['error']}\n")
            
            # Ping информация
            ping = check.get("ping", {})
            f.write(f"\n📶 PING RESULTS:\n")
            f.write(f"    Success: {'✅ Yes' if ping.get('success') else '❌ No'}\n")
            f.write(f"    Packets: {ping.get('packets_received', 0)}/{ping.get('packets_sent', 0)}\n")
            f.write(f"    Loss: {ping.get('packet_loss_percent', 100)}%\n")
            if ping.get("avg_rtt_ms"):
                f.write(f"    RTT: {ping.get('min_rtt_ms')} / {ping.get('avg_rtt_ms')} / {ping.get('max_rtt_ms')} ms (min/avg/max)\n")
            if ping.get("error"):
                f.write(f"    ⚠️ Ping Error: {ping['error']}\n")
            
            # HEAD запрос
            head = check.get("http_head", {})
            f.write(f"\n🌐 HTTP HEAD REQUEST:\n")
            f.write(f"    Success: {'✅ Yes' if head.get('success') else '❌ No'}\n")
            f.write(f"    Status Code: {head.get('status_code', 'N/A')}\n")
            f.write(f"    Response Time: {head.get('response_time_ms', 'N/A')} ms\n")
            f.write(f"    Final URL: {head.get('final_url', 'N/A')[:80]}\n")
            
            if head.get("content_type"):
                f.write(f"    Content-Type: {head['content_type']}\n")
            if head.get("content_length"):
                f.write(f"    Content-Length: {head['content_length']}\n")
            if head.get("server_header"):
                f.write(f"    Server: {head['server_header']}\n")
            
            if head.get("redirect_chain"):
                f.write(f"    Redirects: {' -> '.join(head['redirect_chain'][:3])}\n")
            
            if head.get("headers"):
                f.write(f"    Headers (first 8):\n")
                for hdr, val in list(head["headers"].items())[:8]:
                    f.write(f"      {hdr}: {val[:60]}{'...' if len(val) > 60 else ''}\n")
            
            if head.get("error"):
                f.write(f"    ⚠️ HTTP Error: {head['error']}\n")
            
            # Статус скачивания
            download = check.get("download", {})
            f.write(f"\n⬇️ DOWNLOAD STATUS: {'✅ Success' if download.get('success') else '❌ Failed'}\n")
            if download.get("filename"):
                f.write(f"    Filename: {download['filename']}\n")
            if download.get("size_bytes"):
                f.write(f"    Size: {download['size_bytes']} bytes\n")
            if download.get("error"):
                f.write(f"    Error: {download['error']}\n")
        
        # Сводная статистика
        f.write(f"\n\n{'=' * 80}\n")
        f.write("SUMMARY STATISTICS\n")
        f.write(f"{'=' * 80}\n")
        
        total = len(server_ping_data)
        successful = sum(1 for c in server_ping_data if c.get("http_head", {}).get("success"))
        test_count = sum(1 for c in server_ping_data if c.get("is_test"))
        
        f.write(f"Total checks: {total}\n")
        f.write(f"Successful: {successful} ({successful/total*100:.1f}%)\n")
        f.write(f"Failed: {total - successful}\n")
        f.write(f"Test scenarios: {test_count}\n")
        f.write(f"Production: {total - test_count}\n")
        
        # Статистика по типам ошибок
        error_types = {}
        for c in server_ping_data:
            err = c.get("http_head", {}).get("error")
            if err:
                err_type = err.split(":")[0].strip() if ":" in err else "Unknown"
                error_types[err_type] = error_types.get(err_type, 0) + 1
        
        if error_types:
            f.write(f"\nError breakdown:\n")
            for err_type, count in sorted(error_types.items(), key=lambda x: -x[1]):
                f.write(f"    {err_type}: {count}\n")
    
    logger.info(f"Detailed ping report saved to {SERVER_PING_FILE}")


def save_error_log_json(errors):
    """
    Задание 3: JSON лог ошибок (для Streamlit)
    """
    output = {
        "report_metadata": {
            "generated_at": datetime.now().isoformat(),
            "pipeline": "listing_TyapkinaPA_Rocket",
            "variant": 16,
            "author": "Tyapkina P.A."
        },
        "summary": {
            "total_errors": len(errors),
            "test_errors": sum(1 for e in errors if e.get("is_test_scenario")),
            "production_errors": sum(1 for e in errors if not e.get("is_test_scenario")),
            "by_type": {}
        },
        "errors": errors
    }
    
    for err in errors:
        err_type = err.get("error_type", "Unknown")
        output["summary"]["by_type"][err_type] = output["summary"]["by_type"].get(err_type, 0) + 1
    
    with open(ERROR_LOG_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Error log JSON saved to {ERROR_LOG_JSON}")


def save_error_log_txt(errors):
    """
    Задание 3: Человекочитаемый лог ошибок
    """
    with open(ERROR_LOG_TXT, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("ERROR LOG (Human-readable) | Variant 16\n")
        f.write("=" * 70 + "\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Pipeline: listing_TyapkinaPA_Rocket\n\n")
        
        if not errors:
            f.write("✅ No errors recorded during this run.\n")
        else:
            for i, err in enumerate(errors, 1):
                test_tag = "[TEST] " if err.get("is_test_scenario") else ""
                f.write(f"{i}. {test_tag}[{err['error_type']}] {err['task_id']}\n")
                f.write(f"   Time: {err['timestamp']}\n")
                f.write(f"   URL: {err['url']}\n")
                f.write(f"   Message: {err['error_message']}\n\n")
    
    logger.info(f"Error log TXT saved to {ERROR_LOG_TXT}")


# ============================================
# 6. ОСНОВНАЯ ФУНКЦИЯ ОБРАБОТКИ
# ============================================

def process_rockets():
    """
    Основная функция пайплайна
    Реализует все 3 задания Варианта 16
    """
    logger.info("🚀 Starting pipeline: listing_TyapkinaPA_Rocket")
    
    # Подготовка директорий
    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    
    # Инициализация структур данных
    domain_counts = {}
    server_status = {}
    server_ping_data = []
    errors = []
    
    # === ЧАСТЬ 1: Обработка реальных данных из API ===
    logger.info("📥 Loading launch data from JSON...")
    
    try:
        with open(TMP_JSON_FILE, encoding="utf-8") as f:
            launches = json.load(f)
        logger.info(f"✅ Loaded {len(launches.get('results', []))} launches")
    except json.JSONDecodeError as e:
        log_error(errors, "json_load", "N/A", "JSONDecodeError", str(e), False)
        raise RuntimeError(f"Failed to parse JSON: {e}")
    except FileNotFoundError as e:
        log_error(errors, "json_load", "N/A", "FileNotFoundError", str(e), False)
        raise RuntimeError(f"JSON file not found: {e}")
    
    # Извлечение URL изображений
    image_urls = []
    for launch in launches.get("results", []):
        img = launch.get("image")
        if isinstance(img, dict) and img.get("image_url"):
            image_urls.append(img["image_url"])
        elif isinstance(img, str) and img:
            image_urls.append(img)
    
    # Уникальные URL, максимум MAX_IMAGES
    image_urls = list(dict.fromkeys(image_urls))[:MAX_IMAGES]
    logger.info(f"🖼️ Found {len(image_urls)} unique image URLs")
    
    # Обработка каждого URL
    for idx, url in enumerate(image_urls, 1):
        process_single_url(url, domain_counts, server_status, server_ping_data, errors, f"real_{idx}")
    
    # === ЧАСТЬ 2: Тестовые сценарии (для демонстрации ошибок) ===
    if ENABLE_TEST_ERRORS:
        logger.info("🧪 Running test scenarios for error demonstration...")
        for scenario in TEST_SCENARIOS:
            process_single_url(
                scenario["url"],
                domain_counts,
                server_status,
                server_ping_data,
                errors,
                f"test_{scenario['description']}",
                is_test=True
            )
        logger.info(f"✅ Completed {len(TEST_SCENARIOS)} test scenarios")
    
    # === Сохранение всех отчётов ===
    logger.info("💾 Saving reports...")
    save_domain_report(domain_counts)
    save_server_status_brief(server_status)
    save_server_ping_detailed(server_ping_data)
    save_error_log_json(errors)
    save_error_log_txt(errors)
    
    # Итоговая статистика
    result = {
        "domains": len(domain_counts),
        "requests": sum(domain_counts.values()),
        "images_downloaded": sum(1 for c in server_ping_data if c.get("download", {}).get("success")),
        "errors": len(errors),
        "test_errors": sum(1 for e in errors if e.get("is_test_scenario"))
    }
    
    logger.info(f"✅ Pipeline completed: {result}")
    return result


# ============================================
# 7. ЗАДАЧА PROCESS И ЦЕПОЧКА ВЫПОЛНЕНИЯ
# ============================================

process_task = PythonOperator(
    task_id="process_rockets_variant16",
    python_callable=process_rockets,
    dag=dag,
)

# Task 4: Финальное уведомление
notify = BashOperator(
    task_id="notify_completion",
    bash_command=(
        f'echo "✅ Pipeline completed successfully!" && '
        f'echo "Files generated:" && '
        f'ls -la {DATA_DIR}/*.txt {DATA_DIR}/*.json 2>/dev/null && '
        f'echo "Images:" && ls {IMAGES_DIR}/ 2>/dev/null | wc -l'
    ),
    dag=dag,
)

# Порядок выполнения задач
clean_data >> download_json >> process_task >> notify
