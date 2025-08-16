import os
import json
import requests
import psycopg2
from datetime import datetime
from tqdm import tqdm
import re
import urllib.parse

# Конфигурация
DB_CONFIG = {
    "dbname": "mydb",
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "port": "5432"
}
TARGET_CATEGORY_PART = "долев"
JSON_FILES_DIR = "data/json_zip"
DOWNLOAD_DIR = "data/json_new_files"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def log_message(message, level="INFO"):
    """Логирование с уровнем важности"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}")


def download_file(url, save_path):
    """Скачивание файла с прогресс-баром"""
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        progress = tqdm(total=total_size, unit='B', unit_scale=True,
                        desc=f"Скачивание {os.path.basename(save_path)}",
                        leave=False)

        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    progress.update(len(chunk))
        progress.close()
        return True
    except Exception as e:
        log_message(f"Ошибка скачивания {url}: {str(e)}", "WARNING")
        return False


def get_filename_from_url(url, attachment_name):
    """Генерирует имя файла"""
    try:
        with requests.head(url, allow_redirects=True, timeout=5) as r:
            if 'content-disposition' in r.headers:
                content_disp = r.headers['content-disposition']
                filename = re.findall('filename="?([^"]+)"?', content_disp)[0]
                return filename

        if attachment_name:
            ext = os.path.splitext(url)[1] or '.pdf'
            return f"{attachment_name}{ext}"

        return f"doc_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    except:
        return f"doc_{datetime.now().strftime('%Y%m%d%H%M%S')}"


def process_json_file(file_path, force_download=False):
    """Обработка JSON файла с возможностью принудительного скачивания"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        stats = {
            'total': 0,
            'valid_json': 0,
            'matching_category': 0,
            'has_attachments': 0,
            'valid_links': 0,
            'new_links': 0,
            'existing_links': 0
        }

        with open(file_path, 'r', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {os.path.basename(file_path)}"):
                if not line.strip() or line.strip() == ',':
                    continue

                stats['total'] += 1

                try:
                    json_str = line.lstrip(',')
                    item = json.loads(json_str)
                    stats['valid_json'] += 1
                except json.JSONDecodeError:
                    continue

                if not isinstance(item, dict):
                    continue

                # Проверка категории
                category = str(item.get('category', '')).lower()
                if TARGET_CATEGORY_PART.lower() not in category:
                    continue
                stats['matching_category'] += 1

                # Обработка вложений
                attachments = item.get('attachments', [])
                if not attachments:
                    continue
                stats['has_attachments'] += 1

                for attachment in attachments:
                    if not isinstance(attachment, dict):
                        continue

                    link = attachment.get('link')
                    if not isinstance(link, str) or not link.startswith('http'):
                        continue
                    stats['valid_links'] += 1

                    try:
                        cursor.execute("SELECT 1 FROM saved_court_decisions WHERE url = %s LIMIT 1", (link,))
                        if force_download or cursor.fetchone() is None:
                            attachment_name = attachment.get('displayName', '')
                            filename = get_filename_from_url(link, attachment_name)
                            safe_filename = re.sub(r'[\\/*?:"<>|]', '_', filename)
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            final_filename = f"{timestamp}_{safe_filename}"
                            save_path = os.path.join(DOWNLOAD_DIR, final_filename)

                            if download_file(link, save_path):
                                cursor.execute(
                                    """INSERT INTO saved_court_decisions 
                                    (source_id, download_date, url, original_file_name, txt_file_name, created_at, updated_at) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (url) DO NOTHING""",
                                    (1, datetime.now(), link, final_filename,
                                     f"{final_filename}.txt", datetime.now(), datetime.now())
                                )
                                if cursor.rowcount > 0:
                                    stats['new_links'] += 1
                                    log_message(f"Добавлен документ: {final_filename}", "SUCCESS")
                                else:
                                    stats['existing_links'] += 1
                        else:
                            stats['existing_links'] += 1
                    except Exception as e:
                        log_message(f"Ошибка обработки ссылки {link}: {str(e)}", "ERROR")

                if stats['new_links'] % 10 == 0:
                    conn.commit()

        conn.commit()

        log_message(f"""
Итоговая статистика:
- Всего строк: {stats['total']}
- Валидный JSON: {stats['valid_json']}
- Совпадение категории: {stats['matching_category']}
- Записи с вложениями: {stats['has_attachments']}
- Найдено ссылок: {stats['valid_links']}
- Новых документов добавлено: {stats['new_links']}
- Уже существующих ссылок: {stats['existing_links']}
""", "INFO")
        return stats['new_links']

    except Exception as e:
        log_message(f"Критическая ошибка: {str(e)}", "ERROR")
        if 'conn' in locals():
            conn.rollback()
        return 0
    finally:
        if 'conn' in locals():
            conn.close()


def main():
    json_files = [os.path.join(JSON_FILES_DIR, f)
                  for f in os.listdir(JSON_FILES_DIR)
                  if f.endswith('.json')]

    if not json_files:
        log_message("Нет JSON файлов для обработки", "WARNING")
        return

    log_message(f"Начало обработки {len(json_files)} файлов", "INFO")

    total_links = 0
    for file_path in json_files:
        log_message(f"Обработка файла: {file_path}", "INFO")
        links_added = process_json_file(file_path,
                                        force_download=False)  # Измените на True для принудительного скачивания
        total_links += links_added
        log_message(f"Добавлено ссылок: {links_added}", "INFO")

    log_message(f"Обработка завершена. Всего добавлено ссылок: {total_links}", "INFO")


if __name__ == "__main__":
    main()