import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import os
import urllib.parse

# Конфигурация подключения
DB_CONFIG = {
    "dbname": "mydb",
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "port": "5432"
}

FILE_PATH = "data/data_json_2/links_allnew7.txt"
BATCH_SIZE = 1000  # Уменьшил размер батча для надежности
START_FROM = 165000  # Пропускаем первые 165000 записей


def get_default_source_id(cursor):
    """Получаем ID первого источника по умолчанию"""
    cursor.execute("SELECT id FROM court_decision_sources ORDER BY id LIMIT 1")
    return cursor.fetchone()[0]


def extract_filename_from_url(url):
    """Извлекаем имя файла из URL"""
    parsed = urllib.parse.urlparse(url)
    path = parsed.path
    filename = os.path.basename(path)
    return filename if filename else 'unnamed_file'


def insert_remaining_urls():
    print(f"Начало обработки (пропускаем первые {START_FROM} записей)...")

    if not os.path.exists(FILE_PATH):
        print(f"Ошибка: файл {FILE_PATH} не найден!")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        default_source_id = get_default_source_id(cursor)
        print(f"Используем source_id: {default_source_id}")

        total_inserted = 0
        batch = []
        current_time = datetime.now()

        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            # Пропускаем первые START_FROM строк
            for _ in range(START_FROM):
                next(f)

            print(f"Обрабатываем оставшиеся записи в файле {FILE_PATH}")

            for line_num, line in enumerate(f, START_FROM + 1):
                url = line.strip()
                if url and url.startswith('http'):
                    original_filename = extract_filename_from_url(url)
                    txt_filename = f"{os.path.splitext(original_filename)[0]}.txt"

                    # Явно указываем порядок полей
                    batch.append((
                        default_source_id,  # source_id (integer)
                        current_time,  # download_date (timestamp)
                        url,  # url (text)
                        original_filename,  # original_file_name (text)
                        txt_filename,  # txt_file_name (text)
                        current_time,  # created_at (timestamp)
                        current_time  # updated_at (timestamp)
                    ))

                    if len(batch) >= BATCH_SIZE:
                        execute_batch(
                            cursor,
                            """INSERT INTO saved_court_decisions 
                               (source_id, download_date, url, original_file_name, txt_file_name, created_at, updated_at) 
                               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                            batch
                        )
                        conn.commit()
                        total_inserted += len(batch)
                        print(f"Вставлено: {total_inserted} строк (всего обработано: {line_num})")
                        batch = []

            # Вставка оставшихся записей
            if batch:
                execute_batch(
                    cursor,
                    """INSERT INTO saved_court_decisions 
                       (source_id, download_date, url, original_file_name, txt_file_name, created_at, updated_at) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    batch
                )
                conn.commit()
                total_inserted += len(batch)

        print(f"Готово! Всего вставлено новых строк: {total_inserted}")

    except Exception as e:
        print(f"Ошибка: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    insert_remaining_urls()