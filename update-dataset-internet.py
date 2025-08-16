import os
import requests
import zipfile
import datetime
import psycopg2
from tqdm import tqdm

# Конфигурация
DB_CONFIG = {
    "dbname": "mydb",
    "user": "myuser",
    "password": "mypassword",
    "host": "localhost",
    "port": "5432"
}
DOWNLOAD_DIR = "data/json_zip"
TEMP_DIR = "data/temp"
CHUNK_SIZE = 8192


def ensure_dirs():
    """Создает необходимые директории"""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)


def get_remaining_sources():
    """Получает все источники кроме первого"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT id, url FROM court_decision_sources ORDER BY id OFFSET 1")
        return cursor.fetchall()
    except Exception as e:
        print(f"Ошибка при получении источников: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def download_with_resume(url, file_path):
    """Скачивание файла с поддержкой докачки"""
    headers = {}
    file_size = 0

    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        headers = {'Range': f'bytes={file_size}-'}

    try:
        with requests.get(url, headers=headers, stream=True, timeout=30) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0)) + file_size
            mode = 'ab' if file_size > 0 else 'wb'

            with open(file_path, mode) as f, tqdm(
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    total=total_size,
                    initial=file_size,
                    desc=f"Скачивание {os.path.basename(file_path)}"
            ) as progress:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        progress.update(len(chunk))
        return True
    except Exception as e:
        print(f"Ошибка при скачивании: {e}")
        return False


def extract_zip(zip_path, extract_path):
    """Распаковка ZIP-архива"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file in tqdm(zip_ref.namelist(), desc="Распаковка"):
                zip_ref.extract(file, extract_path)
        return True
    except Exception as e:
        print(f"Ошибка при распаковке: {e}")
        return False


def update_source_status(source_id, status):
    """Обновляет статус источника в БД"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE court_decision_sources SET last_update = %s, update_status = %s WHERE id = %s",
            (datetime.datetime.now(), status, source_id)
        )
        conn.commit()
    except Exception as e:
        print(f"Ошибка обновления статуса: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


def process_source(source_id, url):
    """Обрабатывает один источник"""
    print(f"\nОбработка источника ID {source_id}: {url}")

    # Генерируем уникальное имя файла
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"dataset_{source_id}_{timestamp}.zip"
    zip_path = os.path.join(DOWNLOAD_DIR, file_name)
    extract_path = os.path.join(TEMP_DIR, f"dataset_{source_id}_{timestamp}")

    # Скачивание
    if not download_with_resume(url, zip_path):
        update_source_status(source_id, "download_failed")
        return False

    # Распаковка
    os.makedirs(extract_path, exist_ok=True)
    if not extract_zip(zip_path, extract_path):
        update_source_status(source_id, "extraction_failed")
        return False

    # Проверка результата
    files = os.listdir(extract_path)
    if not files:
        print("Ошибка: архив пуст")
        update_source_status(source_id, "empty_archive")
        return False

    update_source_status(source_id, "success")
    print(f"Успешно обработано. Файлов: {len(files)}")
    return True


def update_datasets():
    """Основная функция обновления датасетов"""
    ensure_dirs()
    sources = get_remaining_sources()

    if not sources:
        print("Нет источников для обработки")
        return False

    print(f"Найдено источников для обработки: {len(sources)}")

    for source_id, url in sources:
        if not process_source(source_id, url):
            print(f"Прерывание обработки источника ID {source_id}")
            # Можно добавить continue для перехода к следующему источнику

    print("Обработка всех источников завершена")
    return True


if __name__ == "__main__":
    update_datasets()