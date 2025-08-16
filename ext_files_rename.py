import os
import shutil
from tqdm import tqdm
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# ================== КОНФИГУРАЦИЯ ==================
PROCESSED_DIR = Path("data/processed_files")  # Папка с исходными файлами
BAD_EXT_DIR = Path("data/bad_ext_files")  # Папка для файлов с плохими расширениями
LOG_FILE = Path("logs") / f"clean_duplicates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Корректные расширения (должны быть в нижнем регистре)
VALID_EXTENSIONS = {'.doc', '.docx', '.rtf'}


# ================== ОСНОВНЫЕ ФУНКЦИИ ==================

def get_file_key(filepath):
    """Генерирует ключ для сравнения файлов (без учёта расширения)"""
    return filepath.stem.lower()  # Имя файла без расширения в нижнем регистре


def find_duplicate_files():
    """Находит файлы с одинаковыми именами, но разными расширениями"""
    files_by_key = defaultdict(list)

    # Собираем все файлы, группируя по ключу (имя без расширения)
    for filepath in PROCESSED_DIR.glob('*.*'):
        if filepath.is_file():
            key = get_file_key(filepath)
            files_by_key[key].append(filepath)

    # Фильтруем только группы с дубликатами
    duplicates = {k: v for k, v in files_by_key.items() if len(v) > 1}
    return duplicates


def select_valid_file(files):
    """Выбирает файл с правильным расширением из списка"""
    for filepath in files:
        if filepath.suffix.lower() in VALID_EXTENSIONS:
            return filepath
    return None  # Если нет файлов с правильным расширением


def move_bad_files(valid_file, bad_files):
    """Перемещает файлы с некорректными расширениями"""
    BAD_EXT_DIR.mkdir(parents=True, exist_ok=True)

    moved_files = []
    for filepath in bad_files:
        # Перемещаем только файлы с некорректными расширениями
        if filepath != valid_file and filepath.suffix.lower() not in VALID_EXTENSIONS:
            target = BAD_EXT_DIR / filepath.name
            shutil.move(str(filepath), str(target))
            moved_files.append((filepath.name, target))

    return moved_files


def process_duplicates():
    """Основная логика обработки дубликатов"""
    duplicates = find_duplicate_files()
    if not duplicates:
        print("Дубликаты не найдены.")
        return

    print(f"Найдено {len(duplicates)} групп дубликатов.")

    # Создаем папку для логов
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(LOG_FILE, 'w', encoding='utf-8') as log:
        log.write(f"Лог обработки дубликатов {datetime.now()}\n")
        log.write(f"Папка с файлами: {PROCESSED_DIR}\n")
        log.write(f"Папка для плохих файлов: {BAD_EXT_DIR}\n")
        log.write("=" * 50 + "\n")

        total_moved = 0

        for key, files in tqdm(duplicates.items(), desc="Обработка дубликатов"):
            valid_file = select_valid_file(files)
            if valid_file:
                bad_files = files  # Теперь передаем все файлы, фильтрация происходит в move_bad_files
                moved = move_bad_files(valid_file, files)

                log.write(f"Группа: {key}\n")
                log.write(f"Оставлен файл: {valid_file.name}\n")
                for orig, target in moved:
                    log.write(f"Перемещен: {orig} -> {target}\n")
                log.write("-" * 50 + "\n")

                total_moved += len(moved)

        log.write(f"\nИтого: перемещено {total_moved} файлов\n")

    print(f"Готово! Перемещено {total_moved} файлов. Лог сохранен в {LOG_FILE}")


# ================== ЗАПУСК СКРИПТА ==================

if __name__ == "__main__":
    print("=== Обработка дубликатов файлов ===")
    print(f"Исходная папка: {PROCESSED_DIR}")
    print(f"Корректные расширения: {', '.join(VALID_EXTENSIONS)}")

    process_duplicates()

    print("\nРабота завершена.")