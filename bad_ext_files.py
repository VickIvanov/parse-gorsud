import os
import shutil
import magic
from tqdm import tqdm
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# ================== КОНФИГУРАЦИЯ ==================
PROCESSED_DIR = Path("data/processed_files")  # Папка с исходными файлами
BAD_EXT_DIR = Path("data/bad_ext_files")  # Папка для файлов с плохими расширениями
LOG_FILE = Path("logs") / f"clean_duplicates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Словарь соответствия MIME-типов и расширений
MIME_TO_EXT = {
    'application/msword': '.doc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
    'application/rtf': '.rtf',
    'text/rtf': '.rtf'
}
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

def detect_file_type(filepath):
    """Определяет реальный тип файла"""
    try:
        mime = magic.Magic(mime=True).from_buffer(open(filepath, 'rb').read())
        return MIME_TO_EXT.get(mime)
    except Exception as e:
        print(f"Ошибка при определении типа файла {filepath.name}: {str(e)}")
        return None

def fix_file_extension(filepath):
    """Исправляет расширение файла на основе его реального типа"""
    real_ext = detect_file_type(filepath)
    if not real_ext:
        return None

    # Получаем базовое имя файла без расширения
    base_name = filepath.stem
    # Убираем возможные суффиксы вроде '.d', '.d.1' и т.д.
    base_name = base_name.split('.')[0]

    # Создаем новое имя файла с правильным расширением
    new_name = f"{base_name}{real_ext}"
    return filepath.parent / new_name

def process_files():
    """Основная логика обработки файлов"""
    if not PROCESSED_DIR.exists():
        print(f"Папка {PROCESSED_DIR} не существует")
        return

    # Создаем папку для логов
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(LOG_FILE, 'w', encoding='utf-8') as log:
        log.write(f"Лог обработки файлов {datetime.now()}\n")
        log.write(f"Папка с файлами: {PROCESSED_DIR}\n")
        log.write("=" * 50 + "\n")

        total_renamed = 0
        total_moved = 0

        for filepath in tqdm(list(PROCESSED_DIR.glob('*')), desc="Обработка файлов"):
            if not filepath.is_file():
                continue

            log.write(f"\nОбработка файла: {filepath.name}\n")

            # Если расширение уже корректное, пропускаем
            if filepath.suffix.lower() in VALID_EXTENSIONS:
                continue

            # Определяем правильное имя файла
            new_filepath = fix_file_extension(filepath)
            if new_filepath:
                try:
                    filepath.rename(new_filepath)
                    log.write(f"Переименован: {filepath.name} -> {new_filepath.name}\n")
                    total_renamed += 1
                except Exception as e:
                    log.write(f"Ошибка при переименовании: {str(e)}\n")
            else:
                # Если тип файла не определён, перемещаем в папку bad_ext
                BAD_EXT_DIR.mkdir(parents=True, exist_ok=True)
                target = BAD_EXT_DIR / filepath.name
                shutil.move(str(filepath), str(target))
                log.write(f"Перемещен в bad_ext: {filepath.name}\n")
                total_moved += 1

        log.write(f"\nИтого:\n")
        log.write(f"Переименовано файлов: {total_renamed}\n")
        log.write(f"Перемещено в bad_ext: {total_moved}\n")

    print(f"Готово! Переименовано: {total_renamed}, перемещено: {total_moved}. Лог сохранен в {LOG_FILE}")

# ================== ЗАПУСК СКРИПТА ==================

if __name__ == "__main__":
    print("=== Обработка файлов с некорректными расширениями ===")
    print(f"Исходная папка: {PROCESSED_DIR}")
    print(f"Поддерживаемые форматы: {', '.join(VALID_EXTENSIONS)}")

    process_files()

    print("\nРабота завершена.")