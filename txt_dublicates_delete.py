import os
import shutil
import hashlib
from pathlib import Path
import re
import logging
from tqdm import tqdm

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('duplicates_cleanup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
TXT_DIR = Path(r"D:\vick\pycharm\parse-gorsud-php\data_final\test_txt").resolve()
DUPLICATES_DIR = TXT_DIR.parent / (TXT_DIR.name + "_duplicates")
TXT_EXT = ".txt"


def clean_filename(name: str) -> str:
    """Очистка имени файла от ненужных символов"""
    # Убираем пробелы в начале и конце, лишние точки в конце, подчеркивание в начале
    name = name.strip().lstrip("_").rstrip(".").strip()
    # Убираем из конца скобки с цифрами, подчеркивания и цифры
    name = re.sub(r'[\s_]*\(?\d+\)?$', '', name)
    name = name.strip().rstrip(".")
    return name


def get_file_key(path: Path) -> tuple:
    """Получаем ключ для сравнения файлов: (очищенное имя без цифр, размер, хэш)"""
    name = path.stem
    cleaned = clean_filename(name)
    # Убираем цифры из конца очищенного имени для группировки
    base_name = re.sub(r'[\s_]*\d+$', '', cleaned)
    size = path.stat().st_size
    h = hashlib.md5(path.read_bytes()).hexdigest()
    return (base_name.lower(), size, h)


def find_duplicate_groups(txt_dir: Path) -> dict:
    """Находим группы дубликатов файлов"""
    logger.info(f"Поиск дубликатов в папке: {txt_dir}")

    files = list(txt_dir.glob(f"*{TXT_EXT}"))
    if not files:
        logger.warning("Нет файлов для обработки")
        return {}

    logger.info(f"Найдено {len(files)} файлов для анализа")

    # Собираем информацию о файлах с прогресс-баром
    file_groups = {}
    for file in tqdm(files, desc="Анализ файлов"):
        try:
            key = get_file_key(file)
            file_groups.setdefault(key, []).append(file)
        except Exception as e:
            logger.error(f"Ошибка обработки файла {file.name}: {e}")

    # Фильтруем только группы с дубликатами
    duplicate_groups = {k: v for k, v in file_groups.items() if len(v) > 1}

    logger.info(f"Найдено {len(duplicate_groups)} групп дубликатов")
    return duplicate_groups


def process_duplicates(duplicate_groups: dict, txt_dir: Path, duplicates_dir: Path):
    """Обрабатываем найденные дубликаты"""
    if not duplicate_groups:
        logger.info("Дубликаты не найдены")
        return

    duplicates_dir.mkdir(exist_ok=True)
    logger.info(f"Папка для дубликатов: {duplicates_dir}")

    total_duplicates = sum(len(files) - 1 for files in duplicate_groups.values())
    processed = 0

    for (base_name, size, _), files in tqdm(duplicate_groups.items(), desc="Обработка дубликатов"):
        try:
            # Сортируем файлы для детерминированного выбора основного
            files_sorted = sorted(files, key=lambda x: x.name)

            # Основной файл - первый в отсортированном списке
            main_file = files_sorted[0]
            new_name = f"{clean_filename(main_file.stem)}{TXT_EXT}"
            new_path = txt_dir / new_name

            # Переименовываем основной файл (если требуется)
            if main_file.name != new_name:
                # Проверяем, не существует ли уже файл с таким именем
                counter = 1
                while new_path.exists() and new_path != main_file:
                    new_name = f"{clean_filename(main_file.stem)}({counter}){TXT_EXT}"
                    new_path = txt_dir / new_name
                    counter += 1

                logger.info(f"Переименовываем: {main_file.name} -> {new_path.name}")
                main_file.rename(new_path)

            # Перемещаем дубликаты
            for duplicate in files_sorted[1:]:
                dest = duplicates_dir / duplicate.name
                # Убедимся, что имя уникально в папке дубликатов
                counter = 1
                while dest.exists():
                    stem = duplicate.stem
                    ext = duplicate.suffix
                    dest = duplicates_dir / f"{stem}({counter}){ext}"
                    counter += 1

                logger.info(f"Перемещаем дубликат: {duplicate.name} -> {dest}")
                shutil.move(str(duplicate), str(dest))
                processed += 1

        except Exception as e:
            logger.error(f"Ошибка обработки группы {base_name}: {e}")

    logger.info(f"Обработано дубликатов: {processed} из {total_duplicates}")


def main():
    logger.info("=" * 50)
    logger.info("Запуск обработки дубликатов")
    logger.info(f"Исходная папка: {TXT_DIR}")
    logger.info(f"Папка для дубликатов: {DUPLICATES_DIR}")

    if not TXT_DIR.exists():
        logger.error("Исходная папка не существует!")
        return

    # Шаг 1: Находим дубликаты
    duplicate_groups = find_duplicate_groups(TXT_DIR)

    # Шаг 2: Обрабатываем дубликаты
    process_duplicates(duplicate_groups, TXT_DIR, DUPLICATES_DIR)

    # Финализация
    remaining_files = len(list(TXT_DIR.glob(f"*{TXT_EXT}")))
    moved_files = len(list(DUPLICATES_DIR.glob(f"*{TXT_EXT}"))) if DUPLICATES_DIR.exists() else 0

    logger.info("=" * 50)
    logger.info(f"Обработка завершена")
    logger.info(f"Осталось файлов в исходной папке: {remaining_files}")
    logger.info(f"Перемещено дубликатов: {moved_files}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()