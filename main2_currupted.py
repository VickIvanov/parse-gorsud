import os
import shutil
from collections import defaultdict
from pathlib import Path
from datetime import datetime
import re

# ================== КОНФИГУРАЦИЯ ==================
SOURCE_DIR = Path("data/test")
DESTINATION_DIR = Path("data/output")
TEMP_DIR = Path("Z:/temp_files")  # Буферная папка для временных файлов
LOG_FILE = Path(f"logs/rename_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")


# ================== ФУНКЦИИ ==================

def setup_environment():
    """Создание необходимых папок"""
    try:
        SOURCE_DIR.mkdir(exist_ok=True, parents=True)
        DESTINATION_DIR.mkdir(exist_ok=True, parents=True)
        LOG_FILE.parent.mkdir(exist_ok=True, parents=True)

        # Проверяем существование буферной папки
        if not TEMP_DIR.exists():
            print(f"❌ Буферная папка не существует: {TEMP_DIR}")
            return False

        # Создаем лог-файл с заголовками
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write("timestamp,operation,original_name,temp_name,new_name,status,error\n")
        return True
    except Exception as e:
        print(f"❌ Ошибка инициализации: {e}")
        return False


def log_action(operation, original_name, temp_name="", new_name="", status="", error=""):
    """Логирование действий"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f'"{timestamp}","{operation}","{original_name}","{temp_name}","{new_name}","{status}","{error}"\n')


def clean_filename(filename):
    """Очистка имени файла от проблемных символов"""
    # Получаем имя без расширения
    stem = Path(filename).stem

    # Удаляем пробелы в начале и конце
    stem = stem.strip()

    # Заменяем точки на подчеркивания
    stem = stem.replace('.', '_')

    # Заменяем проблемные символы
    forbidden_chars = {
        '<': '(',
        '>': ')',
        ':': '_',
        '"': "'",
        '/': '_',
        '\\': '_',
        '|': '_',
        '?': '_',
        '*': '_',
        '\t': ' ',
        '\n': ' ',
        '\r': ' '
    }

    for char, replacement in forbidden_chars.items():
        stem = stem.replace(char, replacement)

    # Убираем множественные пробелы и подчеркивания
    stem = re.sub(r'\s+', ' ', stem)
    stem = re.sub(r'_+', '_', stem)

    # Ограничиваем длину
    if len(stem) > 200:
        stem = stem[:200]

    # Если имя стало пустым, даем ему стандартное имя
    if not stem:
        stem = "document"

    return stem + '.docx'


def get_source_files_info():
    """Получает информацию о файлах в исходной папке"""
    files_info = []

    try:
        print("\n📂 Сканирование исходной директории...")

        if not SOURCE_DIR.exists():
            print(f"❌ Исходная директория не существует: {SOURCE_DIR}")
            return []

        # Используем scandir для более эффективного получения метаданных
        for entry in os.scandir(SOURCE_DIR):
            if entry.is_file():
                try:
                    # Получаем метаданные файла
                    file_stat = entry.stat()
                    file_info = {
                        'path': entry.path,
                        'name': entry.name,
                        'size': file_stat.st_size,
                        'mtime': file_stat.st_mtime
                    }

                    print(f"  • Файл: '{entry.name}'")
                    print(f"    - Размер: {file_info['size']} байт ({file_info['size'] / 1024:.2f} КБ)")
                    print(f"    - Изменён: {datetime.fromtimestamp(file_info['mtime'])}")

                    files_info.append(file_info)
                except Exception as e:
                    print(f"  ⚠️ Ошибка при получении информации о файле {entry.name}: {e}")

        print(f"📊 Всего найдено файлов: {len(files_info)}")
        return files_info

    except Exception as e:
        print(f"❌ Ошибка при сканировании исходной директории: {e}")
        log_action("SCAN", "", "", "", "ERROR", str(e))
        return []


def create_buffer_files_map(buffer_dir):
    """Создает словарь файлов из буферной папки с ключами (размер, время_модификации)"""
    print(f"🔄 Индексирование файлов в буферной папке {buffer_dir}...")
    buffer_files_map = defaultdict(list)
    files_count = 0

    try:
        # Используем os.scandir для эффективного обхода директории
        for entry in os.scandir(buffer_dir):
            if entry.is_file():
                try:
                    file_stat = entry.stat()
                    size = file_stat.st_size
                    # Округляем время до секунд для более надежного сравнения
                    mtime_rounded = round(file_stat.st_mtime)
                    buffer_files_map[(size, mtime_rounded)].append(entry.path)
                    files_count += 1

                    # Индикатор прогресса для больших директорий
                    if files_count % 1000 == 0:
                        print(f"  • Проиндексировано файлов: {files_count}")

                except Exception as e:
                    print(f"  ⚠️ Ошибка при индексировании файла {entry.name}: {e}")

        print(f"✅ Индексирование завершено. Всего файлов: {files_count}")
        return buffer_files_map
    except Exception as e:
        print(f"❌ Ошибка при индексировании буферной папки: {e}")
    return buffer_files_map


def find_matching_file_in_buffer(source_file, buffer_files_map):
    """Находит соответствующий файл в буфере по размеру и времени модификации"""
    try:
        # Проверяем, что нам передали - путь к файлу или словарь с метаданными
        if isinstance(source_file, dict):
            # Если словарь, берем размер и время из него
            size = source_file['size']
            mtime_rounded = round(source_file['mtime'])
        else:
            # Если путь к файлу, получаем метаданные
            file_stat = os.stat(source_file)
            size = file_stat.st_size
            mtime_rounded = round(file_stat.st_mtime)

        # Прямое соответствие по размеру и времени
        key = (size, mtime_rounded)
        if key in buffer_files_map and buffer_files_map[key]:
            matching_file = buffer_files_map[key][0]
            # Удаляем использованный файл из списка, чтобы не использовать его повторно
            buffer_files_map[key].pop(0)
            return matching_file

        # Если точного совпадения нет, ищем по близости размера (±1%)
        # и времени модификации (±10 секунд)
        for potential_key, files in buffer_files_map.items():
            if not files:
                continue

            potential_size, potential_time = potential_key

            # Проверяем близость размера (в пределах 1%)
            size_diff_percent = abs(potential_size - size) / max(size, 1) * 100
            time_diff_seconds = abs(potential_time - mtime_rounded)

            if size_diff_percent <= 1 and time_diff_seconds <= 10:
                matching_file = files[0]
                files.pop(0)
                return matching_file

        # Файл не найден
        return None

    except Exception as e:
        print(f"❌ Ошибка при поиске в буферной папке: {e}")
        return None


def process_files(source_dir, buffer_dir, output_dir, test_mode=False):
    """Обрабатывает файлы в исходной папке, находит соответствия в буфере и копирует их"""
    # Создаем словарь файлов из буферной папки
    print(f"🔄 Создание карты файлов из буферной папки...")
    buffer_files_map = create_buffer_files_map(buffer_dir)
    print(f"📊 Проиндексировано файлов в буфере: {sum(len(files) for files in buffer_files_map.values())}")

    # Получаем информацию о файлах
    source_files_info = get_source_files_info()
    print(f"📄 Файлов для обработки: {len(source_files_info)}")

    matched_count = 0
    processed_count = 0

    # Обрабатываем каждый файл
    for file_info in source_files_info:
        filename = file_info['name']

        # Если тестовый режим, обрабатываем только первый файл
        if test_mode and processed_count > 0:
            break

        print(f"\n🔍 Поиск соответствия для файла '{filename}'")
        print(f"  • Размер оригинала: {file_info['size']} байт")
        print(f"  • Время модификации: {datetime.fromtimestamp(file_info['mtime'])}")

        matching_file = find_matching_file_in_buffer(file_info, buffer_files_map)

        if matching_file:
            # Создаем выходную директорию, если она не существует
            Path(output_dir).mkdir(exist_ok=True, parents=True)

            # Создаем очищенное имя файла
            clean_name = clean_filename(filename)
            new_filepath = Path(output_dir) / clean_name

            # Если файл с таким именем уже существует, добавляем счетчик
            counter = 1
            while new_filepath.exists():
                name_stem = Path(clean_name).stem
                name_stem = re.sub(r'_\d+$', '', name_stem)  # Удаляем предыдущие счетчики
                new_name = f"{name_stem}_{counter}.docx"
                new_filepath = Path(output_dir) / new_name
                counter += 1

            # Копируем файл из буфера с очищенным именем
            shutil.copy2(matching_file, new_filepath)
            matched_count += 1
            print(f"✅ Файл найден и скопирован: {matching_file} -> {new_filepath}")
            log_action("COPY", filename, Path(matching_file).name, new_filepath.name, "SUCCESS")
        else:
            print(f"❌ Не найдено соответствия для файла {filename}")
            log_action("MATCH", filename, "", "", "ERROR", "Файл не найден в буфере")

        processed_count += 1

    print(f"\n📊 Итоги: обработано {processed_count} файлов, найдено соответствий: {matched_count}")
    return matched_count


def main():
    """Основная функция"""
    print(f"🔄 Начало обработки файлов: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Исходная папка: {SOURCE_DIR}")
    print(f"📁 Целевая папка: {DESTINATION_DIR}")
    print(f"📁 Буферная папка: {TEMP_DIR}")

    # Подготавливаем окружение
    if not setup_environment():
        return

    # Запускаем в тестовом режиме (только первый файл) или полный режим
    test_mode = False  # Измените на True для обработки только первого файла
    print(
        f"🧪 {'Тестовый режим: обрабатываем только первый файл...' if test_mode else 'Полный режим: обрабатываем все файлы...'}")

    # Обрабатываем файлы
    start_time = datetime.now()
    matched_count = process_files(SOURCE_DIR, TEMP_DIR, DESTINATION_DIR, test_mode=test_mode)
    end_time = datetime.now()

    print(f"⏱️ Время выполнения: {(end_time - start_time).total_seconds():.2f} секунд")
    print(f"\n📝 Детали в лог-файле: {LOG_FILE}")
    print(f"🏁 Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()