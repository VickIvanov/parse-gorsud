import os
import time
import shutil
import subprocess
import hashlib
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from docx import Document

# ================== КОНФИГУРАЦИЯ ==================
SOURCE_DIR = Path("data/test")  # Папка с исходными файлами
TXT_DIR = Path("data/test_txt")  # Папка для текстовых файлов
PROCESSED_DIR = Path("data/processed_files")  # Папка для обработанных файлов
LOG_FILE = Path(f"logs/conversion_log_{int(time.time())}.csv")  # Лог-файл с timestamp
LOCK_FILE = Path("processing.lock")  # Файл блокировки


# ================== ОСНОВНЫЕ ФУНКЦИИ ==================

def setup_environment():
    """Инициализация папок и lock-файла"""
    try:
        # Создаем папки если их нет
        for folder in [SOURCE_DIR, TXT_DIR, PROCESSED_DIR]:
            folder.mkdir(exist_ok=True, parents=True)

        # Удаляем старый lock-файл
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()

        # Создаем новый lock-файл
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))

        # Инициализация лог-файла
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write("timestamp,human_time,operation,filename,details,filehash\n")

        return True
    except Exception as e:
        print(f"⛔ Ошибка инициализации: {e}")
        return False


def get_file_hash(filepath):
    """Вычисляет MD5 хеш файла"""
    try:
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except:
        return "error"


def log_error(filename, message, filepath=None):
    """Логирование ошибок с хешем файла"""
    timestamp, human_time = time.time(), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filehash = get_file_hash(filepath) if filepath else "none"
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{timestamp},{human_time},ERROR,{filename},{message},{filehash}\n")


def log_success(operation, filename, details="", filepath=None):
    """Логирование успешных операций"""
    timestamp, human_time = time.time(), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filehash = get_file_hash(filepath) if filepath else "none"
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{timestamp},{human_time},{operation},{filename},{details},{filehash}\n")


# ================== КОНВЕРТАЦИЯ ФАЙЛОВ ==================

def convert_docx(src_file, txt_file):
    """Конвертация DOCX в TXT"""
    try:
        doc = Document(src_file)
        text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(text)
        return True
    except Exception as e:
        log_error(src_file.name, f"DOCX error: {str(e)}", src_file)
        return False


def convert_doc_linux(src_file, txt_file):
    """Конвертация DOC в TXT для Linux/Mac"""
    try:
        # Пробуем antiword или catdoc
        for cmd in ['antiword', 'catdoc']:
            if shutil.which(cmd):
                result = subprocess.run([cmd, str(src_file)],
                                        stdout=subprocess.PIPE,
                                        encoding='utf-8',
                                        errors='ignore')
                if result.returncode == 0:
                    with open(txt_file, 'w', encoding='utf-8') as f:
                        f.write(result.stdout)
                    return True
        return False
    except Exception as e:
        log_error(src_file.name, f"DOC linux error: {str(e)}", src_file)
        return False


def convert_doc_windows(src_file, txt_file):
    """Конвертация DOC в TXT для Windows"""
    try:
        import win32com.client
        word = win32com.client.Dispatch("Word.Application")
        word.Visible = False
        doc = word.Documents.Open(str(src_file.resolve()))
        doc.SaveAs(str(txt_file.resolve()), FileFormat=2)  # 2 = TXT format
        doc.Close()
        word.Quit()
        return True
    except Exception as e:
        log_error(src_file.name, f"DOC Windows error: {str(e)}", src_file)
        return False


def convert_rtf(src_file, txt_file):
    """Конвертация RTF в TXT"""
    try:
        # Пробуем striprtf (кросс-платформенный)
        try:
            from striprtf.striprtf import rtf_to_text
            with open(src_file, 'r', encoding='utf-8', errors='ignore') as f:
                text = rtf_to_text(f.read())
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(text)
            log_success("RTF_CONVERT", src_file.name, "used striprtf", src_file)
            return True
        except ImportError:
            pass

        # Пробуем unrtf (Linux/Mac)
        if shutil.which('unrtf'):
            try:
                result = subprocess.run(['unrtf', '--text', str(src_file)],
                                        stdout=subprocess.PIPE,
                                        encoding='utf-8',
                                        errors='ignore')
                if result.returncode == 0:
                    text = result.stdout.split('----------')[0]  # Очистка вывода
                    with open(txt_file, 'w', encoding='utf-8') as f:
                        f.write(text)
                    log_success("RTF_CONVERT", src_file.name, "used unrtf", src_file)
                    return True
            except Exception as e:
                log_error(src_file.name, f"unrtf error: {str(e)}", src_file)

        # Ручная очистка RTF (последний вариант)
        try:
            import re
            with open(src_file, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
            clean_text = re.sub(r'\\[a-z]+[0-9]*|{[^}]+}|\\\'[0-9a-f]{2}', '', text)
            clean_text = re.sub(r'\n{3,}', '\n\n', clean_text)
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(clean_text)
            log_success("RTF_CONVERT", src_file.name, "used manual cleanup", src_file)
            return True
        except Exception as e:
            log_error(src_file.name, f"RTF manual error: {str(e)}", src_file)
            return False

    except Exception as e:
        log_error(src_file.name, f"RTF conversion error: {str(e)}", src_file)
        return False


def convert_to_txt(src_file, txt_file):
    """Главная функция конвертации"""
    ext = src_file.suffix.lower()

    if ext == '.docx':
        return convert_docx(src_file, txt_file)
    elif ext == '.doc':
        if os.name == 'nt':
            return convert_doc_windows(src_file, txt_file)
        else:
            return convert_doc_linux(src_file, txt_file)
    elif ext == '.rtf':
        return convert_rtf(src_file, txt_file)
    else:
        log_error(src_file.name, f"Unsupported format: {ext}", src_file)
        return False


# ================== ОБРАБОТКА ФАЙЛОВ ==================

def process_file(filepath):
    """Обработка одного файла"""
    try:
        # Пропускаем системные файлы
        if filepath.suffix.lower() not in ('.doc', '.docx', '.rtf'):
            return False

        # Создаем временную копию
        temp_file = TXT_DIR / f"temp_{filepath.name}"
        shutil.copy2(filepath, temp_file)

        # Конвертируем
        txt_file = TXT_DIR / f"{filepath.stem}.txt"
        if convert_to_txt(temp_file, txt_file):
            # Перемещаем оригинал
            processed_file = PROCESSED_DIR / filepath.name
            shutil.move(str(filepath), str(processed_file))
            log_success("CONVERTED", filepath.name, "", filepath)
            return True

        return False
    except Exception as e:
        log_error(filepath.name, f"Process error: {str(e)}", filepath)
        return False
    finally:
        # Удаляем временный файл
        if 'temp_file' in locals() and temp_file.exists():
            try:
                temp_file.unlink()
            except:
                pass


# ================== ЗАПУСК СКРИПТА ==================

def main():
    print(f"🔄 Запуск конвертации. Лог: {LOG_FILE}")
    if not setup_environment():
        return

    try:
        files = list(SOURCE_DIR.glob('*'))
        total_files = len(files)
        print(f"🔍 Найдено файлов: {total_files}")

        success_count = 0
        for file in tqdm(files, desc="Обработка"):
            if file.is_file():
                if process_file(file):
                    success_count += 1

        print(f"✅ Готово. Успешно: {success_count}/{total_files}")
    except KeyboardInterrupt:
        print("⛔ Прервано пользователем")
    except Exception as e:
        print(f"🔥 Ошибка: {e}")
    finally:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
        print(f"📊 Результаты сохранены в {LOG_FILE}")


if __name__ == "__main__":
    main()