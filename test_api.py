import os
import ast
from dotenv import load_dotenv
import requests

load_dotenv(encoding="windows-1251")

# Отключаем прокси из переменных окружения
for var in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    os.environ.pop(var, None)

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_API_URL = os.getenv("GROQ_API_URL", "https://api.groq.com/openai/v1/models")

headers = {
    "Authorization": f"Bearer {GROQ_API_KEY}"
}

try:
    response = requests.get(GROQ_API_URL, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()
    print("Ключ рабочий, доступные модели:", [m["id"] for m in data.get("data", [])])
except Exception as e:
    print("Ошибка при обращении к API:", e)