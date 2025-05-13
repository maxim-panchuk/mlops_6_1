# Используем образ с PySpark + Python
FROM bitnami/spark:latest

# Установка зависимостей
USER root
# RUN apt-get update && apt-get install -y curl openjdk-11-jdk

# Установка python-зависимостей
WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Копируем все файлы проекта
COPY . .


# Стартуем пайплайн
CMD ["python3", "main.py"]
