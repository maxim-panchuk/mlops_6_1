# Используем образ с PySpark + Python
FROM bitnami/spark:latest

USER root

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .


# Стартуем пайплайн
CMD ["python3", "main.py"]
