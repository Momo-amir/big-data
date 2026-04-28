FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .

CMD ["python", "main.py"]
