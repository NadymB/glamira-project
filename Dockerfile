FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN pip install poetry==1.8.3 && \
    poetry export -f requirements.txt --output requirements.txt --without-hashes

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app/src