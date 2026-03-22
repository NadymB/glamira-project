FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

CMD ["python", "src/main.py"]