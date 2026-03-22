FROM python:3.12-slim

WORKDIR /app

COPY . .

# install poetry + export plugin
RUN pip install poetry && \
    poetry self add poetry-plugin-export

# export requirements
RUN poetry config virtualenvs.create false && \
    poetry export -f requirements.txt --output requirements.txt --without-hashes

CMD ["python", "src/main.py"]