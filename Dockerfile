FROM python:3.10

WORKDIR /app

COPY . /app


RUN pip install -r requirements.txt

CMD ["python3", "kafka_hapi_migrator/hapi_poster_simple.py"]