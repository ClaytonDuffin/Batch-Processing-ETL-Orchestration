FROM apache/airflow:2.7.3

RUN pip install --no-cache-dir \
    psycopg2-binary \
    openmeteo_requests \
    requests_cache \
    retry_requests

COPY databaseOperations.py /databaseOperations.py

CMD ["python", "/databaseOperations.py"]