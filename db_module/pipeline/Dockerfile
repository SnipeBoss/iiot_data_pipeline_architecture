# Airflow image — deliberately lightweight. We DO NOT ship Java or ojdbc8.jar
# here because DAGs never talk to Oracle directly; they call the FastAPI
# Oracle service at ORACLE_API_URL (default http://host.docker.internal:8000).
FROM apache/airflow:2.8.0-python3.11

USER airflow

COPY requirements.txt /tmp/pipeline-requirements.txt
RUN pip install --no-cache-dir -r /tmp/pipeline-requirements.txt
