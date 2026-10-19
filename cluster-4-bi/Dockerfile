FROM apache/superset:3.0.2

USER root
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

USER superset
