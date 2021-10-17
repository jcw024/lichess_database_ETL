FROM python:3.9
RUN useradd --create-home --shell /bin/bash username
#apt-get update && apt-get install -y build-essential && 
#libpq-dev
WORKDIR /home/user
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
USER username
COPY --chown=username . .
COPY --chown=username ./src /opt/bitnami/airflow/dags
CMD ["bash"]
