FROM apache/airflow:2.9.1

# instala as dependências como o próprio usuário airflow
# usando o caminho completo para evitar o bloqueio de segurança
USER airflow

RUN /home/airflow/.local/bin/pip install minio pandas pyarrow