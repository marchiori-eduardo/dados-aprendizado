from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import pandas as pd
from minio import Minio
from io import BytesIO

def get_minio():
    return Minio(
        "minio:9000",
        access_key="admin",
        secret_key="admin123",
        secure=False
    )

def ingestao():
    cliente = get_minio()

    print("Buscando dados do IBGE...")
    resposta = requests.get(
        "https://servicodados.ibge.gov.br/api/v1/localidades/estados/PR/municipios"
    )
    dados = resposta.json()
    print(f"{len(dados)} municípios encontrados")

    conteudo = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")
    buffer = BytesIO(conteudo)

    cliente.put_object(
        bucket_name="bronze",
        object_name="ibge/municipios_pr.json",
        data=buffer,
        length=len(conteudo),
        content_type="application/json"
    )
    print("Salvo em bronze/ibge/municipios_pr.json")

def transformacao():
    cliente = get_minio()

    print("Lendo Bronze...")
    resposta = cliente.get_object("bronze", "ibge/municipios_pr.json")
    dados = json.loads(resposta.read().decode("utf-8"))

    df = pd.DataFrame(dados)

    df_limpo = pd.DataFrame({
        "id":              df["id"],
        "nome":            df["nome"],
        "microrregiao":    df["microrregiao"].apply(lambda x: x["nome"]),
        "mesorregiao":     df["microrregiao"].apply(lambda x: x["mesorregiao"]["nome"]),
        "regiao_imediata": df["regiao-imediata"].apply(lambda x: x["nome"]),
    })

    nulos = df_limpo.isnull().sum().sum()
    duplicatas = df_limpo.duplicated().sum()
    print(f"Nulos: {nulos} | Duplicatas: {duplicatas}")
    print(f"Shape final: {df_limpo.shape}")

    buffer = BytesIO()
    df_limpo.to_parquet(buffer, index=False)
    buffer.seek(0)

    cliente.put_object(
        bucket_name="silver",
        object_name="ibge/municipios_pr.parquet",
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    print("Salvo em silver/ibge/municipios_pr.parquet")

with DAG(
    dag_id="pipeline_municipios_pr",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    tarefa_ingestao = PythonOperator(
        task_id="ingestao",
        python_callable=ingestao,
    )

    tarefa_transformacao = PythonOperator(
        task_id="transformacao",
        python_callable=transformacao,
    )

    tarefa_ingestao >> tarefa_transformacao