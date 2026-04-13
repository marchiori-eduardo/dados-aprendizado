from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import pandas as pd
from minio import Minio
from io import BytesIO

def get_minio():
    # centraliza a conexão com o MinIO para não repetir configuração
    # dentro do Docker os containers se comunicam pelo nome do serviço
    return Minio(
        "minio:9000",
        access_key="admin",
        secret_key="admin123",
        secure=False
    )

def ingestao(data_interval_start=None, **context):
    # data_interval_start é injetado pelo Airflow automaticamente
    # representa a data lógica de execução da DAG
    data = context.get("data_interval_start") or datetime.now()
    particao = data.strftime("%Y-%m-%d")

    cliente = get_minio()

    print(f"Buscando dados do IBGE para partição {particao}...")
    resposta = requests.get(
        "https://servicodados.ibge.gov.br/api/v1/localidades/estados/PR/municipios"
    )
    dados = resposta.json()
    print(f"{len(dados)} municípios encontrados")

    conteudo = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")
    buffer = BytesIO(conteudo) # cria um buffer em memória para o conteúdo JSON

    # salva com partição por data — idempotente
    # rodar duas vezes no mesmo dia sobrescreve, não duplica
    caminho = f"ibge/{particao}/municipios_pr.json"
    cliente.put_object(
        bucket_name="bronze",
        object_name=caminho,
        data=buffer,
        length=len(conteudo),
        content_type="application/json"
    )
    print(f"Salvo em bronze/{caminho}")

    # passa o caminho para a próxima tarefa via XCom
    # XCom é o mecanismo do Airflow para tarefas trocarem dados entre si
    return caminho

def transformacao(**context):
    # recupera o caminho gerado pela tarefa de ingestão via XCom
    ti = context["ti"]
    caminho_bronze = ti.xcom_pull(task_ids="ingestao")

    # extrai a partição de data do caminho para manter consistência
    particao = caminho_bronze.split("/")[1]

    cliente = get_minio()

    print(f"Lendo Bronze: bronze/{caminho_bronze}")
    resposta = cliente.get_object("bronze", caminho_bronze)
    dados = json.loads(resposta.read().decode("utf-8"))

    df = pd.DataFrame(dados)

    # achata colunas aninhadas — os dados do IBGE têm objetos dentro de objetos
    df_limpo = pd.DataFrame({
        "id":              df["id"],
        "nome":            df["nome"],
        "microrregiao":    df["microrregiao"].apply(lambda x: x["nome"]),
        "mesorregiao":     df["microrregiao"].apply(lambda x: x["mesorregiao"]["nome"]),
        "regiao_imediata": df["regiao-imediata"].apply(lambda x: x["nome"]),
    })

    # data quality — valida antes de salvar
    nulos = df_limpo.isnull().sum().sum()
    duplicatas = df_limpo.duplicated().sum()
    print(f"Nulos: {nulos} | Duplicatas: {duplicatas}")
    print(f"Shape final: {df_limpo.shape}")

    # salva em Parquet com a mesma partição de data da camada Bronze
    buffer = BytesIO()
    df_limpo.to_parquet(buffer, index=False)
    buffer.seek(0)

    caminho_silver = f"ibge/{particao}/municipios_pr.parquet"
    cliente.put_object(
        bucket_name="silver",
        object_name=caminho_silver,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    print(f"Salvo em silver/{caminho_silver}")

with DAG(
    dag_id="pipeline_municipios_pr",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    # doc_md aparece na interface do Airflow como documentação da DAG
    doc_md="""
    ## Pipeline Municípios PR
    Ingere municípios do Paraná via API do IBGE e transforma para camada Silver.
    - **Bronze**: JSON bruto particionado por data
    - **Silver**: Parquet limpo particionado por data
    - **Idempotência**: reexecuções sobrescrevem a partição do dia sem duplicar
    """,
) as dag:

    tarefa_ingestao = PythonOperator(
        task_id="ingestao",
        python_callable=ingestao,
    )

    tarefa_transformacao = PythonOperator(
        task_id="transformacao",
        python_callable=transformacao,
    )

    # define a ordem de execução — transformação só roda após ingestão bem-sucedida
    tarefa_ingestao >> tarefa_transformacao