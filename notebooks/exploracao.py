import duckdb
from minio import Minio
import os
from datetime import datetime

# conecta no MinIO e baixa o parquet da camada silver
cliente = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

# cria pasta local temporária para o arquivo
os.makedirs("tmp", exist_ok=True)

# usa a data de hoje para encontrar o arquivo mais recente
data_hoje = datetime.now().strftime("%Y-%m-%d")
object_name = f"ibge/{data_hoje}/municipios_pr.parquet"
file_path = "tmp/municipios_pr.parquet"

# baixa o parquet do MinIO para uma pasta local temporária
cliente.fget_object(
    bucket_name="silver",
    object_name=object_name,
    file_path=file_path
)
print("Arquivo baixado com sucesso")

# conecta o DuckDB — sem servidor, roda direto na memória
con = duckdb.connect()

# consulta 1 — visão geral dos dados
print("\n--- Todos os municípios (primeiros 10) ---")
resultado = con.execute("""
    SELECT *
    FROM 'tmp/municipios_pr.parquet'
    LIMIT 10
""").fetchdf()
print(resultado)

# consulta 2 — quantidade de municípios por mesorregião
print("\n--- Municípios por mesorregião ---")
resultado2 = con.execute("""
    SELECT
        mesorregiao,
        COUNT(*) AS total_municipios
    FROM 'tmp/municipios_pr.parquet'
    GROUP BY mesorregiao
    ORDER BY total_municipios DESC
""").fetchdf()
print(resultado2)

# consulta 3 — municípios da região de Londrina
print("\n--- Municípios da região de Londrina ---")
resultado3 = con.execute("""
    SELECT nome, microrregiao, mesorregiao
    FROM 'tmp/municipios_pr.parquet'
    WHERE mesorregiao = 'Norte Central Paranaense'
    ORDER BY nome
""").fetchdf()
print(resultado3)