import pandas as pd
from minio import Minio
from io import BytesIO
import json

cliente_minio = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

# lê o arquivo bruto do bucket bronze
print("Lendo dados da camada Bronze...")
resposta = cliente_minio.get_object("bronze", "ibge/municipios_pr.json")
dados = json.loads(resposta.read().decode("utf-8"))

# transforma em DataFrame
df = pd.DataFrame(dados)

# achata as colunas aninhadas
df_limpo = pd.DataFrame({
    "id":               df["id"],
    "nome":             df["nome"],
    "microrregiao":     df["microrregiao"].apply(lambda x: x["nome"]),
    "mesorregiao":      df["microrregiao"].apply(lambda x: x["mesorregiao"]["nome"]),
    "regiao_imediata":  df["regiao-imediata"].apply(lambda x: x["nome"]),
})

# data quality — checa nulos
nulos = df_limpo.isnull().sum()
print("\nNulos por coluna:")
print(nulos)

# checa duplicatas
duplicatas = df_limpo.duplicated().sum()
print(f"\nLinhas duplicadas: {duplicatas}")

print(f"\nShape final: {df_limpo.shape}")
print(df_limpo.head())

# salva em Parquet no bucket silver
buffer = BytesIO()
df_limpo.to_parquet(buffer, index=False)
buffer.seek(0)

cliente_minio.put_object(
    bucket_name="silver",
    object_name="ibge/municipios_pr.parquet",
    data=buffer,
    length=buffer.getbuffer().nbytes,
    content_type="application/octet-stream"
)

print("\nArquivo salvo em silver/ibge/municipios_pr.parquet")