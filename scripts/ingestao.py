import requests
import json
from minio import Minio
from io import BytesIO

# conexão com o MinIO
cliente_minio = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

# busca dados de municípios do Paraná na API do IBGE
print("Buscando dados do IBGE...")
resposta = requests.get(
    "https://servicodados.ibge.gov.br/api/v1/localidades/estados/PR/municipios"
)
dados = resposta.json()
print(f"{len(dados)} municípios encontrados")

# converte para bytes para salvar no MinIO
conteudo = json.dumps(dados, ensure_ascii=False, indent=2).encode("utf-8")
conteudo_bytes = BytesIO(conteudo)
tamanho = len(conteudo)

# salva no bucket bronze
cliente_minio.put_object(
    bucket_name="bronze",
    object_name="ibge/municipios_pr.json",
    data=conteudo_bytes,
    length=tamanho,
    content_type="application/json"
)

print("Arquivo salvo em bronze/ibge/municipios_pr.json")