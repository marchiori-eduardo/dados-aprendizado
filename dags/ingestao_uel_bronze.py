from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- 1. Definindo as Funções (O que as tarefas vão fazer) ---

def extrair_dados_portal():
    print("Iniciando a extração...")
    print("Simulando a leitura do CSV de Folha de Pagamento da UEL.")
    # Aqui entrará o seu código Pandas no futuro
    return "csv_lido_com_sucesso"

def carregar_dados_minio():
    print("Conectando ao Data Lake (MinIO)...")
    print("Salvando o arquivo CSV no bucket 'dados-brutos-uel'.")
    # Aqui entrará o seu código Boto3 (AWS S3) no futuro
    return "arquivo_salvo_na_camada_bronze"


# --- 2. Configurando a DAG (A rotina) ---

with DAG(
    dag_id='ingestao_dados_uel',
    description='Minha primeira DAG de Engenharia de Dados',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, # None significa que vamos rodar manualmente por enquanto
    catchup=False,
    tags=['uel', 'bronze', 'dados_abertos']
) as dag:

    # --- 3. Criando as Tarefas ---
    
    tarefa_extracao = PythonOperator(
        task_id='extrair_csv_portal_uel',
        python_callable=extrair_dados_portal
    )

    tarefa_carga = PythonOperator(
        task_id='enviar_para_minio',
        python_callable=carregar_dados_minio
    )

    # --- 4. Definindo a Ordem de Execução (O fluxo) ---
    # É aqui que a mágica da orquestração acontece!
    tarefa_extracao >> tarefa_carga