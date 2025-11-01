# -- coding: utf-8 --

import boto3 
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
from datetime import datetime

# ============================================================
# CONFIGURAÇÕES
# ============================================================
BUCKET = "data-ingest"
BASE_PATH = "bronze/dbloja/"

# Configuração do banco de dados
DB_CONFIG = {
    "host": "db",
    "port": 5432,
    "database": "mydb",
    "user": "myuser",
    "password": "mypassword"
}

# Cria conexão SQLAlchemy
DB_URL = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DB_URL)

# ============================================================
# CONEXÃO MINIO
# ============================================================
def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1"
    )

s3 = get_minio_client()

# ============================================================
# ETAPA 1: CONFIGURAÇÃO DA EXECUÇÃO
# ============================================================
data_execucao = datetime.now().strftime("%Y%m%d")
WATERMARK_KEY = f"{BASE_PATH}data={data_execucao}/watermark_produto.txt"

# ============================================================
# FUNÇÕES DE SUPORTE
# ============================================================
def ler_watermark():
    """Lê a data da última atualização incremental salva no controle."""
    try:
        response = s3.get_object(Bucket=BUCKET, Key=WATERMARK_KEY)
        data = response["Body"].read().decode("utf-8").strip()
        print(f"🕒 Última marca d’água encontrada: {data}")
        return data
    except s3.exceptions.NoSuchKey:
        print("⚠️ Nenhuma marca d’água encontrada na pasta atual. Processamento inicial será executado.")
        return None

def salvar_watermark(data_str):
    """Salva/atualiza a marca d’água no diretório do lote atual."""
    s3.put_object(Bucket=BUCKET, Key=WATERMARK_KEY, Body=data_str.encode("utf-8"))
    print(f"💧 Marca d’água salva em: {WATERMARK_KEY} ({data_str})")

def executar_query(query, params=None):
    """Executa uma consulta SQL e retorna um DataFrame."""
    return pd.read_sql_query(query, engine, params=params)

def salvar_parquet_s3(df, tabela, data_execucao):
    """Salva um DataFrame no MinIO como arquivo parquet."""
    nome_arquivo = f"{tabela}_{data_execucao}_{datetime.now().strftime('%H%M%S')}.parquet"
    caminho = f"{BASE_PATH}data={data_execucao}/{nome_arquivo}"

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=caminho, Body=buffer.getvalue())

    print(f"💾 {tabela} salva com {len(df)} registros em: {caminho}")

# ============================================================
# ETAPA 2: PRODUTO (INCREMENTAL)
# ============================================================
marca_dagua_anterior = ler_watermark()

query_produto = """
    SELECT id, nome, descricao, preco, estoque, id_categoria,
           data_criacao, data_atualizacao
    FROM db_loja.produto
"""
if marca_dagua_anterior:
    query_produto += " WHERE data_atualizacao > %s"
    df_produto = executar_query(query_produto, (marca_dagua_anterior,))
else:
    df_produto = executar_query(query_produto)

if df_produto.empty:
    print("✅ Nenhum novo produto encontrado para incremento.")
else:
    salvar_parquet_s3(df_produto, "produto", data_execucao)
    nova_data = df_produto["data_atualizacao"].max().strftime("%Y-%m-%d %H:%M:%S")
    salvar_watermark(nova_data)

# ============================================================
# ETAPA 3: OUTRAS TABELAS (FULL LOAD)
# ============================================================
# 3.1 Categorias
df_categorias = executar_query("""
    SELECT id, nome, descricao
    FROM db_loja.categorias_produto
    ORDER BY id
""")
salvar_parquet_s3(df_categorias, "categorias_produto", data_execucao)

# 3.2 Clientes
df_clientes = executar_query("""
    SELECT id, nome, email, telefone, data_cadastro
    FROM db_loja.cliente
    ORDER BY id
""")
salvar_parquet_s3(df_clientes, "cliente", data_execucao)

# 3.3 Pedidos (cabeçalho)
df_pedidos = executar_query("""
    SELECT id, id_cliente, data_pedido, valor_total
    FROM db_loja.pedido_cabecalho
    ORDER BY id
""")
salvar_parquet_s3(df_pedidos, "pedido_cabecalho", data_execucao)

# 3.4 Itens de pedido
df_itens = executar_query("""
    SELECT id, id_pedido, id_produto, quantidade, preco_unitario
    FROM db_loja.pedido_itens
    ORDER BY id
""")
salvar_parquet_s3(df_itens, "pedido_itens", data_execucao)

# ============================================================
# FINALIZAÇÃO
# ============================================================
print("\n✅ Carga concluída com sucesso!")
print(f"📁 Estrutura gerada: bronze/dbloja/data={data_execucao}/")
print("📈 'produto' incremental e demais tabelas full load.")
