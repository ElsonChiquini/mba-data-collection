# -*- coding: utf-8 -*-
"""
Pipeline - Camada Bronze (Raw)
Responsável por:
1️⃣ Extrair dados do PostgreSQL (db_loja)
2️⃣ Copiar arquivos JSON locais (Fonte 2)
3️⃣ Fazer requisição de API pública (Fonte 3)
4️⃣ Salvar todos em MinIO sob o caminho data-ingest/bronze/...
"""

import os
import json
import pandas as pd
import boto3
import psycopg2
import requests
from datetime import date
from io import StringIO

# ==============================
# CONFIGURAÇÕES GERAIS
# ==============================
DATA_INGESTAO = str(date.today())
BUCKET = "data-ingest"
PREFIX = f"bronze/"
ENDPOINT = "http://minio:9001"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

# Cliente MinIO
s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1"
)

# ==============================
# 1️⃣ FONTE 1 - PostgreSQL (db_loja)
# ==============================
print("📦 Iniciando ingestão PostgreSQL...")

conn = psycopg2.connect(
    host="localhost",
    database="db_loja",
    user="postgres",
    password="postgres"
)

full_tables = ["categorias_produto", "cliente", "pedido_cabecalho", "pedido_itens"]
incremental_table = "produto"

# FULL LOAD
for tbl in full_tables:
    df = pd.read_sql(f"SELECT * FROM {tbl}", conn)
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    path = f"{PREFIX}dbloja/{tbl}/data_ingestao={DATA_INGESTAO}/{tbl}.csv"
    s3.put_object(Bucket=BUCKET, Key=path, Body=buffer.getvalue().encode("utf-8"))
    print(f"✅ {tbl} (Full Load) -> {path}")

# INCREMENTAL
df_prod = pd.read_sql("SELECT * FROM produto WHERE data_atualizacao >= CURRENT_DATE - INTERVAL '1 day';", conn)
buffer = StringIO()
df_prod.to_csv(buffer, index=False)
path = f"{PREFIX}dbloja/produto/data_ingestao={DATA_INGESTAO}/delta_produtos.csv"
s3.put_object(Bucket=BUCKET, Key=path, Body=buffer.getvalue().encode("utf-8"))
print(f"✅ produto (Incremental) -> {path}")

conn.close()

# ==============================
# 2️⃣ FONTE 2 - JSONs locais
# ==============================
print("\n📁 Iniciando ingestão de arquivos JSON locais...")

json_files = {
    "extratos": "dados_extrato.json",
    "pedidos_externos": "dados_pedidos.json",
    "produtos_parceiros": "dados_produtos.json",
    "tags_produtos": "dados_tags.json"
}

for key, fname in json_files.items():
    if os.path.exists(f"/workspace/data/{fname}"):
        with open(f"/workspace/data/{fname}", "rb") as f:
            path = f"{PREFIX}json/{key}/data_ingestao={DATA_INGESTAO}/{fname}"
            s3.put_object(Bucket=BUCKET, Key=path, Body=f)
            print(f"✅ {fname} -> {path}")
    else:
        print(f"⚠️ Arquivo {fname} não encontrado, pulando...")

# ==============================
# 3️⃣ FONTE 3 - API Pública (IBGE)
# ==============================
print("\n🌎 Iniciando ingestão da API BrasilAPI...")

url = "https://brasilapi.com.br/api/ibge/uf/v1"
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    path = f"{PREFIX}api/ibge_uf/data_ingestao={DATA_INGESTAO}/uf.json"
    s3.put_object(Bucket=BUCKET, Key=path, Body=json_bytes)
    print(f"✅ API IBGE -> {path}")
else:
    print(f"❌ Erro na API IBGE: {response.status_code}")

print("\n🏁 Ingestão Bronze finalizada.")
