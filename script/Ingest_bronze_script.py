# -*- coding: utf-8 -*-
"""
Extrai tabelas e dados do arquivo Script-DDL-dbloja.sql (PostgreSQL com schema db_loja)
e envia cada tabela como arquivo .parquet para o bucket 'raw' (pasta bronze/dbloja/data=YYYYMMDD/).
"""

from minio import Minio
from io import BytesIO
import re
import json
import pandas as pd
from datetime import datetime

# === CONFIGURAÇÕES ===
SQL_FILE = "sql/Script-DDL-dbloja.sql"
BUCKET_NAME = "data-ingest"   # bucket já existente
BRONZE_PREFIX = "bronze/dbloja"
SCHEMA = "db_loja"

# Conexão MinIO (igual ao seu ambiente funcional)
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
SECURE_CONNECTION = False


def parse_sql_file(sql_path):
    """Extrai tabelas e dados do SQL do PostgreSQL com schema db_loja."""
    with open(sql_path, "r", encoding="utf-8") as f:
        content = f.read()

    content = re.sub(r'--.*', '', content)
    content = re.sub(r'\s+', ' ', content)

    data = {}

    # CREATE TABLE
    create_table_pattern = rf"CREATE TABLE\s+(?:{SCHEMA}\.)?(\w+)\s*\((.*?)\);"
    for match in re.finditer(create_table_pattern, content, re.IGNORECASE):
        table = match.group(1)
        columns_block = match.group(2)
        columns = []
        parts = re.split(r",(?![^()]*\))", columns_block)
        for col_def in parts:
            col_def = col_def.strip()
            if not col_def or col_def.upper().startswith(("PRIMARY", "FOREIGN", "CONSTRAINT")):
                continue
            col_match = re.match(r'"?(\w+)"?\s+([\w\s\(\),]+)', col_def)
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2).strip()
                columns.append(col_name)
        data[table] = {"columns": columns, "rows": []}

    # INSERTS
    insert_pattern = rf"INSERT INTO\s+(?:{SCHEMA}\.)?(\w+)\s*\((.*?)\)\s*VALUES\s*(.*?);"
    for match in re.finditer(insert_pattern, content, re.IGNORECASE):
        table = match.group(1)
        cols_block = match.group(2)
        values_block = match.group(3)

        cols = [c.strip().strip('"') for c in cols_block.split(",")]

        tuples = re.findall(r"\((.*?)\)", values_block)
        for t in tuples:
            vals = [v.strip().strip("'") for v in re.split(r",(?![^']*')", t)]
            if table not in data:
                data[table] = {"columns": cols, "rows": []}
            row = dict(zip(cols, vals))
            data[table]["rows"].append(row)

    return data


def main():
    print("🧩 Extraindo tabelas do arquivo SQL...")
    data = parse_sql_file(SQL_FILE)

    if not data:
        print("⚠️ Nenhuma tabela encontrada.")
        return
    print(f"📦 {len(data)} tabelas encontradas.")

    # Conectar ao MinIO
    print("🚀 Conectando ao MinIO...")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=SECURE_CONNECTION
    )
    client.list_buckets()
    print("✅ Conexão estabelecida.")

    # Criar bucket, se necessário
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"🪣 Bucket '{BUCKET_NAME}' criado.")
    else:
        print(f"🪣 Bucket '{BUCKET_NAME}' já existe.")

    # Diretório de destino (ex: bronze/dbloja/data=20251025/)
    date_str = datetime.now().strftime("%Y%m%d")
    timestamp_str = datetime.now().strftime("%H%M%S")
    base_path = f"{BRONZE_PREFIX}/data={date_str}/"

    # Converter e enviar cada tabela
    for table, table_data in data.items():
        print(f"🔄 Processando tabela: {table}")
        df = pd.DataFrame(table_data["rows"])

        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        object_name = f"{base_path}{table}_{date_str}_{timestamp_str}.parquet"

        client.put_object(
            BUCKET_NAME,
            object_name,
            parquet_buffer,
            length=len(parquet_buffer.getvalue()),
            content_type="application/octet-stream"
        )

        print(f"✅ {table} enviada -> {object_name}")

    print("\n🏁 Todas as tabelas foram exportadas e enviadas para o MinIO com sucesso!")


if __name__ == "__main__":
    main()
