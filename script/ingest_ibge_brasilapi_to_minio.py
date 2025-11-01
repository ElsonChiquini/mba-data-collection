# -*- coding: utf-8 -*-
"""
Ingestão da API pública BrasilAPI (IBGE-UF)
-> https://brasilapi.com.br/api/ibge/uf/v1

Salva o resultado em JSON no MinIO:
bronze/ibge/data=YYYYMMDD/ibge-uf_YYYYMMDD_HHMMSS.json
"""

from minio import Minio
from datetime import datetime
from io import BytesIO
import requests
import json

# === CONFIGURAÇÕES ===
BUCKET_NAME = "data-ingest"       # bucket no MinIO
PREFIX = "bronze/ibge"            # pasta dentro do bucket
API_URL = "https://brasilapi.com.br/api/ibge/uf/v1"

# Conexão MinIO (igual ao seu ambiente)
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
SECURE_CONNECTION = False


def main():
    print("🌐 Iniciando coleta da API pública BrasilAPI (IBGE-UF)...")

    # 1. Requisição à API
    try:
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        data = response.json()
        print(f"✅ Dados obtidos com sucesso: {len(data)} registros encontrados.")
    except Exception as e:
        print(f"❌ Erro ao consultar a API: {e}")
        return

    # 2. Preparar nomes e datas
    date_str = datetime.now().strftime("%Y%m%d")
    time_str = datetime.now().strftime("%H%M%S")
    object_name = f"{PREFIX}/data={date_str}/ibge-uf_{date_str}_{time_str}.json"

    # 3. Converter para JSON bytes
    json_bytes = json.dumps(data, indent=4, ensure_ascii=False).encode("utf-8")

    # 4. Conectar ao MinIO
    print("🚀 Conectando ao MinIO...")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=SECURE_CONNECTION
    )

    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"🪣 Bucket '{BUCKET_NAME}' criado.")
        else:
            print(f"🪣 Bucket '{BUCKET_NAME}' já existe.")
    except Exception as e:
        print(f"❌ Erro ao verificar/criar bucket: {e}")
        return

    # 5. Upload para o MinIO
    try:
        print(f"📤 Enviando arquivo para {object_name} ...")
        client.put_object(
            BUCKET_NAME,
            object_name,
            BytesIO(json_bytes),
            length=len(json_bytes),
            content_type="application/json"
        )
        print("✅ Upload concluído com sucesso!")
    except Exception as e:
        print(f"❌ Erro durante o upload para o MinIO: {e}")
        return

    print("\n🏁 Processo finalizado com sucesso!")
    print(f"📂 Caminho no bucket: {BUCKET_NAME}/{object_name}")


if __name__ == "__main__":
    main()
