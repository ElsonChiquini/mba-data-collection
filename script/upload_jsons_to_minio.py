# -*- coding: utf-8 -*-
"""
Faz upload de todos os arquivos .json da pasta local 'json/'
para o bucket MinIO, seguindo a estrutura:

bronze/json/data=YYYYMMDD/{arquivo_YYYYMMDD_HHMMSS.json}
"""

from minio import Minio
from datetime import datetime
from io import BytesIO
import os

# === CONFIGURAÇÕES ===
LOCAL_FOLDER = "json"             # pasta local com os .json
BUCKET_NAME = "data-ingest"       # bucket no MinIO
BRONZE_PREFIX = "bronze/json"     # diretório base dentro do bucket

# Configuração do MinIO (igual ao seu ambiente)
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
SECURE_CONNECTION = False


def main():
    print("🚀 Iniciando upload dos arquivos JSON para a camada Bronze no MinIO...")

    # Data/hora atuais
    date_str = datetime.now().strftime("%Y%m%d")
    time_str = datetime.now().strftime("%H%M%S")

    # Caminho lógico de destino no MinIO
    remote_base_path = f"{BRONZE_PREFIX}/data={date_str}/"

    # Conecta ao MinIO
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=SECURE_CONNECTION
    )

    # Cria bucket se necessário
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"🪣 Bucket '{BUCKET_NAME}' criado.")
    else:
        print(f"🪣 Bucket '{BUCKET_NAME}' já existe.")

    # Lista arquivos locais
    files = [f for f in os.listdir(LOCAL_FOLDER) if f.endswith(".json")]
    if not files:
        print(f"⚠️ Nenhum arquivo .json encontrado na pasta '{LOCAL_FOLDER}'.")
        return

    print(f"📦 {len(files)} arquivos encontrados.\n")

    # Faz upload de cada arquivo
    for filename in files:
        local_path = os.path.join(LOCAL_FOLDER, filename)

        # Remove o prefixo "dados_" se existir
        base_name = os.path.splitext(filename)[0].replace("dados_", "")
        new_name = f"{base_name}_{date_str}_{time_str}.json"

        # Caminho completo no bucket
        object_name = f"{remote_base_path}{new_name}"

        with open(local_path, "rb") as file_data:
            file_bytes = file_data.read()
            client.put_object(
                BUCKET_NAME,
                object_name,
                BytesIO(file_bytes),
                length=len(file_bytes),
                content_type="application/json"
            )

        print(f"✅ {filename} enviado -> {object_name}")

    print("\n🏁 Upload concluído com sucesso!")
    print(f"📂 Estrutura criada no bucket '{BUCKET_NAME}': {remote_base_path}")


if __name__ == "__main__":
    main()
