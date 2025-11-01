import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime

# ============================================================
# CONFIGURAÇÕES
# ============================================================
BUCKET = "data-ingest"
PASTA_BRONZE = "bronze/dbloja/"
PASTA_PRATA = "prata/dbloja/produto/"
WATERMARK_KEY = "prata/dbloja/controle/watermark_produto.txt"
DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d")

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
# FUNÇÕES DE SUPORTE
# ============================================================
def ler_watermark():
    """Lê a data da última atualização incremental salva na PRATA."""
    try:
        response = s3.get_object(Bucket=BUCKET, Key=WATERMARK_KEY)
        data = response["Body"].read().decode("utf-8").strip()
        print(f"🕒 Última marca d’água encontrada na PRATA: {data}")
        return data
    except s3.exceptions.NoSuchKey:
        print("⚠️ Nenhuma marca d’água encontrada. Processamento inicial será executado.")
        return None

def salvar_watermark(data_str):
    """Salva/atualiza a marca d’água na PRATA."""
    s3.put_object(Bucket=BUCKET, Key=WATERMARK_KEY, Body=data_str.encode("utf-8"))
    print(f"💧 Marca d’água atualizada para: {data_str}")

def listar_arquivos_bronze():
    """Lista arquivos de produto na BRONZE."""
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PASTA_BRONZE)
    arquivos = []
    if "Contents" in response:
        for obj in response["Contents"]:
            nome = obj["Key"]
            if "produto" in nome and nome.endswith(".parquet"):
                arquivos.append(nome)
    return sorted(arquivos)

def ler_parquet_do_s3(path):
    """Lê Parquet direto do MinIO."""
    obj = s3.get_object(Bucket=BUCKET, Key=path)
    return pd.read_parquet(BytesIO(obj["Body"].read()))

# ============================================================
# ETAPA 1: LOCALIZAR O ARQUIVO MAIS RECENTE NA BRONZE
# ============================================================
arquivos = listar_arquivos_bronze()

if not arquivos:
    print("❌ Nenhum arquivo de produto encontrado na Bronze.")
    exit()

arquivo_recente = arquivos[-1]
print(f"📦 Arquivo mais recente da Bronze: {arquivo_recente}")

# ============================================================
# ETAPA 2: LER E APLICAR INCREMENTO
# ============================================================
marca_dagua_anterior = ler_watermark()
df = ler_parquet_do_s3(arquivo_recente)

if df.empty:
    print("⚠️ O arquivo da Bronze está vazio, nada a processar.")
    exit()

if marca_dagua_anterior and "data_atualizacao" in df.columns:
    df["data_atualizacao"] = pd.to_datetime(df["data_atualizacao"], errors='coerce')
    marca_dagua_datetime = pd.to_datetime(marca_dagua_anterior)
    df = df[df["data_atualizacao"] > marca_dagua_datetime]
    print(f"📊 Registros novos/atualizados desde {marca_dagua_anterior}: {len(df)}")
else:
    print("🚀 Processamento inicial — todos os dados serão enviados à Prata.")

if df.empty:
    print("✅ Nenhum novo registro a enviar para Prata.")
    exit()

# ============================================================
# ETAPA 3: SALVAR NA CAMADA PRATA
# ============================================================
novo_nome = f"produtos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
caminho_prata = f"{PASTA_PRATA}data_processamento={DATA_PROCESSAMENTO}/{novo_nome}"

buffer = BytesIO()
df.to_parquet(buffer, index=False)
s3.put_object(Bucket=BUCKET, Key=caminho_prata, Body=buffer.getvalue())

print(f"💾 Dados atualizados salvos em: {caminho_prata}")

# ============================================================
# ETAPA 4: ATUALIZAR MARCA D’ÁGUA
# ============================================================
nova_data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
salvar_watermark(nova_data)

print("\n✅ Processamento concluído com sucesso! Dados enviados à camada PRATA.")
