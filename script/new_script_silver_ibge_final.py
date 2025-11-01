import boto3
import pandas as pd
import json
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import re

# ============================================================
# CONFIGURAÇÕES
# ============================================================
BUCKET = "data-ingest"
PATH_BRONZE_IBGE = "bronze/ibge/"
PATH_SILVER_IBGE = "prata/ibge_uf/"

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
)

# ============================================================
# FUNÇÕES DE SUPORTE
# ============================================================
def list_keys(prefix: str):
    """Lista objetos dentro de um prefixo S3."""
    res = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    if "Contents" not in res:
        return []
    return [o["Key"] for o in res["Contents"]]

def latest_date_folder(prefix: str) -> str | None:
    """Detecta automaticamente a última pasta data=YYYYMMDD."""
    keys = list_keys(prefix)
    dates = set()
    pat = re.compile(r"data=(\d{8})/")
    for k in keys:
        m = pat.search(k)
        if m:
            dates.add(m.group(1))
    return sorted(dates)[-1] if dates else None

def read_json_from_s3(key: str):
    """Lê e carrega um JSON diretamente do MinIO."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def write_parquet_s3(df: pd.DataFrame, key: str):
    """Salva DataFrame como arquivo Parquet no S3."""
    buf = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"💾 Arquivo salvo: {key} ({len(df)} linhas)")

def delete_prefix(prefix: str):
    """Apaga todos os arquivos dentro de um prefixo (modo overwrite)."""
    keys = list_keys(prefix)
    if not keys:
        return
    to_delete = [{"Key": k} for k in keys if k.startswith(prefix)]
    if to_delete:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_delete})

# ============================================================
# PROCESSAMENTO IBGE → ibge_uf/
# ============================================================
def process_ibge_uf():
    print("=== PROCESSAMENTO IBGE (UF) → CAMADA PRATA ===")

    # Detecta a pasta data=YYYYMMDD mais recente
    run_date = latest_date_folder(PATH_BRONZE_IBGE)
    if not run_date:
        print("❌ Nenhuma pasta data=YYYYMMDD encontrada em bronze/ibge/")
        raise SystemExit(1)

    run_time = datetime.now().strftime("%H%M%S")

    prefix = f"{PATH_BRONZE_IBGE}data={run_date}/"
    # 🔍 Aceita arquivos ibge-uf_*, ibge_uf_*, ou uf_*
    keys = [k for k in list_keys(prefix) if re.search(r"(ibge[-_]?uf)", k) and k.endswith(".json")]

    if not keys:
        print(f"⚠️ Nenhum arquivo ibge-uf_*.json encontrado em {prefix}")
        return

    key = sorted(keys)[-1]
    print(f"📄 Lendo arquivo: {key}")

    # Lê o JSON
    data = read_json_from_s3(key)

    # O JSON deve ser uma lista de objetos
    if not isinstance(data, list):
        print("❌ O arquivo JSON não é um array de objetos válido.")
        raise SystemExit(1)

    # Cria DataFrame
    df = pd.DataFrame(data)

    # Aplica schema
    expected_cols = ["id", "sigla", "nome"]
    df = df[[col for col in expected_cols if col in df.columns]]

    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["sigla"] = df["sigla"].astype("string")
    df["nome"] = df["nome"].astype("string")

    # Overwrite por data
    silver_prefix = f"{PATH_SILVER_IBGE}data={run_date}/"
    delete_prefix(silver_prefix)

    # Caminho de saída
    key_out = f"{silver_prefix}ibge_uf_{run_date}_{run_time}.parquet"
    write_parquet_s3(df, key_out)

    print("\n✅ Processamento IBGE concluído com sucesso!")
    print(f"📁 Saída: {key_out}")

# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================
if __name__ == "__main__":
    process_ibge_uf()
