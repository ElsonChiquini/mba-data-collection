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
PATH_BRONZE_JSON = "bronze/json/"
PATH_PRATA_JSON = "prata/json/"

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
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def write_parquet_s3(df: pd.DataFrame, key: str):
    """Salva DataFrame em Parquet no S3."""
    df = df.where(pd.notnull(df), None)
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            try:
                df[col] = pd.to_numeric(df[col], errors="ignore")
            except Exception:
                df[col] = df[col].astype("string")

    buf = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False, safe=True)
    pq.write_table(table, buf)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"💾 salvo: {key} ({len(df)} linhas)")

def delete_prefix(prefix: str):
    """Apaga todos os arquivos de um prefixo (usado para overwrite)."""
    keys = list_keys(prefix)
    if not keys:
        return
    to_delete = [{"Key": k} for k in keys if k.startswith(prefix)]
    if to_delete:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_delete})

# ============================================================
# EXTRATO → transacoes/
# ============================================================
def process_extrato(run_date: str, run_time: str):
    prefix = f"{PATH_BRONZE_JSON}data={run_date}/"
    keys = [k for k in list_keys(prefix) if "extrato" in k and k.endswith(".json")]
    if not keys:
        print("⚠️ Nenhum arquivo extrato_*.json encontrado.")
        return

    key = sorted(keys)[-1]
    data = read_json_from_s3(key)
    if isinstance(data, dict):
        data = [data]

    data = [d for d in data if "transacoes" in d]
    if not data:
        print("⚠️ Nenhum registro com campo 'transacoes' encontrado.")
        return

    df = pd.json_normalize(
        data,
        record_path=["transacoes"],
        meta=["id_extrato", "cliente_id", "numero_conta"],
        errors="ignore"
    )

    for c in ("id_extrato", "cliente_id"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    if "numero_conta" in df.columns:
        df["numero_conta"] = df["numero_conta"].astype("string")

    path = f"{PATH_PRATA_JSON}transacoes/data={run_date}/"
    delete_prefix(path)
    key_out = f"{path}transacoes_{run_date}_{run_time}.parquet"
    write_parquet_s3(df, key_out)

# ============================================================
# PEDIDOS → pedidos_externos/ e pedidos_externos_itens/
# ============================================================
def process_pedidos(run_date: str, run_time: str):
    prefix = f"{PATH_BRONZE_JSON}data={run_date}/"
    keys = [k for k in list_keys(prefix) if "pedidos" in k and k.endswith(".json")]
    if not keys:
        print("⚠️ Nenhum arquivo pedidos_*.json encontrado.")
        return

    key = sorted(keys)[-1]
    data = read_json_from_s3(key)
    if isinstance(data, dict):
        data = [data]

    df_pedidos = pd.json_normalize(data, sep="_", max_level=1)
    df_itens = pd.json_normalize(
        data,
        record_path=["itens"],
        meta=["id_pedido", "cliente_id", "data_pedido", "valor_total"],
        sep="_",
        errors="ignore"
    )

    for c in ("id_pedido", "cliente_id"):
        if c in df_pedidos.columns:
            df_pedidos[c] = pd.to_numeric(df_pedidos[c], errors="coerce").astype("Int64")
    if "id_pedido" in df_itens.columns:
        df_itens["id_pedido"] = pd.to_numeric(df_itens["id_pedido"], errors="coerce").astype("Int64")

    path_head = f"{PATH_PRATA_JSON}pedidos_externos/data={run_date}/"
    path_itens = f"{PATH_PRATA_JSON}pedidos_externos_itens/data={run_date}/"
    delete_prefix(path_head)
    delete_prefix(path_itens)

    key_head = f"{path_head}pedidos_externos_{run_date}_{run_time}.parquet"
    key_itens = f"{path_itens}pedidos_externos_itens_{run_date}_{run_time}.parquet"
    write_parquet_s3(df_pedidos, key_head)
    write_parquet_s3(df_itens, key_itens)

# ============================================================
# PRODUTOS → produtos_parceiros/
# ============================================================
def process_produtos(run_date: str, run_time: str):
    prefix = f"{PATH_BRONZE_JSON}data={run_date}/"
    keys = [k for k in list_keys(prefix) if "produtos" in k and k.endswith(".json")]
    if not keys:
        print("⚠️ Nenhum arquivo produtos_*.json encontrado.")
        return

    key = sorted(keys)[-1]
    data = read_json_from_s3(key)

    # Acessa chave 'produtos'
    if isinstance(data, dict) and "produtos" in data:
        produtos = data["produtos"]
    else:
        produtos = data

    df = pd.json_normalize(produtos, sep="_")

    # Flatten de especificacoes (se existir)
    if "especificacoes" in df.columns:
        esp = pd.json_normalize(df["especificacoes"])
        df = pd.concat([df.drop(columns=["especificacoes"]), esp.add_prefix("esp_")], axis=1)

    path = f"{PATH_PRATA_JSON}produtos_parceiros/data={run_date}/"
    delete_prefix(path)
    key_out = f"{path}produtos_parceiros_{run_date}_{run_time}.parquet"
    write_parquet_s3(df, key_out)

# ============================================================
# TAGS → tags_produtos/
# ============================================================
def process_tags(run_date: str, run_time: str):
    prefix = f"{PATH_BRONZE_JSON}data={run_date}/"
    keys = [k for k in list_keys(prefix) if "tags" in k and k.endswith(".json")]
    if not keys:
        print("⚠️ Nenhum arquivo tags_*.json encontrado.")
        return

    key = sorted(keys)[-1]
    data = read_json_from_s3(key)
    df = pd.json_normalize(data)

    # Trata null e listas vazias
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(lambda x: x if x not in [None, [], ""] else None)

    path = f"{PATH_PRATA_JSON}tags_produtos/data={run_date}/"
    delete_prefix(path)
    key_out = f"{path}tags_produtos_{run_date}_{run_time}.parquet"
    write_parquet_s3(df, key_out)

# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================
if __name__ == "__main__":
    print("=== PROCESSAMENTO JSON → CAMADA PRATA (estrutura domain/data=YYYYMMDD) ===")

    run_date = latest_date_folder(PATH_BRONZE_JSON)
    if not run_date:
        print("❌ Nenhuma pasta data=YYYYMMDD encontrada em bronze/json/")
        raise SystemExit(1)

    run_time = datetime.now().strftime("%H%M%S")

    process_extrato(run_date, run_time)
    process_pedidos(run_date, run_time)
    process_produtos(run_date, run_time)
    process_tags(run_date, run_time)

    print("\n✅ Processamento concluído com sucesso!")
    print(f"📁 Saída organizada em: {PATH_PRATA_JSON}")
