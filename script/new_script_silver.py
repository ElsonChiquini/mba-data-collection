import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from datetime import datetime

# ===================== CONFIG =====================
BUCKET = "data-ingest"
PATH_BRONZE = "bronze/dbloja/"
PATH_PRATA  = "prata/dbloja/"
WATERMARK_KEY = "prata/dbloja/controle/watermark_produto.txt"

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1"
)

# ===================== HELPERS S3 =====================
def list_parquets(prefix: str):
    """Lista todos os .parquet em um prefixo (ordenados)."""
    res = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    if "Contents" not in res:
        return []
    return sorted([o["Key"] for o in res["Contents"] if o["Key"].endswith(".parquet")])

def read_parquet_s3(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))

def write_parquet_s3(df: pd.DataFrame, key: str):
    df = df.where(pd.notnull(df), None)
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            try:
                df[col] = pd.to_numeric(df[col], errors="ignore")
            except Exception:
                pass
    buf = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False, safe=True)
    pq.write_table(table, buf)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"💾 Salvo: {key}  ({len(df)} linhas)")

def latest_silver_snapshot_key(table: str) -> str | None:
    """Retorna o arquivo mais recente da PRATA para a tabela, se existir."""
    keys = list_parquets(f"{PATH_PRATA}{table}/data=")
    return keys[-1] if keys else None

# ===================== SCHEMA =====================
def apply_schema(table: str, df: pd.DataFrame) -> pd.DataFrame:
    types = {
        "produto": {
            "id": "Int64", "nome": "string", "descricao": "string",
            "preco": "float64", "estoque": "Int64", "id_categoria": "Int64",
            "data_criacao": "datetime64[ns]", "data_atualizacao": "datetime64[ns]"
        },
        "categorias_produto": {"id": "Int64", "nome": "string", "descricao": "string"},
        "cliente": {"id": "Int64", "nome": "string", "email": "string",
                    "telefone": "string", "data_cadastro": "datetime64[ns]"},
        "pedido_cabecalho": {"id": "Int64", "id_cliente": "Int64",
                             "data_pedido": "datetime64[ns]", "valor_total": "float64"},
        "pedido_itens": {"id": "Int64", "id_pedido": "Int64", "id_produto": "Int64",
                         "quantidade": "Int64", "preco_unitario": "float64"},
    }
    if table in types:
        for col, typ in types[table].items():
            if col in df.columns:
                try:
                    if typ == "Int64":
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    elif typ == "float64":
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    elif "datetime" in typ:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    else:
                        df[col] = df[col].astype("string")
                except Exception as e:
                    print(f"⚠️ conversão {table}.{col}->{typ} falhou: {e}")
    # qualquer object restante: tenta int, senão string
    for col in df.select_dtypes(include="object").columns:
        if df[col].apply(lambda x: str(x).isdigit() if pd.notnull(x) else True).all():
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        else:
            df[col] = df[col].astype("string")
    return df

# ===================== FULL LOAD =====================
def silver_full_from_bronze(table: str, run_date: str, run_time: str):
    print(f"\n🚀 FULL LOAD: {table}")
    bronze_files = list_parquets(f"{PATH_BRONZE}data=")
    bronze_files = [k for k in bronze_files if f"/{table}_" in k]
    if not bronze_files:
        print(f"⚠️ Bronze sem arquivos para {table}.")
        return
    dfs = [read_parquet_s3(k) for k in bronze_files]
    df = apply_schema(table, pd.concat(dfs, ignore_index=True))

    # novo formato de pasta
    key = f"{PATH_PRATA}{table}/data={run_date}/{table}_{run_date}_{run_time}.parquet"
    write_parquet_s3(df, key)

# ===================== INCREMENTAL PRODUTO =====================
def silver_merge_produto_from_bronze(run_date: str, run_time: str):
    print("\n🚀 INCREMENTAL (MERGE) : produto")
    bronze_files = list_parquets(f"{PATH_BRONZE}data=")
    bronze_prod = [k for k in bronze_files if "/produto_" in k]
    if not bronze_prod:
        print("⚠️ Bronze sem arquivos de produto.")
        return
    delta_key = bronze_prod[-1]
    df_delta = apply_schema("produto", read_parquet_s3(delta_key))

    last_snap_key = latest_silver_snapshot_key("produto")
    if last_snap_key:
        df_silver = apply_schema("produto", read_parquet_s3(last_snap_key))
        s = df_silver.set_index("id")
        d = df_delta.set_index("id")
        s.update(d)
        novos = d.loc[~d.index.isin(s.index)]
        df_final = pd.concat([s, novos]).reset_index()
    else:
        print("ℹ️ Não há snapshot anterior na Prata. Criando o primeiro snapshot.")
        df_final = df_delta

    key = f"{PATH_PRATA}produto/data={run_date}/produto_{run_date}_{run_time}.parquet"
    write_parquet_s3(apply_schema("produto", df_final), key)

# ===================== MAIN =====================
if __name__ == "__main__":
    print("=== INICIANDO CARGA PARA PRATA (estrutura por tabela) ===")
    run_date = datetime.now().strftime("%Y%m%d")
    run_time = datetime.now().strftime("%H%M%S")

    # FULL LOAD
    for tbl in ["categorias_produto", "cliente", "pedido_cabecalho", "pedido_itens"]:
        silver_full_from_bronze(tbl, run_date, run_time)

    # INCREMENTAL produto
    silver_merge_produto_from_bronze(run_date, run_time)

    print("\n✅ Finalizado! Estrutura de saída:")
    print(f"👉 {PATH_PRATA}<tabela>/data={run_date}/<tabela>_{run_date}_{run_time}.parquet")
