import boto3

# Configuração do cliente
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# Nome do bucket
bucket = "data-ingest"

print(f"📦 Listando arquivos no bucket '{bucket}/bronze':\n")

# Listar objetos na pasta bronze
response = s3.list_objects_v2(Bucket=bucket, Prefix="bronze/")

if "Contents" in response:
    for obj in response["Contents"]:
        print(obj["Key"])
else:
    print("⚠️ Nenhum arquivo encontrado na pasta Bronze.")
