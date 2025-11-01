# orchestrator_pipeline.py
import subprocess
from datetime import datetime

scripts = [
    # Camada BRONZE
    #("Ingest_bronze_script.py", "📦 Extraindo dados do SQL DDL"),
    ("ingest_dbloja.py", "📦 Extraindo e inserindo os dados do SQL DDL"),
    ("controle_produto.py", "🗄️ Cria o arquivo controle watermark)"),
   # ("IncrementalVsFullLoad.py", "🗄️ Ingestão PostgreSQL (Full + Incremental)"),
    ("upload_jsons_to_minio.py", "📂 Upload dos arquivos JSON"),
    ("ingest_ibge_brasilapi_to_minio.py", "🌎 Ingestão da API IBGE (BrasilAPI)"),
    ("listar_bronze_minio.py", "🧾 Listagem de arquivos Bronze (diagnóstico)"),

    # Camada PRATA
    ("new_script_silver.py", "⚙️ Transformação DB_LOJA (Silver)"),
    ("new_script_silver_json.py", "⚙️ Transformação JSON (Silver)"),
    ("new_script_silver_ibge_final.py", "⚙️ Transformação IBGE (Silver)"),
]

def run_pipeline():
    print(f"🚀 Iniciando pipeline completo às {datetime.now()}\n")
    for script, desc in scripts:
        print(f"=== {desc} ===")
        try:
            subprocess.run(["python", f"script/{script}"], check=True)
        except subprocess.CalledProcessError:
            print(f"❌ Erro ao executar {script}, abortando pipeline.")
            break
        print(f"✅ {script} finalizado.\n")

    print("\n🏁 Pipeline completo executado com sucesso!")

if __name__ == "__main__":
    run_pipeline()
