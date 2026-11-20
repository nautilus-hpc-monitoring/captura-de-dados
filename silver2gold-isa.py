from silver2gold import *
from config_aws import s3_client
import tempfile
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import joblib


MODELO_VERSAO = os.getenv("MODELO_VERSAO", "v1")
LIMIAR_Z = 3.0
PISO_MAD = 0.5

HORAS_JANELA = 24
FUSO_HORARIO = ZoneInfo("America/Sao_Paulo")
HORARIO_CONSOLIDACAO = 5

PREFIXO_CATEGORIZACAO = "03-gold/categorizacao/"
PREFIXO_DASHBOARD = "03-gold/dashboard-categorica/"

CHAVE_HISTORICO = PREFIXO_CATEGORIZACAO + "historico_episodios.json"
CHAVE_CONSOLIDACAO = PREFIXO_DASHBOARD + "consolidacao_diaria.json"
CHAVE_DASHBOARD = PREFIXO_DASHBOARD + "dashboard.json"

METRICAS_CATEGORIZACAO = [
    "CPU_PERCENT",
    "LOAD_AVG_1",
    "RAM_PERCENT",
    "SWAP_PERCENT",
    "DISCO_PERCENT",
    "CPU_IOWAIT_PERCENT",
]

COLUNAS_CONSTANTES = ["CPU_COUNT_LOGICA", "RAM_TOTAL", "DISCO_TOTAL"]

def buscar_todos():
    if os.getenv("HOSTNAMES"):
        hosts = json.loads(os.environ["HOSTNAMES"])
        if not isinstance(hosts, list) or not hosts or not all(
                isinstance(host, str) and host.strip() for host in hosts):
            raise ValueError("HOSTNAMES deve ser uma lista JSON com nomes de nodes.")
        return sorted(set(hosts))

    import pymysql  
    conexao = pymysql.connect(
        host=os.environ["DB_HOST"], port=int(os.getenv("DB_PORT", "3306")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_DATABASE"], charset="utf8mb4")
    try:
        with conexao.cursor() as cursor:
            cursor.execute('SELECT a.id_ambiente_hpc,a.nome,c.id_cluster,c.nome,n.id_node,n.hostname FROM ambiente_hpc AS a JOIN cluster AS c ON c.fk_ambiente_hpc = a.id_ambiente_hpc JOIN node AS n ON n.fk_cluster = c.id_cluster;')
            return [r[0] for r in cursor.fetchall()]
    finally:
        conexao.close()

def diretorio_cache():
    return os.path.join(
        tempfile.gettempdir(),
        "isolation-forest",
        MODELO_VERSAO,
    )

def arquivos_modelo(nome):
    diretorio = diretorio_cache
    os.makedirs(diretorio, exist_ok=True)
    caminho = os.path.join(diretorio, nome)

    if os.path.exists(caminho) and os.path.getsize(caminho) > 0:
        return caminho

    bucket = os.getenv("BUCKET_NAME")

    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

    chave = f"modelos/{MODELO_VERSAO}/{nome}"

    try:
        s3_client.download_file(bucket, chave, caminho)
    except Exception as erro:
        raise RuntimeError(
            f"Não foi possível baixar o artefato {chave} do S3: {erro}"
        ) from erro

    if not os.path.exists(caminho) or os.path.getsize(caminho) == 0:
        raise FileNotFoundError(f"Artefato ausente ou vazio: {caminho}")

    return caminho

def baixar_arquivos_modelo():
    caminho_modelo = arquivos_modelo("isolation_forest.joblib")
    caminho_baseline = arquivos_modelo("baseline.json")

    modelo = joblib.load(caminho_modelo)

    with open(caminho_baseline, "r", encoding="utf-8") as arquivo:
        baseline = json.load(arquivo)

    if not isinstance(baseline, dict) or not baseline:
        raise ValueError("baseline.json inválido ou vazio.")

    return modelo, baseline