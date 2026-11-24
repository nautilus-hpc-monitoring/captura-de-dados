"""ETL do gestor: lê CSVs da Silver e publica o JSON da dashboard na Gold.

MAPA DO ARQUIVO
1. Configuração e nodes: variáveis de ambiente ou cadastro do banco.
2. Leitura da Silver: validação, filtro de datas e remoção de duplicatas.
3. Publicação JSON e reutilização das previsões do período.
4. Gráfico 1: ocupação de disco real e prevista.
5. Gráfico 2: mapa de calor de CPU e RAM.
6. Gráfico 3: consumo real e referência das duas semanas anteriores.
7. Gráfico 5: leituras de RAM e contadores de swap disponíveis.
8. KPI: médias dos componentes desde segunda-feira.
9. Execução e entrada da Lambda.

A captura continua em local2bronze.py; não é feita por esta ETL.
Alertas, SLA e gráfico 4 são calculados pelo site usando o MySQL.
Local: python silver2gold.py. Lambda: silver2gold.lambda_handler.
"""
import io
import json
import math
import os
from urllib.parse import quote

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()
FUSO = "America/Sao_Paulo"
ORIGEM = os.getenv("GESTOR_FUSO_ORIGEM", FUSO)
SILVER = os.getenv("SILVER_PREFIX", "02-silver/")
GOLD = os.getenv("CLUSTER_GOLD_PREFIX", "03-gold/gestor").rstrip("/")
PASSO = int(os.getenv("COLETA_SEGUNDOS", "10"))
COBERTURA = float(os.getenv("COBERTURA_MINIMA", "0.8"))
COLUNAS = ["CPU_PERCENT", "RAM_PERCENT", "RAM_TOTAL", "RAM_AVAILABLE",
           "SWAP_PERCENT", "SWAP_USED", "SWAP_FREE", "SWAP_IN", "SWAP_OUT",
           "DISCO_TOTAL", "DISCO_USED"]


# 1. CADASTRO: nodes informados na configuração ou consultados no banco.
def buscar_hosts(cluster_id):
    # Na Lambda, HOSTNAMES evita uma conexão com o MySQL da EC2.
    if os.getenv("HOSTNAMES"):
        hosts = json.loads(os.environ["HOSTNAMES"])
        if not isinstance(hosts, list) or not hosts or not all(
                isinstance(host, str) and host.strip() for host in hosts):
            raise ValueError("HOSTNAMES deve ser uma lista JSON com nomes de nodes.")
        return sorted(set(hosts))

    import pymysql  # Necessário somente no modo que consulta o cadastro no banco.
    conexao = pymysql.connect(
        host=os.environ["DB_HOST"], port=int(os.getenv("DB_PORT", "3306")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_DATABASE"], charset="utf8mb4")
    try:
        with conexao.cursor() as cursor:
            cursor.execute("SELECT hostname FROM node WHERE fk_cluster = %s ORDER BY hostname",
                           (cluster_id,))
            return [r[0] for r in cursor.fetchall()]
    finally:
        conexao.close()


def instante(valor):
    try:
        t = pd.Timestamp(valor)
        if pd.isna(t):
            return pd.NaT
        if t.tzinfo is None:
            t = t.tz_localize(ORIGEM, ambiguous="NaT", nonexistent="NaT")
        return t.tz_convert(FUSO)
    except (ValueError, TypeError):
        return pd.NaT


# 2. SILVER: limpeza e filtro antes de juntar os arquivos na memória.
def carregar_silver(s3, bucket, hosts, agora, inicio=None, colunas=None):
    partes = []
    if inicio is None:
        inicio = agora - pd.Timedelta(days=45)
    if colunas is None:
        colunas = COLUNAS
    paginas = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=SILVER)
    for pagina in paginas:
        for item in pagina.get("Contents", []):
            if not item["Key"].endswith(".csv"):
                continue
            corpo = s3.get_object(Bucket=bucket, Key=item["Key"])["Body"]
            try:
                with io.TextIOWrapper(corpo, encoding="utf-8-sig") as texto:
                    for df in pd.read_csv(texto, sep=";", chunksize=50000):
                        if not {"HOSTNAME", "TIMESTAMP"}.issubset(df.columns):
                            raise ValueError(f"CSV sem identificação: {item['Key']}")
                        df = df[df["HOSTNAME"].isin(hosts)].copy()
                        if df.empty:
                            continue
                        df["TIMESTAMP"] = pd.to_datetime(
                            df["TIMESTAMP"].map(instante), utc=True).dt.tz_convert(FUSO)
                        df = df[df["TIMESTAMP"].between(inicio, agora)].copy()
                        for c in colunas:
                            df[c] = pd.to_numeric(df[c], errors="coerce") if c in df else np.nan
                            df[c] = df[c].where(np.isfinite(df[c]) & (df[c] >= 0))
                            if c.endswith("_PERCENT"):
                                df[c] = df[c].where(df[c] <= 100)
                        if "DISCO_USED" in colunas and "DISCO_TOTAL" in df:
                            df["DISCO_USED"] = df["DISCO_USED"].where(
                                (df["DISCO_TOTAL"] > 0) & (df["DISCO_USED"] <= df["DISCO_TOTAL"]))
                        if "RAM_AVAILABLE" in colunas and "RAM_TOTAL" in df:
                            df["RAM_AVAILABLE"] = df["RAM_AVAILABLE"].where(
                                (df["RAM_TOTAL"] > 0) & (df["RAM_AVAILABLE"] <= df["RAM_TOTAL"]))
                        partes.append(df[["HOSTNAME", "TIMESTAMP"] + colunas])
            finally:
                corpo.close()
    if not partes:
        raise ValueError("Nenhum dado Silver encontrado para os hosts do cluster.")
    df = pd.concat(partes, ignore_index=True)
    return (df.drop_duplicates(["HOSTNAME", "TIMESTAMP"])
            .sort_values(["HOSTNAME", "TIMESTAMP"]))


def limpar_json(valor):
    if isinstance(valor, dict):
        return {k: limpar_json(v) for k, v in valor.items()}
    if isinstance(valor, (list, tuple)):
        return [limpar_json(v) for v in valor]
    if isinstance(valor, pd.Timestamp):
        return valor.isoformat()
    if valor is None or valor is pd.NA:
        return None
    if isinstance(valor, np.generic):
        return limpar_json(valor.item())
    if isinstance(valor, float) and not math.isfinite(valor):
        return None
    return valor


def gravar(s3, bucket, chave, dados, **opcoes):
    s3.put_object(Bucket=bucket, Key=chave, ContentType="application/json",
                  Body=json.dumps(limpar_json(dados), ensure_ascii=False,
                                  allow_nan=False).encode("utf-8"), **opcoes)


# Previsão emitida uma vez por período. Reexecuções reutilizam a mesma versão.
def congelar(s3, bucket, chave, calcular):
    def ler():
        resposta = s3.get_object(Bucket=bucket, Key=chave)
        try:
            return json.loads(resposta["Body"].read())
        finally:
            resposta["Body"].close()
    try:
        return ler()
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("NoSuchKey", "404"):
            raise
    dados = limpar_json(calcular())
    try:
        gravar(s3, bucket, chave, dados, IfNoneMatch="*")
        return dados
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("PreconditionFailed", "412"):
            raise
        return ler()


# 3. BASE HORÁRIA: média por hora com as amostras disponíveis.
def medias_horarias(g):
    colunas = ["CPU_PERCENT", "RAM_PERCENT"]
    amostras = g.set_index("TIMESTAMP")[colunas].resample(f"{PASSO}s").mean()
    return amostras.resample("1h").mean()


# 4. GRÁFICO 1: último disco do dia e regressão dos 30 dias anteriores à semana.
def disco(g, semana, corte, previsao=None):
    dias = pd.date_range(semana, periods=7, freq="D")
    validos = g.dropna(subset=["DISCO_USED", "DISCO_TOTAL"]).copy()
    if validos.empty:
        return None
    validos["DIA"] = validos["TIMESTAMP"].dt.normalize()
    diarios = validos.groupby("DIA").tail(1).set_index("DIA")
    capacidade = float(validos.iloc[-1]["DISCO_TOTAL"]) / 1e12
    if capacidade <= 0:
        return None
    if previsao is None:
        h = diarios[(diarios.index >= semana - pd.Timedelta(days=30)) &
                    (diarios.index < semana)].copy()
        # Só considerar fechamento coletado na última hora do dia.
        h = h[(h.index + pd.Timedelta(days=1) - h["TIMESTAMP"]) <= pd.Timedelta(hours=1)]
        previstos = [None] * 7
        if (len(h) >= 7 and h["DISCO_TOTAL"].nunique() == 1
                and h["DISCO_TOTAL"].iloc[-1] == validos.iloc[-1]["DISCO_TOTAL"]):
            x = np.array([(d - semana).total_seconds() / 86400 for d in h.index])
            b, a = np.polyfit(x, h["DISCO_USED"].to_numpy(dtype=float) / 1e12, 1)
            previstos = [max(0.0, float(a + b * i)) for i in range(7)]
        return {"capacidade": capacidade, "previsto": previstos}
    real = diarios["DISCO_USED"].reindex(dias) / 1e12
    real = real.where(dias <= corte.normalize())
    previstos = previsao["previsto"] if previsao["capacidade"] == capacidade else [None] * 7
    return {"volume": os.environ["DISCO_VOLUME"], "capacidade": capacidade,
            "datas": [d.date().isoformat() for d in dias],
            "real": real.tolist(), "previsto": previstos}


# 5. GRÁFICO 2: sete dias completos; cada dia possui 24 horas.
def calor(horas, hoje):
    dias = pd.date_range(hoje - pd.Timedelta(days=7), periods=7, freq="D")
    linhas = []
    for d in dias:
        h = horas.reindex(pd.date_range(d, periods=24, freq="1h"))
        linhas.append([{"CPU": r.CPU_PERCENT, "RAM": r.RAM_PERCENT}
                       for r in h.itertuples()])
    return {"dias": [d.date().isoformat() for d in dias], "leituras": linhas}


# 6. GRÁFICO 3: referência das mesmas horas de 7 e 14 dias atrás.
def consumo(horas, hoje, corte, previsao=None):
    datas = pd.date_range(hoje, periods=24, freq="1h")
    if previsao is None:
        resultado = {}
        for recurso, coluna in [("cpuExpected", "CPU_PERCENT"), ("ramExpected", "RAM_PERCENT")]:
            anterior = horas[coluna].reindex(datas - pd.Timedelta(days=7)).to_numpy()
            anterior2 = horas[coluna].reindex(datas - pd.Timedelta(days=14)).to_numpy()
            # Se uma das duas horas falta, a previsão daquela hora permanece null.
            resultado[recurso] = ((anterior + anterior2) / 2).tolist()
        return resultado
    real = horas.reindex(datas).copy()
    real.loc[real.index >= corte.floor("1h"), :] = np.nan
    return {"datas": [d.isoformat() for d in datas],
            "cpuObserved": real["CPU_PERCENT"].tolist(),
            "ramObserved": real["RAM_PERCENT"].tolist(), **previsao}


# 7. GRÁFICO 5: RAM e ocupação da swap, disponíveis também no Windows.
def memoria(g, corte):
    janela = g[g["TIMESTAMP"].between(corte - pd.Timedelta(hours=24), corte)].copy()
    if len(janela) < 2:
        return None
    total_swap = janela["SWAP_USED"] + janela["SWAP_FREE"]
    if not total_swap.notna().any():
        return None
    sem_swap = bool((total_swap.dropna() == 0).all())
    disponivel = janela["RAM_AVAILABLE"] / janela["RAM_TOTAL"] * 100
    percentual_swap = janela["SWAP_USED"] / total_swap.where(total_swap > 0) * 100
    percentual_swap = percentual_swap.where(percentual_swap.between(0, 100))
    return {"semSwap": sem_swap, "leituras": [
        {"timestamp": r.TIMESTAMP.isoformat(), "ramDisponivel": disponivel.loc[i],
         "swapPercent": percentual_swap.loc[i],
         "swapIn": r.SWAP_IN, "swapOut": r.SWAP_OUT}
        for i, r in janela.iterrows()]}


# 8. KPI 2: média de cada componente desde segunda até a última coleta.
# Cada intervalo de coleta tem o mesmo peso; valores ausentes não viram zero.
def medias_semana(g, semana, corte):
    janela = g[g["TIMESTAMP"].between(semana, corte)].copy()
    if janela.empty:
        return None
    janela["CPU"] = janela["CPU_PERCENT"]
    janela["RAM"] = janela["RAM_PERCENT"]
    janela["Disco"] = janela["DISCO_USED"] / janela["DISCO_TOTAL"].where(janela["DISCO_TOTAL"] > 0) * 100
    total_swap = janela["SWAP_USED"] + janela["SWAP_FREE"]
    janela["Swap"] = janela["SWAP_USED"] / total_swap.where(total_swap > 0) * 100
    componentes = ["CPU", "RAM", "Disco", "Swap"]
    valores = janela.set_index("TIMESTAMP")[componentes]
    valores = valores.where((valores >= 0) & (valores <= 100))
    intervalos = valores.resample(f"{PASSO}s").mean()
    return limpar_json({
        "inicio": semana,
        "fim": corte,
        "valores": intervalos.mean().to_dict(),
        "amostras": intervalos.count().to_dict()
    })


# 9. EXECUÇÃO: publica somente o contrato consumido pela dashboard atual.
def executar():
    if not 0 < PASSO <= 300 or 3600 % PASSO or not 0 < COBERTURA <= 1:
        raise ValueError("COLETA_SEGUNDOS deve dividir 3600; cobertura deve estar entre 0 e 1.")
    bucket = os.environ["BUCKET_NAME"]
    cluster_id = int(os.environ["GESTOR_CLUSTER_ID"])
    if cluster_id <= 0:
        raise ValueError("GESTOR_CLUSTER_ID inválido.")
    hosts = buscar_hosts(cluster_id)
    if not hosts:
        raise ValueError("Cluster sem nodes cadastrados.")
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", os.getenv("REGION_NAME", "us-east-1")))
    agora = pd.Timestamp.now(tz=FUSO)
    bucket_silver = os.getenv("SILVER_BUCKET_NAME", bucket)
    df = carregar_silver(s3, bucket_silver, hosts, agora)
    if df.empty:
        raise ValueError("Silver sem leituras válidas no período; publicação cancelada.")
    hoje = agora.normalize()
    semana = hoje - pd.Timedelta(days=hoje.dayofweek)
    base = f"{GOLD}/cluster={cluster_id}"
    nodes = []
    for host in hosts:
        g = df[df["HOSTNAME"] == host].copy()
        if g.empty:
            continue  # O cadastro do banco continua exibindo esse node como sem telemetria.
        corte = min(agora, g["TIMESTAMP"].max())
        horas = medias_horarias(g)
        caminho_host = quote(host, safe="")
        chave_disco = f"{base}/previsoes-v2/disco/{semana.date()}/{caminho_host}.json"
        chave_consumo = f"{base}/previsoes-v2/consumo/{hoje.date()}/{caminho_host}.json"
        prev_disco = congelar(s3, bucket, chave_disco, lambda: disco(g, semana, corte))
        prev_consumo = congelar(s3, bucket, chave_consumo, lambda: consumo(horas, hoje, corte))
        nodes.append({"hostname": host, "disco": disco(g, semana, corte, prev_disco) if prev_disco else None,
                      "calor": calor(horas, hoje), "consumo": consumo(horas, hoje, corte, prev_consumo),
                      "memoria": memoria(g, corte), "medias_semana": medias_semana(g, semana, corte)})
    painel = {"schema_version": 2,
              "metadados": {"cluster_id": cluster_id, "gerado_em": agora,
                            "telemetria_ate": df["TIMESTAMP"].max(), "demonstrativo": False},
              "nodes": nodes}
    chave = f"{base}/dashboard.json"
    gravar(s3, bucket, chave, painel)
    print(f"Gold publicada: s3://{bucket}/{chave} | {len(nodes)} nodes com telemetria")
    return {"bucket": bucket, "chave": chave, "nodes": len(nodes)}


def lambda_handler(event, context):
    """Entrada da Lambda; a execução é acionada externamente."""
    for variavel in ("BUCKET_NAME", "GESTOR_CLUSTER_ID", "HOSTNAMES", "DISCO_VOLUME"):
        if not os.getenv(variavel):
            raise ValueError(f"Configure a variável {variavel} na Lambda.")
    return executar()


if __name__ == "__main__":
    executar()

from config_aws import s3_client
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import joblib


MODELO_VERSAO = os.getenv("MODELO_VERSAO", "v1")
LIMIAR_Z = 3.0
PISO_MAD = 0.5

HORAS_JANELA = 24
MIN_EPISODIO = 5
GAP_MINUTOS = 1

PREFIXO_CATEGORIZACAO = "03-gold/categorizacao/"
PREFIXO_DASHBOARD = "03-gold/dashboard-categorica/"

CHAVE_HISTORICO = PREFIXO_CATEGORIZACAO + "historico_episodios.json"
CHAVE_INTERPRETACAO_LATEST = PREFIXO_CATEGORIZACAO + "latest.json"
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
COLUNAS_CARGA = list(dict.fromkeys(
    COLUNAS + METRICAS_CATEGORIZACAO + COLUNAS_CONSTANTES
))
FUSO_TZ = ZoneInfo(FUSO)


def buscar_todos():
    if os.getenv("HOSTNAMES"):
        hosts = json.loads(os.environ["HOSTNAMES"])
        if not isinstance(hosts, list) or not hosts or not all(
                isinstance(host, str) and host.strip() for host in hosts):
            raise ValueError("HOSTNAMES deve ser uma lista JSON com nomes de nodes.")
        return [{"hpc_id": None, "hpc": None, "cluster_id": None,
                 "cluster": None, "node_id": None, "hostname": host}
                for host in sorted(set(hosts))]

    import pymysql
    conexao = pymysql.connect(
        host=os.environ["DB_HOST"], port=int(os.getenv("DB_PORT", "3306")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_DATABASE"], charset="utf8mb4")
    try:
        with conexao.cursor() as cursor:
            cursor.execute('SELECT a.id_ambiente_hpc,a.nome,c.id_cluster,c.nome,n.id_node,n.hostname FROM ambiente_hpc AS a JOIN cluster AS c ON c.fk_ambiente_hpc = a.id_ambiente_hpc JOIN node AS n ON n.fk_cluster = c.id_cluster;')
            return [
                {"hpc_id": r[0], "hpc": r[1], "cluster_id": r[2],
                 "cluster": r[3], "node_id": r[4], "hostname": r[5]}
                for r in cursor.fetchall()
            ]
    finally:
        conexao.close()


def diretorio_cache():
    return os.path.join(
        tempfile.gettempdir(),
        "isolation-forest",
        MODELO_VERSAO,
    )


def arquivos_modelo(nome):
    diretorio = diretorio_cache()
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


def colunas_modelo():
    caminho = arquivos_modelo("colunas.json")
    with open(caminho, "r", encoding="utf-8") as arquivo:
        colunas = json.load(arquivo)
    if (not isinstance(colunas, list) or not colunas
            or not all(isinstance(coluna, str) and coluna.strip() for coluna in colunas)):
        raise ValueError("colunas.json inválido ou vazio.")
    return colunas


def validar_compatibilidade(df, baseline):
    avisos = []

    if df.empty:
        return df.copy(), avisos

    hosts = df["HOSTNAME"].astype(str).unique()

    fora_do_baseline = sorted(
        host
        for host in hosts
        if host not in baseline
    )

    for host in fora_do_baseline:
        avisos.append(
            f"Node sem chave no baseline: {host}"
        )

    df = df[
        df["HOSTNAME"].astype(str).isin(baseline.keys())
    ].copy()

    if df.empty:
        return df, avisos

    colunas_presentes = [
        coluna
        for coluna in COLUNAS_CONSTANTES
        if coluna in df.columns
    ]

    for coluna in COLUNAS_CONSTANTES:
        if coluna not in df.columns:
            avisos.append(
                f"Coluna constante ausente nos dados: {coluna}"
            )

    for host, grupo in df.groupby("HOSTNAME"):
        baseline_node = baseline.get(str(host))

        if not baseline_node:
            continue

        mediana_baseline = baseline_node.get("mediana", {})

        for coluna in colunas_presentes:
            if coluna not in mediana_baseline:
                avisos.append(
                    f"Coluna constante ausente no baseline: {coluna}"
                )
                continue

            valores = grupo[coluna].dropna()

            if valores.empty:
                continue

            atual = float(valores.median())
            base = float(mediana_baseline[coluna])

            if base == 0:
                divergente = abs(atual - base) > 1e-9
            else:
                divergente = (
                    abs(atual - base) / abs(base) > 0.01
                )

            if divergente:
                avisos.append(
                    f"Mediana de {coluna} divergente para {host}: "
                    f"atual={atual:.2f}, baseline={base:.2f}"
                )

    return df.reset_index(drop=True), avisos


def detectar_anomalias(df, modelo, colunas):
    df = df.copy()

    if df.empty:
        df["anomalia"] = pd.Series(dtype="int64")
        df["score"] = pd.Series(dtype="float64")
        return df

    features = [str(coluna) for coluna in getattr(modelo, "feature_names_in_", colunas)]
    faltantes = [coluna for coluna in features if coluna not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes para o modelo: {faltantes}")
    matriz = df[features].apply(pd.to_numeric, errors="coerce")
    validas = matriz.notna().all(axis=1)

    df["anomalia"] = 1
    df["score"] = 0.0

    if validas.any():
        df.loc[validas, "anomalia"] = modelo.predict(matriz[validas])
        df.loc[validas, "score"] = modelo.decision_function(matriz[validas])

    return df


def criar_episodio(hostname, registros):
    if len(registros) < MIN_EPISODIO:
        return None

    inicio = registros["TIMESTAMP"].iloc[0]
    fim = registros["TIMESTAMP"].iloc[-1]

    return {
        "episodio_id": f"{hostname}|{inicio.isoformat()}",
        "hostname": str(hostname),
        "inicio": inicio,
        "fim": fim,
        "duracao_segundos": int(
            (fim - inicio).total_seconds()
        ),
        "quantidade_registros_anomalos": int(len(registros)),
        "registros": registros.copy(),
    }


def consolidar_episodios(df):
    if df.empty or "anomalia" not in df.columns:
        return []

    episodios = []

    for hostname, grupo in df.groupby("HOSTNAME"):
        anomalos = grupo[
            grupo["anomalia"] == -1
        ].sort_values("TIMESTAMP")

        if anomalos.empty:
            continue

        inicio_bloco = 0

        for indice in range(1, len(anomalos)):
            diferenca = (
                anomalos.iloc[indice]["TIMESTAMP"]
                - anomalos.iloc[indice - 1]["TIMESTAMP"]
            )

            if diferenca > timedelta(minutes=GAP_MINUTOS):
                episodio = criar_episodio(
                    hostname,
                    anomalos.iloc[inicio_bloco:indice],
                )

                if episodio is not None:
                    episodios.append(episodio)

                inicio_bloco = indice

        episodio = criar_episodio(
            hostname,
            anomalos.iloc[inicio_bloco:],
        )

        if episodio is not None:
            episodios.append(episodio)

    return sorted(
        episodios,
        key=lambda episodio: (
            episodio["hostname"],
            episodio["inicio"],
        ),
    )


def mad_interpretacao(metrica):
    return 1.0 if metrica == "LOAD_AVG_1" else PISO_MAD


def calcular_zscores(registros, baseline_node):
    mediana_baseline = baseline_node.get("mediana", {})
    mad_baseline = baseline_node.get("mad", {})

    resultado = {}

    for metrica in METRICAS_CATEGORIZACAO:
        if (
            metrica not in registros.columns
            or metrica not in mediana_baseline
            or metrica not in mad_baseline
        ):
            resultado[metrica] = {
                "z": None,
                "valor_mediano": None,
                "normal_mediana": None,
            }
            continue

        valores = pd.to_numeric(
            registros[metrica],
            errors="coerce",
        ).dropna()

        if valores.empty:
            resultado[metrica] = {
                "z": None,
                "valor_mediano": None,
                "normal_mediana": float(
                    mediana_baseline[metrica]
                ),
            }
            continue

        mediana = float(mediana_baseline[metrica])
        mad = float(mad_baseline[metrica])

        denominador = (
            1.4826
            * max(
                mad,
                mad_interpretacao(metrica),
            )
        )

        z_serie = (valores - mediana) / denominador

        z_mediana = float(z_serie.median())
        valor_mediano = float(valores.median())

        resultado[metrica] = {
            "z": (
                z_mediana
                if math.isfinite(z_mediana)
                else None
            ),
            "valor_mediano": (
                valor_mediano
                if math.isfinite(valor_mediano)
                else None
            ),
            "normal_mediana": (
                mediana
                if math.isfinite(mediana)
                else None
            ),
        }

    return resultado


def categorizar_episodio(zscores):
    nomes = {
        0: "Não classificado",
        1: "Pressão de processamento",
        2: "Degradação de memória",
        3: "Degradação de disco",
        4: "Pressão combinada",
    }

    destaque = [
        metrica
        for metrica in METRICAS_CATEGORIZACAO
        if zscores.get(metrica, {}).get("z") is not None
        and zscores[metrica]["z"] > LIMIAR_Z
    ]

    destaque.sort(
        key=lambda metrica: zscores[metrica]["z"],
        reverse=True,
    )

    if "DISCO_PERCENT" in destaque:
        codigo = 3
        assinatura = [
            "DISCO_PERCENT",
            "SWAP_PERCENT",
        ]

    elif "RAM_PERCENT" in destaque and (
        "SWAP_PERCENT" in destaque
        or "CPU_IOWAIT_PERCENT" in destaque
    ):
        codigo = 2
        assinatura = [
            "RAM_PERCENT",
            "SWAP_PERCENT",
            "CPU_IOWAIT_PERCENT",
        ]

    elif (
        "RAM_PERCENT" in destaque
        and "CPU_PERCENT" in destaque
    ):
        codigo = 4
        assinatura = [
            "CPU_PERCENT",
            "RAM_PERCENT",
        ]

    elif (
        "CPU_PERCENT" in destaque
        and "LOAD_AVG_1" in destaque
    ):
        codigo = 1
        assinatura = [
            "CPU_PERCENT",
            "LOAD_AVG_1",
        ]

    else:
        codigo = 0
        assinatura = []

    severidade_z = max(
        (
            zscores[metrica]["z"]
            for metrica in destaque
        ),
        default=0.0,
    )

    if codigo == 0:
        cobertura = 0.0
    else:
        cobertura = (
            len(set(assinatura) & set(destaque))
            / len(assinatura)
        )

    metricas_destaque = [
        {
            "metrica": metrica,
            "z": zscores[metrica]["z"],
            "valor_mediano": zscores[metrica]["valor_mediano"],
            "normal_mediana": zscores[metrica]["normal_mediana"],
        }
        for metrica in destaque
    ]

    return {
        "codigo": codigo,
        "nome": nomes[codigo],
        "severidade_z": severidade_z,
        "cobertura_assinatura": cobertura,
        "metricas_destaque": metricas_destaque,
    }


def converter_num(valor):
    if valor is None:
        return None

    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(numero):
        return None

    return round(numero, 2)


def executar_interpretacao(df, modelo, colunas, baseline):
    df_validado, avisos = validar_compatibilidade(
        df,
        baseline,
    )

    df_detectado = detectar_anomalias(
        df_validado,
        modelo,
        colunas,
    )

    episodios = consolidar_episodios(
        df_detectado
    )

    episodios_json = []

    for episodio in episodios:
        hostname = str(episodio["hostname"])
        baseline_node = baseline.get(hostname, {})

        zscores = calcular_zscores(
            episodio["registros"],
            baseline_node,
        )

        categoria = categorizar_episodio(
            zscores
        )

        scores = pd.to_numeric(
            episodio["registros"]["score"],
            errors="coerce",
        ).dropna()

        score_max = float(scores.max()) if not scores.empty else None
        score_medio = float(scores.mean()) if not scores.empty else None

        if score_max is not None and not math.isfinite(score_max):
            score_max = None
        if score_medio is not None and not math.isfinite(score_medio):
            score_medio = None

        metricas_destaque = [
            {
                "metrica": item["metrica"],
                "z": converter_num(item["z"]),
                "valor_mediano": converter_num(
                    item["valor_mediano"]
                ),
                "normal_mediana": converter_num(
                    item["normal_mediana"]
                ),
            }
            for item in categoria["metricas_destaque"]
        ]

        episodios_json.append(
            {
                "episodio_id": episodio["episodio_id"],
                "hostname": hostname,
                "inicio": episodio["inicio"].isoformat(),
                "fim": episodio["fim"].isoformat(),
                "duracao_segundos": int(
                    episodio["duracao_segundos"]
                ),
                "quantidade_registros_anomalos": int(
                    episodio["quantidade_registros_anomalos"]
                ),
                "categoria": {
                    "codigo": int(categoria["codigo"]),
                    "nome": categoria["nome"],
                },
                "score": score_max,
                "score_medio": score_medio,
                "severidade_z": converter_num(
                    categoria["severidade_z"]
                ),
                "cobertura_assinatura": converter_num(
                    categoria["cobertura_assinatura"]
                ),
                "metricas_destaque": metricas_destaque,
            }
        )

    quantidade_anomalias = (
        int((df_detectado["anomalia"] == -1).sum())
        if not df_detectado.empty
        else 0
    )

    return {
        "schema_version": "2.0",
        "metadados": {
            "gerado_em": datetime.now(
                FUSO_TZ
            ).isoformat(),
            "modelo_versao": MODELO_VERSAO,
            "janela_horas": int(HORAS_JANELA),
            "quantidade_registros": int(
                len(df_detectado)
            ),
            "quantidade_anomalias": quantidade_anomalias,
            "quantidade_episodios": len(episodios_json),
            "avisos": avisos,
        },
        "episodios": episodios_json,
    }


def mesclar_episodios(historico, novos):
    resultado = list(historico)

    for novo in novos:
        inicio_novo = instante(
            novo["inicio"]
        )

        fim_novo = instante(
            novo["fim"]
        )

        correspondencia = None

        for indice, existente in enumerate(resultado):
            if existente["hostname"] != novo["hostname"]:
                continue

            inicio_existente = instante(
                existente["inicio"]
            )

            fim_existente = instante(
                existente["fim"]
            )

            distancia = max(
                inicio_existente - fim_novo,
                inicio_novo - fim_existente,
                timedelta(0),
            )

            if distancia <= timedelta(minutes=GAP_MINUTOS):
                correspondencia = indice
                break

        if correspondencia is None:
            resultado.append(novo)
            continue

        existente = resultado[correspondencia]

        inicio_final = min(
            instante(
                existente["inicio"]
            ),
            inicio_novo,
        )

        fim_final = max(
            instante(
                existente["fim"]
            ),
            fim_novo,
        )

        scores = [
            valor
            for valor in (existente.get("score"), novo.get("score"))
            if valor is not None
        ]

        scores_medios = [
            valor
            for valor in (existente.get("score_medio"), novo.get("score_medio"))
            if valor is not None
        ]

        resultado[correspondencia] = {
            **existente,
            **novo,
            "episodio_id": (
                f"{novo['hostname']}|{inicio_final.isoformat()}"
            ),
            "inicio": inicio_final.isoformat(),
            "fim": fim_final.isoformat(),
            "duracao_segundos": int(
                (fim_final - inicio_final).total_seconds()
            ),
            "quantidade_registros_anomalos": max(
                existente["quantidade_registros_anomalos"],
                novo["quantidade_registros_anomalos"],
            ),
            "score": max(scores) if scores else None,
            "score_medio": max(scores_medios) if scores_medios else None,
        }

    resultado.sort(
        key=lambda episodio: (
            episodio["hostname"],
            episodio["inicio"],
        )
    )

    return resultado


def ler_json_com_etag(chave, padrao):
    try:
        resposta = s3_client.get_object(
            Bucket=os.getenv("BUCKET_NAME"),
            Key=chave,
        )

        try:
            conteudo = resposta["Body"].read().decode("utf-8")
            return json.loads(conteudo), resposta.get("ETag")
        finally:
            resposta["Body"].close()

    except ClientError as erro:
        codigo = erro.response.get("Error", {}).get("Code")

        if codigo in ("NoSuchKey", "404", "NotFound"):
            return padrao, None

        raise


def ler_json(chave, padrao):
    return ler_json_com_etag(chave, padrao)[0]


def podar_historico(historico, referencia):
    limite = referencia - timedelta(hours=HORAS_JANELA)
    podados = []

    for episodio in historico:
        fim = instante(episodio.get("fim"))

        if not pd.isna(fim) and fim >= limite:
            podados.append(episodio)

    return podados


def atualizar_historico_interpretacao(novos):
    fins = [
        instante(episodio["fim"])
        for episodio in novos
    ]
    fins = [fim for fim in fins if not pd.isna(fim)]
    referencia = max(fins) if fins else pd.Timestamp.now(tz=FUSO)

    bucket = os.getenv("BUCKET_NAME")

    for _ in range(3):
        historico, etag = ler_json_com_etag(
            CHAVE_HISTORICO,
            [],
        )

        if not isinstance(historico, list):
            raise ValueError("Histórico de episódios inválido.")

        atualizado = mesclar_episodios(
            podar_historico(historico, referencia),
            novos,
        )

        opcoes = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}

        try:
            gravar(
                s3_client,
                bucket,
                CHAVE_HISTORICO,
                atualizado,
                **opcoes,
            )
            return atualizado
        except ClientError as erro:
            if erro.response["Error"]["Code"] not in ("PreconditionFailed", "412"):
                raise

    raise RuntimeError("Não foi possível atualizar o histórico de episódios.")


def agregar_categorias(historico):
    contagem = {}

    for episodio in historico:
        categoria = episodio["categoria"]["nome"]

        contagem[categoria] = (
            contagem.get(categoria, 0) + 1
        )

    return [
        {
            "nome": nome,
            "quantidade_episodios": quantidade,
        }
        for nome, quantidade in sorted(
            contagem.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]


def agregar_nodes(historico):
    contagem = {}

    for episodio in historico:
        hostname = episodio["hostname"]

        contagem[hostname] = (
            contagem.get(hostname, 0) + 1
        )

    return [
        {
            "hostname": hostname,
            "quantidade_episodios": quantidade,
        }
        for hostname, quantidade in sorted(
            contagem.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]


def agregar_scores(historico):
    agrupado = {}

    for episodio in historico:
        nome = episodio["categoria"]["nome"]
        score = episodio.get("score")

        if score is None:
            continue

        agrupado.setdefault(nome, []).append(float(score))

    return [
        {
            "nome": nome,
            "score_max": max(valores),
            "score_medio": sum(valores) / len(valores),
        }
        for nome, valores in sorted(
            agrupado.items(),
            key=lambda item: (-max(item[1]), item[0]),
        )
    ]


def agregar_matriz(historico):
    matriz = {}

    for episodio in historico:
        categoria = episodio["categoria"]["nome"]
        hostname = episodio["hostname"]
        linha = matriz.setdefault(categoria, {})
        linha[hostname] = linha.get(hostname, 0) + 1

    return matriz


def agregar_filtros(nos):
    hpc = {}
    cluster = {}

    for no in nos or []:
        if no.get("hpc_id") is not None:
            hpc.setdefault(no["hpc_id"], no.get("hpc"))
        if no.get("cluster_id") is not None:
            cluster.setdefault(no["cluster_id"], no.get("cluster"))

    return {
        "hpc": [
            {"id": chave, "nome": nome}
            for chave, nome in sorted(hpc.items())
        ],
        "cluster": [
            {"id": chave, "nome": nome}
            for chave, nome in sorted(cluster.items())
        ],
    }


def dados_dash(historico, nos=None):
    categorias = agregar_categorias(
        historico
    )

    nodes = agregar_nodes(
        historico
    )

    scores = agregar_scores(
        historico
    )

    matriz = agregar_matriz(
        historico
    )

    total_episodios = len(historico)

    node_mais_problematico = (
        nodes[0]
        if nodes
        else None
    )

    categoria_mais_comum = (
        categorias[0]
        if categorias
        else None
    )

    categoria_mais_severa = (
        scores[0]
        if scores
        else None
    )

    return {
        "schema_version": "2.0",
        "metadados": {
            "gerado_em": datetime.now(
                FUSO_TZ
            ).isoformat(),
            "fuso_horario": FUSO,
            "janela_horas": HORAS_JANELA,
            "modelo_versao": MODELO_VERSAO,
            "total_episodios_historicos": total_episodios,
            "total_nodes": len(nodes),
            "total_categorias": len(categorias),
        },
        "indicadores": {
            "node_mais_problematico": node_mais_problematico,
            "categoria_mais_comum": categoria_mais_comum,
            "categoria_mais_severa": categoria_mais_severa,
            "total_episodios": total_episodios,
        },
        "categorias": categorias,
        "nodes": nodes,
        "matriz_categoria_node": matriz,
        "filtros": agregar_filtros(nos),
        "episodios": historico,
    }


def processar_categorizacao():
    modelo, baseline = baixar_arquivos_modelo()
    features = colunas_modelo()

    nos = buscar_todos()
    hosts = [no["hostname"] for no in nos]
    mapa_nos = {no["hostname"]: no for no in nos}

    bucket = os.getenv("BUCKET_NAME")
    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

    agora = pd.Timestamp.now(tz=FUSO)
    inicio = agora - timedelta(hours=HORAS_JANELA)
    bucket_silver = os.getenv("SILVER_BUCKET_NAME", bucket)

    df = carregar_silver(
        s3_client,
        bucket_silver,
        hosts,
        agora,
        inicio=inicio,
        colunas=features,
    )

    relatorio = executar_interpretacao(
        df,
        modelo,
        features,
        baseline,
    )

    for episodio in relatorio["episodios"]:
        no = mapa_nos.get(episodio["hostname"], {})
        episodio.update({
            "hpc_id": no.get("hpc_id"),
            "hpc": no.get("hpc"),
            "cluster_id": no.get("cluster_id"),
            "cluster": no.get("cluster"),
            "node_id": no.get("node_id"),
        })

    historico = atualizar_historico_interpretacao(
        relatorio["episodios"]
    )

    dashboard = dados_dash(
        historico,
        nos,
    )

    gravar(
        s3_client,
        bucket,
        CHAVE_INTERPRETACAO_LATEST,
        relatorio,
    )

    gravar(
        s3_client,
        bucket,
        CHAVE_DASHBOARD,
        dashboard,
    )

    return dashboard


def lambda_handler_categorizacao(event, context):
    if not os.getenv("BUCKET_NAME"):
        raise ValueError("Configure a variável BUCKET_NAME na Lambda.")

    return processar_categorizacao()


def executar_categorizacao():
    return processar_categorizacao()
