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
Gestor: python silver2gold.py ou silver2gold.lambda_handler na Lambda.
Analista: MODO_LOCAL=1 no teste local ou silver2gold.lambda_handler_analista na Lambda.
"""
import io
import json
import math
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

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


# ETL de categorização de anomalias.
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

    from config_aws import s3_client

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
    import joblib

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
    from config_aws import s3_client

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
    from config_aws import s3_client

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
    from config_aws import s3_client

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


# ETL do analista.
# os.getenv("NOME", "padrao") devolve o valor da variável de ambiente ou o
# padrão informado. Assim o script funciona sem configuração obrigatória.
GOLD_ANALISTA = os.getenv("S3_KEY", "03-gold/analista").rstrip("/")

# Quantas semanas aparecem no gráfico. 24 semanas ~ 6 meses.
SEMANAS = int(os.getenv("ANALISTA_SEMANAS", "24"))

# A página compara o semestre atual com o anterior. Ler 48 semanas dobra o volume
# de arquivos, então esse trecho é opcional: "1" liga, "0" desliga (padrão).
CARREGAR_ANTERIOR = os.getenv("ANALISTA_CARREGAR_ANTERIOR", "0") == "1"
SEMANAS_TOTAL = SEMANAS * 2 if CARREGAR_ANTERIOR else SEMANAS
JANELA_DIAS = SEMANAS_TOTAL * 7  # dias de histórico que serão lidos.

# Regras de negócio usadas pelo JSON e pela página, para que os dois falem a
# mesma língua (o front usa os mesmos limites para desenhar os pontos).
LIMITE_PICO = float(os.getenv("ANALISTA_LIMITE_PICO", "80"))    # acima disto é "pico".
LIMITE_RESIDUO = float(os.getenv("ANALISTA_LIMITE_RESIDUO", "5"))  # acima disto é "anomalia".
MAX_ANOMALIAS = 3        # a página destaca no máximo 3 anomalias por componente.
MAX_PONTOS_CORRELACAO = 120  # a página desenha no máximo 120 pontos no dispersão.

# "MODO_LOCAL=1" faz a ETL ler uma pasta do computador em vez do S3.
# Serve para testar e aprender sem credenciais da AWS.
# O padrão é "0" (produção/Lambda), para não ler disco sem querer. Quem quiser
# testar sem AWS deve ligar explicitamente: $env:MODO_LOCAL="1".
MODO_LOCAL = os.getenv("MODO_LOCAL", "0") == "1"


def caminho_local(valor):
    """Devolve um caminho absoluto para pastas locais de entrada/saída.

    Isso evita um erro comum: executar o script da raiz do projeto e o Python
    procurar "silver_local" na raiz, em vez de dentro de captura-de-dados.
    Se a variável de ambiente já vier com um caminho absoluto, mantemos ele.
    """
    caminho = Path(valor).expanduser()       # expanduser resolve o "~" da casa.
    if not caminho.is_absolute():            # relativo: usa a pasta do script.
        caminho = Path(__file__).resolve().parent / caminho
    return str(caminho.resolve())            # resolve tira os ".." e deixa completo.


SILVER_LOCAL_DIR = caminho_local(os.getenv("SILVER_LOCAL_DIR", "silver_local"))
GOLD_LOCAL_DIR = caminho_local(os.getenv("GOLD_LOCAL_DIR", "gold_local"))

# Cada componente do gráfico é identificado por uma chave curta ("cpu") e pelo
# nome da coluna correspondente no CSV ("CPU_PERCENT").
COMPONENTES = {
    "cpu": "CPU_PERCENT",
    "ram": "RAM_PERCENT",
    "disco": "DISCO_PERCENT",
}

# Nomes exibidos nas legendas e nas mensagens.
NOMES = {"cpu": "CPU", "ram": "RAM", "disco": "Disco"}

# Colunas que a ETL precisa converter para número. Se alguma não existir no CSV,
# ela é criada vazia (NaN) para o código não quebrar.
COLUNAS_ANALISTA = [
    "CPU_PERCENT",
    "RAM_PERCENT",
    "CPU_IOWAIT_PERCENT",
    "DISCO_PERCENT",
    "DISCO_USED",
    "DISCO_TOTAL",
]

# Bordas dos "baldes" do histograma: 0.0, 0.1, 0.2 ... 100.0.
# np.linspace(inicio, fim, quantidade) cria uma sequência igualmente espaçada.
# Com 1001 bordas temos 1000 baldes de 0,1 ponto percentual cada.
BINS = np.linspace(0.0, 100.0, 1001)

# Abreviações em português para montar rótulos curtos ("07 abr") no eixo X.
MESES_CURTOS = ["jan", "fev", "mar", "abr", "mai", "jun",
                "jul", "ago", "set", "out", "nov", "dez"]


# ---------------------------------------------------------------------------
# 2. FUNÇÕES PEQUENAS E REUTILIZÁVEIS
# ---------------------------------------------------------------------------
def buscar_hosts_analista(cluster_id):
    """Descobre quais nodes pertencem ao cluster.

    Ordem de preferência:
      1. HOSTNAMES (JSON em variável de ambiente) -> não precisa de banco, é o
         modo usado na Lambda.
      2. MySQL (tabela node) -> modo usado rodando na EC2/servidor.
    """
    # No modo local sem HOSTNAMES, devolvemos None e o script aceita qualquer
    # hostname que aparecer nos CSVs (útil para testar com dados de exemplo).
    if MODO_LOCAL and not os.getenv("HOSTNAMES"):
        return None

    if os.getenv("HOSTNAMES"):
        # json.loads converte o texto '["a","b"]' em uma lista Python ["a","b"].
        hosts = json.loads(os.environ["HOSTNAMES"])

        # all(...) devolve True somente se TODOS os itens passarem no teste.
        # Aqui: precisa ser lista, não pode estar vazia e todo item tem que ser
        # texto não vazio. Caso contrário, levantamos um erro explicativo.
        if not isinstance(hosts, list) or not hosts or not all(
                isinstance(host, str) and host.strip() for host in hosts):
            raise ValueError("HOSTNAMES deve ser uma lista JSON com nomes de nodes.")

        # sorted(set(...)) remove repetidos e ordena alfabeticamente.
        return sorted(set(hosts))

    # Import feito aqui (e não no topo) porque só é necessário neste caminho.
    import pymysql

    # pymysql.connect abre a conexão. os.environ["DB_HOST"] sem valor padrão
    # provoca KeyError se a variável não existir: é intencional, para falhar cedo.
    conexao = pymysql.connect(
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_DATABASE"],
        charset="utf8mb4",
    )
    try:
        # "with" garante que o cursor (o objeto que executa SQL) seja fechado.
        with conexao.cursor() as cursor:
            # "%s" é um espaço reservado: o driver coloca o valor com segurança.
            cursor.execute(
                "SELECT hostname FROM node WHERE fk_cluster = %s ORDER BY hostname",
                (cluster_id,),
            )
            # fetchall() devolve uma lista de tuplas; r[0] é a primeira coluna.
            return [linha[0] for linha in cursor.fetchall()]
    finally:
        # "finally" roda mesmo se der erro, evitando conexões abertas para sempre.
        conexao.close()


def instante(valor):
    """Converte um texto de data/hora do CSV em um instante com fuso horário.

    CSV pode vir "2026-09-15 14:32:00" (sem fuso). Assumimos que está no fuso
    ORIGEM e convertemos para FUSO. Qualquer coisa inválida vira pd.NaT
    (o "valor vazio" de datas no pandas).
    """
    try:
        t = pd.Timestamp(valor)  # cria um ponto no tempo a partir do texto.

        # pd.isna verifica se o valor é vazio/inválido.
        if pd.isna(t):
            return pd.NaT

        # tzinfo None significa "sem fuso". Nesse caso, carimbamos o fuso de
        # origem. ambiguous/nonexistent="NaT" evita erro nos dias de horário de
        # verão (quando uma hora existe duas vezes ou não existe).
        if t.tzinfo is None:
            t = t.tz_localize(ORIGEM, ambiguous="NaT", nonexistent="NaT")

        # Converter para o fuso final (America/Sao_Paulo).
        return t.tz_convert(FUSO)
    except (ValueError, TypeError):
        # Se o texto não for uma data, devolvemos NaT em vez de derrubar a ETL.
        return pd.NaT


def data_da_chave(chave):
    """Extrai a data do caminho do arquivo no S3.

    As chaves têm o formato:
        .../hostname=node01/ano=2026/mes=09/dia=15/data_20260915_143200.csv
    Ler a data pelo caminho evita baixar arquivos antigos só para descartá-los.
    """
    partes = {}
    for pedaco in chave.split("/"):     # quebra o caminho nas barras.
        if "=" in pedaco:               # só interessam pedaços "nome=valor".
            nome, valor = pedaco.split("=", 1)  # 1 = divide só na primeira "=".
            partes[nome] = valor
    try:
        return pd.Timestamp(
            year=int(partes["ano"]),
            month=int(partes["mes"]),
            day=int(partes["dia"]),
        )
    except (KeyError, ValueError):
        # KeyError = falta ano/mes/dia; ValueError = número inválido.
        return None


def limpar_json(valor):
    """Deixa qualquer estrutura Python segura para virar JSON.

    Converte datas em texto ISO, números numpy em números comuns e NaN/infinito
    em None (o JSON não aceita NaN nem infinito).
    """
    if isinstance(valor, dict):                      # dicionário -> varre valores.
        return {chave: limpar_json(item) for chave, item in valor.items()}
    if isinstance(valor, (list, tuple)):             # lista/tupla -> varre itens.
        return [limpar_json(item) for item in valor]
    if isinstance(valor, pd.Timestamp):              # data do pandas -> texto.
        return valor.isoformat()
    if valor is None or valor is pd.NA:              # vazio -> null no JSON.
        return None
    if isinstance(valor, np.generic):                # número numpy -> número Python.
        return limpar_json(valor.item())
    if isinstance(valor, float) and not math.isfinite(valor):
        return None                                  # NaN e infinito -> null.
    return valor


def formatar_duracao(segundos):
    """Converte segundos em texto curto: 9420 -> '2h 37min'."""
    if segundos is None:
        return None
    minutos = int(round(segundos / 60))   # round arredonda; int corta decimais.
    horas, minutos = divmod(minutos, 60)  # divmod devolve (quociente, resto).
    if horas and minutos:
        return f"{horas}h {minutos}min"
    if horas:
        return f"{horas}h"
    return f"{minutos}min"


def rotulo_semana(semana):
    """Cria o rótulo curto do eixo X: '07 abr'."""
    return f"{semana.day:02d} {MESES_CURTOS[semana.month - 1]}"


def inteiro_env(nome, padrao=0):
    """Lê uma variável de ambiente numérica, ignorando texto vazio.

    Evita o erro int("") quando a variável existe mas está em branco.
    """
    texto = os.getenv(nome)
    if texto is None or not str(texto).strip():
        return padrao
    return int(texto)


# ---------------------------------------------------------------------------
# 3. ACUMULADORES: memória por node enquanto os arquivos são lidos
# ---------------------------------------------------------------------------
# Em vez de juntar 6 meses de linhas em uma tabela gigante, guardamos apenas
# resumos: somas, contagens, histogramas e listas curtas. A memória usada fica
# praticamente constante, não importa o tamanho do histórico.
def novo_acumulador():
    """Cria um acumulador vazio para um node."""
    return {
        # Somas e contagens por semana e por componente.
        # Formato: {semana: {"CPU_PERCENT_soma": ..., "CPU_PERCENT_n": ...}}
        "semanas": {},

        # Um histograma por componente: quantas amostras caíram em cada balde
        # de 0,1 ponto percentual. np.zeros(criar) preenche tudo com zero.
        "histograma": {
            chave: np.zeros(BINS.size - 1, dtype="int64") for chave in COMPONENTES
        },

        # Instantes em que cada componente passou de LIMITE_PICO. A lista é
        # curta porque só guarda eventos raros.
        "picos": {chave: [] for chave in COMPONENTES},

        # Somas necessárias para calcular a correlação de forma incremental:
        # n = quantidade de pares; sx/sy = soma de x/y; sxx/syy/sxy = somas dos
        # produtos. Com isso obtemos r e a reta sem guardar todos os pontos.
        "correlacao": {"n": 0, "sx": 0.0, "sy": 0.0,
                       "sxx": 0.0, "syy": 0.0, "sxy": 0.0},

        # Amostra limitada de pontos (x, y) apenas para desenhar o dispersão.
        "pontos": [],

        "amostras": 0,          # total de linhas válidas lidas.
        "ultima_leitura": None, # maior TIMESTAMP visto.
    }


def garantir_host(acumuladores, host):
    """Devolve o acumulador do node, criando-o na primeira vez que aparece."""
    if host not in acumuladores:          # "in" testa se a chave existe no dict.
        acumuladores[host] = novo_acumulador()
    return acumuladores[host]


def acumular_semanas(acumulador, df):
    """Soma, conta e guarda o máximo de cada componente por semana.

    O máximo semanal é necessário para comparar o maior pico do período atual
    com o do período anterior sem precisar reler os CSVs.
    """
    # groupby(["HOSTNAME", "SEMANA"]) percorre a tabela em blocos: cada bloco
    # reúne as linhas de um node em uma semana. "para cada (host, semana)".
    for (_host, semana), grupo in df.groupby(["HOSTNAME", "SEMANA"]):
        # setdefault cria a semana com um dicionário vazio se ainda não existir.
        resumo = acumulador["semanas"].setdefault(semana, {})

        for coluna in COMPONENTES.values():   # .values() percorre só os valores.
            # min_count=1 faz a soma virar NaN quando todas as amostras são
            # vazias (em vez de virar 0, que seria um valor falso).
            resumo[coluna + "_soma"] = resumo.get(coluna + "_soma", 0.0) + grupo[coluna].sum(min_count=1)
            resumo[coluna + "_n"] = resumo.get(coluna + "_n", 0) + grupo[coluna].count()

            # Máximo da semana e o instante em que ele aconteceu.
            serie = grupo[coluna].dropna()
            if serie.empty:
                continue
            posicao = serie.idxmax()                  # índice da maior amostra.
            maximo = float(serie.loc[posicao])
            momento = grupo.loc[posicao, "TIMESTAMP"]
            guardado = resumo.get(coluna + "_max")

            # Só substitui se o máximo desta parte do arquivo for maior.
            if guardado is None or pd.isna(guardado) or maximo > guardado:
                resumo[coluna + "_max"] = maximo
                resumo[coluna + "_max_ts"] = momento


def acumular_histograma(acumulador, df):
    """Conta quantas amostras caem em cada faixa de 0,1 ponto percentual."""
    for chave, coluna in COMPONENTES.items():
        # dropna() remove vazios; to_numpy converte a coluna em array numpy.
        valores = df[coluna].dropna().to_numpy(dtype=float)
        if valores.size == 0:
            continue  # size é o número de elementos do array.

        # np.histogram conta quantos valores caem em cada intervalo de BINS.
        # [0] pega só as contagens (a posição [1] traria as bordas).
        contagens = np.histogram(valores, bins=BINS)[0]
        acumulador["histograma"][chave] += contagens


def acumular_picos(acumulador, df):
    """Guarda os instantes em que cada componente passou do limite de pico."""
    for chave, coluna in COMPONENTES.items():
        acima = df.loc[df[coluna] > LIMITE_PICO, "TIMESTAMP"]
        if acima.empty:      # .empty é True quando não há nenhuma linha.
            continue
        # extend adiciona todos os itens da série ao final da lista.
        acumulador["picos"][chave].extend(acima.tolist())


def acumular_correlacao(acumulador, df):
    """Soma as estatísticas da correlação entre uso do disco e IO wait da CPU."""
    # dropna() mantém apenas as linhas em que os DOIS valores existem: um par
    # incompleto estragaria a correlação.
    pares = df[["DISCO_PERCENT", "CPU_IOWAIT_PERCENT"]].dropna()
    if pares.empty:
        return

    x = pares["DISCO_PERCENT"].to_numpy(dtype=float)   # eixo X do gráfico.
    y = pares["CPU_IOWAIT_PERCENT"].to_numpy(dtype=float)  # eixo Y do gráfico.

    # Somatórios incrementais (fórmula de correlação de Pearson).
    acumulador["correlacao"]["n"] += int(x.size)
    acumulador["correlacao"]["sx"] += float(x.sum())
    acumulador["correlacao"]["sy"] += float(y.sum())
    acumulador["correlacao"]["sxx"] += float((x * x).sum())
    acumulador["correlacao"]["syy"] += float((y * y).sum())
    acumulador["correlacao"]["sxy"] += float((x * y).sum())

    # Guarda alguns pontos só para o gráfico de dispersão. O limite evita
    # crescer sem controle; o recorte final (MAX_PONTOS_CORRELACAO) vem depois.
    espaco = MAX_PONTOS_CORRELACAO - len(acumulador["pontos"])
    if espaco > 0:
        for valor_x, valor_y in zip(x[:espaco], y[:espaco]):
            acumulador["pontos"].append(
                {"x": round(float(valor_x), 2), "y": round(float(valor_y), 2)}
            )


# ---------------------------------------------------------------------------
# 4. LEITURA DA SILVER
# ---------------------------------------------------------------------------
def preparar_chunk(df, hosts, inicio, fim):
    """Limpa um pedaço (chunk) do CSV e devolve só o que interessa."""
    # issubset pergunta: as colunas obrigatórias estão todas presentes?
    if not {"HOSTNAME", "TIMESTAMP"}.issubset(df.columns):
        raise ValueError("CSV sem as colunas HOSTNAME/TIMESTAMP.")

    # Se a lista de hosts foi informada, descarta os demais nodes.
    if hosts is not None:
        # isin devolve True/False por linha; df[máscara] mantém as linhas True.
        df = df[df["HOSTNAME"].isin(hosts)]
    if df.empty:
        return df

    # .copy() cria uma tabela própria, para podermos alterar sem avisos do pandas.
    df = df.copy()

    # map(instante) aplica a função a cada valor da coluna. O 'utc=True' garante
    # que o resultado tenha fuso; depois convertemos tudo para FUSO.
    df["TIMESTAMP"] = pd.to_datetime(
        df["TIMESTAMP"].map(instante), utc=True
    ).dt.tz_convert(FUSO)

    # between(inicio, fim) mantém somente a janela de análise.
    df = df[df["TIMESTAMP"].between(inicio, fim)].copy()
    if df.empty:
        return df

    for coluna in COLUNAS_ANALISTA:
        # Se a coluna não existe no CSV, criamos com NaN (np.nan) para que os
        # cálculos seguintes encontrem sempre o mesmo conjunto de colunas.
        if coluna not in df.columns:
            df[coluna] = np.nan

        # to_numeric com errors="coerce" transforma texto em número e troca o
        # que não for número por NaN.
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce").astype("float64")

        # where(condição) mantém o valor onde a condição é True e troca por NaN
        # onde é False. Aqui: descarta negativos e valores não finitos.
        df[coluna] = df[coluna].where(np.isfinite(df[coluna]) & (df[coluna] >= 0))

        # Percentuais não podem passar de 100.
        if coluna.endswith("_PERCENT"):
            df[coluna] = df[coluna].where(df[coluna] <= 100)

    # Semanas ISO começam na segunda-feira. dayofweek devolve 0 (segunda) a 6.
    # normalize() zera horas/minutos/segundos, deixando apenas a data.
    df["SEMANA"] = df["TIMESTAMP"].dt.normalize() - pd.to_timedelta(
        df["TIMESTAMP"].dt.dayofweek, unit="D"
    )

    # Descarta linhas em que TODOS os componentes estão vazios.
    componentes = list(COMPONENTES.values())
    df = df.dropna(subset=componentes, how="all")

    return df


def processar_chunk(df, hosts, inicio, fim, acumuladores):
    """Limpa o chunk, alimenta os acumuladores e devolve quantas linhas sobraram."""
    df = preparar_chunk(df, hosts, inicio, fim)
    if df.empty:
        return 0

    # Pode haver mais de um node no mesmo chunk, por isso agrupamos por HOSTNAME.
    for host, grupo in df.groupby("HOSTNAME"):
        acumulador = garantir_host(acumuladores, host)
        acumular_semanas(acumulador, grupo)
        acumular_histograma(acumulador, grupo)
        acumular_picos(acumulador, grupo)
        acumular_correlacao(acumulador, grupo)

        acumulador["amostras"] += int(len(grupo))   # len = número de linhas.

        # máximo do grupo, para sabermos até quando há telemetria.
        maior = grupo["TIMESTAMP"].max()
        if acumulador["ultima_leitura"] is None or maior > acumulador["ultima_leitura"]:
            acumulador["ultima_leitura"] = maior

    return int(len(df))


def ler_silver_s3(s3, bucket, hosts, inicio, fim, acumuladores):
    """Percorre os CSVs da Silver no S3 e alimenta os acumuladores."""
    lidos = 0
    ignorados = 0

    # paginate lista o bucket em páginas: o S3 nunca devolve tudo de uma vez.
    paginas = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=SILVER)

    for pagina in paginas:
        # .get("Contents", []) evita KeyError quando a "pasta" está vazia.
        for item in pagina.get("Contents", []):
            chave = item["Key"]
            if not chave.endswith(".csv"):
                continue

            # Pula arquivos cuja data no caminho está fora da janela.
            # Comparamos apenas a data (.date()) para não misturar fuso horário.
            data = data_da_chave(chave)
            if data is not None and data.date() < inicio.date():
                ignorados += 1
                continue

            corpo = s3.get_object(Bucket=bucket, Key=chave)["Body"]
            try:
                # TextIOWrapper transforma bytes em texto; utf-8-sig remove o BOM
                # (aquele caractere invisível que o Excel coloca no começo).
                with io.TextIOWrapper(corpo, encoding="utf-8-sig") as texto:
                    # chunksize limita quantas linhas ficam na memória por vez.
                    for df in pd.read_csv(texto, sep=";", chunksize=50000):
                        lidos += processar_chunk(df, hosts, inicio, fim, acumuladores)
            finally:
                # close() libera a conexão HTTP, mesmo se o CSV estiver corrompido.
                corpo.close()

    print(f"Silver (S3): {lidos} linhas válidas | {ignorados} arquivos fora da janela")
    return lidos, ignorados


def ler_silver_local(hosts, inicio, fim, acumuladores):
    """Mesma ideia da leitura S3, mas lendo uma pasta do computador."""
    pasta = Path(SILVER_LOCAL_DIR)
    if not pasta.is_dir():
        raise ValueError(f"Pasta local não encontrada: {SILVER_LOCAL_DIR}")

    lidos = 0
    ignorados = 0

    # rglob("*.csv") procura recursivamente em todas as subpastas.
    for caminho in sorted(pasta.rglob("*.csv")):
        # A data é obtida do caminho relativo, igual ao formato do S3.
        data = data_da_chave(caminho.as_posix())
        if data is not None and data.date() < inicio.date():
            ignorados += 1
            continue

        for df in pd.read_csv(caminho, sep=";", chunksize=50000, encoding="utf-8-sig"):
            lidos += processar_chunk(df, hosts, inicio, fim, acumuladores)

    print(f"Silver (local): {lidos} linhas válidas | {ignorados} arquivos fora da janela")
    return lidos, ignorados


# ---------------------------------------------------------------------------
# 5. CÁLCULOS
# ---------------------------------------------------------------------------
def percentil(contagens, valor_percentil):
    """Calcula um percentil a partir do histograma acumulado.

    Devolve None quando não há amostras. A precisão é de 0,05 ponto percentual,
    porque o valor é o centro do balde onde o percentil cai.
    """
    total = int(contagens.sum())
    if total == 0:
        return None

    # cumsum soma progressivamente: [3,4,1] vira [3,7,8].
    acumulado = np.cumsum(contagens)

    # Alvo: por exemplo, p90 de 1000 amostras -> 900º valor.
    alvo = (valor_percentil / 100.0) * total

    # searchsorted acha a posição onde o alvo "entraria" na lista acumulada.
    indice = int(np.searchsorted(acumulado, alvo, side="left"))
    indice = min(indice, len(contagens) - 1)  # protege contra estouro do índice.

    # BINS[indice] é a borda inferior do balde; +0.05 é o centro dele.
    return round(float(BINS[indice] + 0.05), 1)


def regressao_linear(valores):
    """Ajusta uma reta aos valores e devolve inclinação, intercepto e resíduos.

    valores: lista de números na ordem dos índices 0, 1, 2, ...
    Reta: y = intercepto + inclinacao * x
    """
    quantidade = len(valores)
    if quantidade < 2:
        return None  # sem dois pontos não existe reta.

    xs = np.arange(quantidade, dtype=float)      # [0, 1, 2, ...]
    ys = np.array(valores, dtype=float)

    # np.polyfit(grau=1) devolve [inclinacao, intercepto].
    inclinacao, intercepto = np.polyfit(xs, ys, 1)

    # Resíduo = quanto o valor real se afastou da reta (positivo ou negativo).
    residuos = ys - (intercepto + inclinacao * xs)

    return {
        "inclinacao": float(inclinacao),
        "intercepto": float(intercepto),
        "residuos": residuos,
    }


def contar_eventos(instantes, passo):
    """Transforma uma lista de instantes de pico em eventos contínuos.

    Exemplo com passo de 10 s e instantes 12:00:00, 12:00:10, 12:00:20, 12:05:00:
      - os três primeiros formam UM evento de 3 amostras (30 s);
      - o último, separado por 4min40s, é OUTRO evento de 1 amostra (10 s).
    """
    if not instantes:
        return {
            "eventos": 0,
            "amostras": 0,
            "duracao_acumulada_segundos": 0.0,
            "duracao_evento_mais_longo_segundos": 0.0,
        }

    # sorted organiza do mais antigo para o mais recente.
    ordenados = sorted(instantes)

    # Se duas amostras estão a mais de 1,5 passo de distância, consideramos que
    # o pico "parou" entre elas (houve uma leitura sem pico no meio).
    limite = pd.Timedelta(seconds=passo * 1.5)

    eventos = 1                 # o primeiro instante já inicia um evento.
    sequencia_atual = 1
    maior_sequencia = 1

    for anterior, atual in zip(ordenados, ordenados[1:]):
        # zip(lista, lista[1:]) percorre pares consecutivos: (1,2), (2,3)...
        if atual - anterior <= limite:
            sequencia_atual += 1
        else:
            eventos += 1
            sequencia_atual = 1

        # max mantém sempre a maior sequência vista até agora.
        maior_sequencia = max(maior_sequencia, sequencia_atual)

    return {
        "eventos": eventos,
        "amostras": len(ordenados),
        # Cada amostra acima do limite representa aproximadamente um passo.
        "duracao_acumulada_segundos": float(len(ordenados) * passo),
        "duracao_evento_mais_longo_segundos": float(maior_sequencia * passo),
    }


def minimo_amostras(inicio_semana, fim_semana, inicio_janela, fim_janela):
    """Quantas amostras uma semana precisa ter para a média ser considerada válida.

    Considera apenas a parte da semana que está dentro da janela analisada
    (a semana atual e a primeira semana são parciais).
    """
    # max/min escolhem os extremos; a diferença dá a duração real do trecho.
    comeco = max(inicio_semana, inicio_janela)
    fim = min(fim_semana, fim_janela)

    segundos = (fim - comeco).total_seconds()
    if segundos <= 0:
        return 1

    # ceil arredonda para cima: 80% de 6048 amostras -> 4839.
    return max(1, math.ceil(segundos / PASSO * COBERTURA))


def montar_semanas(acumulador, semanas_datas, inicio, fim):
    """Monta a série semanal e os percentis de cada componente."""
    resultado = {}

    for chave in COMPONENTES:                 # "cpu", "ram", "disco"
        coluna = COMPONENTES[chave]
        medias = []       # médias semanais, na ordem de semanas_datas.
        amostras = []     # quantas amostras sustentam cada média.
        validos = []      # pares (posição, valor) só das semanas com média.

        # enumerate devolve (índice, item) ao mesmo tempo.
        for posicao, semana in enumerate(semanas_datas):
            resumo = acumulador["semanas"].get(semana, {})
            soma = resumo.get(coluna + "_soma")
            quantidade = resumo.get(coluna + "_n", 0)

            # Média válida = existe soma, existem amostras e a cobertura mínima
            # daquela semana foi atingida.
            necessario = minimo_amostras(
                semana, semana + pd.Timedelta(days=7), inicio, fim
            )
            # pd.isna cobre tanto None quanto NaN (soma sem nenhuma amostra).
            if soma is None or pd.isna(soma) or quantidade < necessario:
                media = None
            else:
                media = round(float(soma) / float(quantidade), 1)

            medias.append(media)
            amostras.append(int(quantidade))
            if media is not None:
                validos.append((posicao, media))   # usado na regressão.

        # Regressão e anomalias das médias semanais.
        indices = [posicao for posicao, _ in validos]
        valores = [valor for _, valor in validos]
        ajuste = regressao_linear(valores)

        anomalias = []
        inclinacao = None
        intercepto = None

        if ajuste is not None:
            inclinacao = round(ajuste["inclinacao"], 4)
            intercepto = round(ajuste["intercepto"], 2)

            # Percorre os pontos válidos junto com o resíduo correspondente.
            # np.nditer não é necessário: zip alinha as duas listas.
            for posicao, valor, residuo in zip(indices, valores, ajuste["residuos"]):
                # Regra da página: pico (>80) não entra na lista de anomalias,
                # para não contar o mesmo evento duas vezes.
                if valor > LIMITE_PICO:
                    continue
                if abs(float(residuo)) <= LIMITE_RESIDUO:
                    continue

                anomalias.append({
                    "semana": semanas_datas[posicao].date().isoformat(),
                    "rotulo": rotulo_semana(semanas_datas[posicao]),
                    "valor": valor,
                    "residuo": round(abs(float(residuo)), 1),
                })

            # sorted com key ordena pelo campo indicado; reverse=True = maior antes.
            anomalias = sorted(anomalias, key=lambda item: item["residuo"], reverse=True)

        resultado[chave] = {
            "nome": NOMES[chave],
            "medias": medias,
            "amostras": amostras,
            "media_geral": round(float(np.mean(valores)), 1) if valores else None,
            "percentis": {
                "p50": percentil(acumulador["histograma"][chave], 50),
                "p90": percentil(acumulador["histograma"][chave], 90),
                "p95": percentil(acumulador["histograma"][chave], 95),
                "p99": percentil(acumulador["histograma"][chave], 99),
            },
            "regressao": {
                "inclinacao": inclinacao,
                "intercepto": intercepto,
                "linha": [
                    # min/max limitam a reta ao intervalo 0..100%.
                    round(min(100.0, max(0.0, intercepto + inclinacao * posicao)), 1)
                    if ajuste is not None else None
                    for posicao in range(len(semanas_datas))
                ],
            },
            "anomalias": anomalias[:MAX_ANOMALIAS],  # a página destaca no máximo 3.
            "quantidade_anomalias": len(anomalias),
        }

    return resultado


def montar_correlacao(acumulador):
    """Calcula r, r² e a reta a partir dos somatórios guardados."""
    soma = acumulador["correlacao"]
    n = soma["n"]

    base = {
        "n": n,
        "eixo_x": "DISCO_PERCENT",
        "eixo_y": "CPU_IOWAIT_PERCENT",
        "rotulo_x": "Uso do disco (%)",
        "rotulo_y": "IO wait da CPU (%)",
        "observacao": (
            "IOPS não é coletado pela pipeline; a correlação usa o percentual de "
            "uso do disco e o tempo de espera de I/O da CPU."
        ),
        "pontos": acumulador["pontos"],
    }

    if n < 2:
        base.update({"r": None, "r2": None, "inclinacao": None, "intercepto": None})
        return base

    numerador = n * soma["sxy"] - soma["sx"] * soma["sy"]

    # math.sqrt devolve a raiz quadrada. O max(..., 0.0) evita erro de
    # arredondamento quando o denominador é praticamente zero.
    denominador_x = max(0.0, n * soma["sxx"] - soma["sx"] ** 2)
    denominador_y = max(0.0, n * soma["syy"] - soma["sy"] ** 2)
    denominador = math.sqrt(denominador_x * denominador_y)

    if denominador == 0:
        base.update({"r": None, "r2": None, "inclinacao": None, "intercepto": None})
        return base

    r = numerador / denominador
    inclinacao = numerador / denominador_x if denominador_x else None
    intercepto = ((soma["sy"] - inclinacao * soma["sx"]) / n
                  if inclinacao is not None else None)

    base.update({
        "r": round(float(r), 2),
        "r2": round(float(r * r), 2),
        "inclinacao": round(float(inclinacao), 4) if inclinacao is not None else None,
        "intercepto": round(float(intercepto), 2) if intercepto is not None else None,
    })
    return base


def maior_das_semanas(acumulador, semanas, coluna):
    """Maior leitura de um componente dentro de uma lista de semanas.

    Usa o máximo guardado em acumular_semanas, então não é preciso reler os CSVs.
    """
    melhor = None
    for semana in semanas:
        resumo = acumulador["semanas"].get(semana, {})
        valor = resumo.get(coluna + "_max")
        if valor is None or pd.isna(valor):
            continue
        if melhor is None or valor > melhor["valor"]:
            melhor = {
                "valor": round(float(valor), 1),
                "timestamp": resumo.get(coluna + "_max_ts"),
            }
    return melhor


def montar_kpis(acumulador, componentes, semanas_datas, semanas_anteriores):
    """Resume os eventos do node nos cartões de KPI da página.

    Calcula o período atual e também o anterior, porque a página mostra a
    diferença entre os dois (+2 picos, +30 min, +9%).
    """
    # Primeira semana do gráfico: tudo antes dela pertence ao período anterior.
    limite_anterior = semanas_datas[0]

    # Picos do período atual: calculamos por componente e somamos.
    total_eventos = 0
    duracao_total = 0.0
    duracao_mais_longa = 0.0
    resumo_picos = {}

    for chave in COMPONENTES:
        # Só os instantes dentro do período exibido no gráfico.
        atuais = [t for t in acumulador["picos"][chave] if t >= limite_anterior]
        resumo = contar_eventos(atuais, PASSO)

        resumo_picos[chave] = {
            "eventos": resumo["eventos"],
            "amostras": resumo["amostras"],
            "duracao_acumulada_segundos": resumo["duracao_acumulada_segundos"],
            "duracao_acumulada_texto": formatar_duracao(resumo["duracao_acumulada_segundos"]),
            "duracao_evento_mais_longo_segundos": resumo["duracao_evento_mais_longo_segundos"],
        }
        total_eventos += resumo["eventos"]
        duracao_total += resumo["duracao_acumulada_segundos"]
        duracao_mais_longa = max(
            duracao_mais_longa, resumo["duracao_evento_mais_longo_segundos"]
        )

    # Picos do período anterior, usados apenas para os deltas da página.
    anterior_eventos = 0
    anterior_duracao_total = 0.0
    anterior_duracao_mais_longa = 0.0

    for chave in COMPONENTES:
        antigos = [t for t in acumulador["picos"][chave] if t < limite_anterior]
        resumo = contar_eventos(antigos, PASSO)
        anterior_eventos += resumo["eventos"]
        anterior_duracao_total += resumo["duracao_acumulada_segundos"]
        anterior_duracao_mais_longa = max(
            anterior_duracao_mais_longa, resumo["duracao_evento_mais_longo_segundos"]
        )

    # Maior pico do período atual: a maior leitura isolada entre os componentes.
    # Só entra no KPI se realmente passou do limite de pico.
    maior_pico = None
    for chave, coluna in COMPONENTES.items():
        registro = maior_das_semanas(acumulador, semanas_datas, coluna)
        if registro is None or registro["valor"] <= LIMITE_PICO:
            continue

        if maior_pico is None or registro["valor"] > maior_pico["valor"]:
            momento = registro["timestamp"]
            maior_pico = {
                "componente": chave,
                "componente_nome": NOMES[chave],
                "valor": registro["valor"],
                "timestamp": momento,
                "texto": f"{NOMES[chave]} - {momento:%d/%m/%Y - %H:%M}",
            }

    # Maior pico do período anterior (só o valor, sem instante).
    maior_pico_anterior = None
    for chave, coluna in COMPONENTES.items():
        registro = maior_das_semanas(acumulador, semanas_anteriores, coluna)
        if registro is None:
            continue
        if maior_pico_anterior is None or registro["valor"] > maior_pico_anterior["valor"]:
            maior_pico_anterior = {
                "componente": chave,
                "componente_nome": NOMES[chave],
                "valor": registro["valor"],
            }

    # Maior anomalia: o maior resíduo entre as médias semanais dos componentes.
    maior_anomalia = None
    quantidade_anomalias = 0

    for chave, dados in componentes.items():
        quantidade_anomalias += dados["quantidade_anomalias"]
        for anomalia in dados["anomalias"]:
            if maior_anomalia is None or anomalia["residuo"] > maior_anomalia["residuo"]:
                maior_anomalia = {
                    "componente": chave,
                    "componente_nome": NOMES[chave],
                    "valor": anomalia["valor"],
                    "residuo": anomalia["residuo"],
                    "semana": anomalia["semana"],
                    "rotulo": anomalia["rotulo"],
                }

    # Componente mais instável: maior amplitude entre as médias semanais.
    mais_instavel = None
    for chave, dados in componentes.items():
        medias = [valor for valor in dados["medias"] if valor is not None]
        if len(medias) < 2:
            continue

        amplitude = round(max(medias) - min(medias), 1)
        media = sum(medias) / len(medias)
        variacao = round(amplitude / media * 100, 1) if media else None

        if mais_instavel is None or amplitude > mais_instavel["amplitude"]:
            mais_instavel = {
                "componente": chave,
                "componente_nome": NOMES[chave],
                "amplitude": amplitude,
                "variacao_percentual": variacao,
            }

    return {
        "maior_pico": maior_pico,
        "maior_anomalia": maior_anomalia,
        "quantidade_picos": total_eventos,
        "quantidade_anomalias": quantidade_anomalias,
        "duracao_acumulada_picos_segundos": float(duracao_total),
        "duracao_acumulada_picos_texto": formatar_duracao(duracao_total),
        "duracao_pico_mais_longo_segundos": float(duracao_mais_longa),
        "duracao_pico_mais_longo_texto": formatar_duracao(duracao_mais_longa),
        "componente_mais_instavel": mais_instavel,
        "por_componente": resumo_picos,
        # Métricas do período anterior: a página usa para calcular os deltas.
        # Só fazem sentido quando node["comparacao"]["disponivel"] é True.
        "periodo_anterior": {
            "quantidade_picos": anterior_eventos,
            "duracao_acumulada_picos_segundos": float(anterior_duracao_total),
            "duracao_pico_mais_longo_segundos": float(anterior_duracao_mais_longa),
            "duracao_pico_mais_longo_texto": formatar_duracao(anterior_duracao_mais_longa),
            "maior_pico": maior_pico_anterior,
        },
    }


def montar_comparacao(acumulador, semanas_datas, semanas_anteriores, inicio):
    """Compara o período atual com o anterior.

    Publica três coisas usadas pela página:
      - medias_atuais / medias_anteriores: média geral de cada componente;
      - series: média semanal do período anterior (a linha tracejada do gráfico);
      - variacao_percentual: diferença relativa entre as médias.

    Só existe resultado quando ANALISTA_CARREGAR_ANTERIOR=1 e há dados antigos.
    """
    if not CARREGAR_ANTERIOR:
        return {
            "disponivel": False,
            "motivo": "Período anterior não carregado (defina ANALISTA_CARREGAR_ANTERIOR=1).",
        }

    atual = {}
    anterior = {}
    variacao = {}
    series = {}

    # O período anterior termina onde o gráfico atual começa.
    fim_anterior = semanas_datas[0]

    for chave, coluna in COMPONENTES.items():
        atual[chave] = _media_das_semanas(acumulador, semanas_datas, coluna)
        anterior[chave] = _media_das_semanas(acumulador, semanas_anteriores, coluna)

        if atual[chave] is None or anterior[chave] in (None, 0):
            variacao[chave] = None
        else:
            variacao[chave] = round(
                (atual[chave] - anterior[chave]) / anterior[chave] * 100, 1
            )

        # Série semanal do período anterior, com a mesma regra de cobertura.
        valores = []
        for semana in semanas_anteriores:
            resumo = acumulador["semanas"].get(semana, {})
            soma = resumo.get(coluna + "_soma")
            quantidade = resumo.get(coluna + "_n", 0)
            necessario = minimo_amostras(
                semana, semana + pd.Timedelta(days=7), inicio, fim_anterior
            )
            if soma is None or pd.isna(soma) or quantidade < necessario:
                valores.append(None)
            else:
                valores.append(round(float(soma) / float(quantidade), 1))
        series[chave] = valores

    if all(valor is None for valor in anterior.values()):
        return {
            "disponivel": False,
            "motivo": "Sem amostras para o período anterior na Silver.",
        }

    return {
        "disponivel": True,
        "medias_atuais": atual,
        "medias_anteriores": anterior,
        "variacao_percentual": variacao,
        "rotulos": [rotulo_semana(dia) for dia in semanas_anteriores],
        "datas": [dia.date().isoformat() for dia in semanas_anteriores],
        "series": series,
    }


def _media_das_semanas(acumulador, semanas, coluna):
    """Média ponderada de uma coluna em uma lista de semanas."""
    soma = 0.0
    quantidade = 0

    for semana in semanas:
        resumo = acumulador["semanas"].get(semana, {})
        valor = resumo.get(coluna + "_soma")
        contagem = resumo.get(coluna + "_n", 0)
        if valor is None or pd.isna(valor) or contagem == 0:
            continue
        soma += float(valor)
        quantidade += int(contagem)

    if quantidade == 0:
        return None
    return round(soma / quantidade, 1)


def montar_node(acumulador, agora, inicio):
    """Monta o bloco de um node no JSON final."""
    # Semana atual: segunda-feira desta semana, no fuso de São Paulo.
    semana_atual = agora.normalize() - pd.Timedelta(days=agora.dayofweek)

    # Lista das últimas SEMANAS semanas, da mais antiga para a mais recente.
    # list comprehension: para cada i, recua (SEMANAS - 1 - i) semanas.
    semanas_datas = [
        semana_atual - pd.Timedelta(weeks=SEMANAS - 1 - i) for i in range(SEMANAS)
    ]

    # As semanas anteriores são as que terminam onde o gráfico começa.
    # range(SEMANAS) gera 0..SEMANAS-1; o offset SEMANAS-i vai de SEMANAS até 1,
    # produzindo a lista em ordem crescente (mais antiga primeiro).
    semanas_anteriores = [
        semanas_datas[0] - pd.Timedelta(weeks=SEMANAS - i) for i in range(SEMANAS)
    ]

    fim = acumulador["ultima_leitura"] or agora
    componentes = montar_semanas(acumulador, semanas_datas, inicio, fim)

    return {
        "hostname": None,  # preenchido por quem chama esta função.
        "telemetria_ate": fim,
        "amostras": acumulador["amostras"],
        "semanas": {
            "datas": [dia.date().isoformat() for dia in semanas_datas],
            "rotulos": [rotulo_semana(dia) for dia in semanas_datas],
        },
        "componentes": componentes,
        "kpis": montar_kpis(acumulador, componentes, semanas_datas, semanas_anteriores),
        "correlacao": montar_correlacao(acumulador),
        "comparacao": montar_comparacao(
            acumulador, semanas_datas, semanas_anteriores, inicio
        ),
    }


# ---------------------------------------------------------------------------
# 6. PUBLICAÇÃO E EXECUÇÃO
# ---------------------------------------------------------------------------
def gravar_json(destino, dados):
    """Publica o JSON na Gold: S3 em produção, arquivo local no modo de teste."""
    # limpar_json remove datas/números especiais; dumps transforma em texto.
    # ensure_ascii=False mantém acentos; allow_nan=False impede JSON inválido.
    texto = json.dumps(limpar_json(dados), ensure_ascii=False, allow_nan=False)

    if MODO_LOCAL:
        caminho = Path(GOLD_LOCAL_DIR) / destino
        caminho.parent.mkdir(parents=True, exist_ok=True)  # cria as subpastas.
        caminho.write_text(texto, encoding="utf-8")
        return str(caminho)

    # Em produção, destino é a chave dentro do bucket.
    s3, bucket, chave = destino
    s3.put_object(Bucket=bucket, Key=chave, ContentType="application/json",
                  Body=texto.encode("utf-8"))
    return f"s3://{bucket}/{chave}"


def executar_analista():
    """Fluxo principal da ETL."""
    # Validações de configuração antes de qualquer trabalho pesado.
    if not 0 < PASSO <= 300 or 3600 % PASSO:
        raise ValueError("COLETA_SEGUNDOS deve estar entre 1 e 300 e dividir 3600.")
    if not 0 < COBERTURA <= 1:
        raise ValueError("COBERTURA_MINIMA deve estar entre 0 e 1.")
    if SEMANAS <= 0:
        raise ValueError("ANALISTA_SEMANAS deve ser maior que zero.")

    # No modo local, o cluster pode ser informado ou ficar em 0.
    cluster_id = inteiro_env("ANALISTA_CLUSTER_ID", None)
    if cluster_id is None:
        cluster_id = inteiro_env("GESTOR_CLUSTER_ID")
    if not MODO_LOCAL and cluster_id <= 0:
        raise ValueError("ANALISTA_CLUSTER_ID (ou GESTOR_CLUSTER_ID) inválido.")

    hosts = buscar_hosts_analista(cluster_id)
    if hosts is not None and not hosts:
        raise ValueError("Cluster sem nodes cadastrados.")

    agora = pd.Timestamp.now(tz=FUSO)
    inicio = agora - pd.Timedelta(days=JANELA_DIAS)

    acumuladores = {}   # dicionário vazio: será preenchido durante a leitura.

    if MODO_LOCAL:
        lidos, ignorados = ler_silver_local(hosts, inicio, agora, acumuladores)
    else:
        bucket = os.environ["BUCKET_NAME"]
        regiao = os.getenv("AWS_REGION", os.getenv("REGION_NAME", "us-east-1"))
        s3 = boto3.client("s3", region_name=regiao)
        bucket_silver = os.getenv("SILVER_BUCKET_NAME", bucket)
        lidos, ignorados = ler_silver_s3(
            s3, bucket_silver, hosts, inicio, agora, acumuladores
        )

    if not acumuladores:
        raise ValueError("Nenhuma leitura válida na Silver para os nodes do cluster.")

    # Monta um bloco por node; sorted garante ordem estável no JSON.
    nodes = []
    for hostname in sorted(acumuladores):
        bloco = montar_node(acumuladores[hostname], agora, inicio)
        bloco["hostname"] = hostname
        nodes.append(bloco)

    avisos = [
        "IOPS não é coletado pela pipeline: a correlação usa uso do disco x IO wait.",
    ]
    if not CARREGAR_ANTERIOR:
        avisos.append(
            "Comparação com o período anterior desligada (ANALISTA_CARREGAR_ANTERIOR=0)."
        )

    painel = {
        "schema_version": 1,
        "metadados": {
            "cluster_id": cluster_id,
            "gerado_em": agora,
            "telemetria_ate": max(acumulador["ultima_leitura"] for acumulador in acumuladores.values()),
            "janela_dias": JANELA_DIAS,
            "semanas": SEMANAS,
            "passo_coleta_segundos": PASSO,
            "cobertura_minima": COBERTURA,
            "origem": SILVER,
            "modo_local": MODO_LOCAL,
            "demonstrativo": False,
            "linhas_lidas": lidos,
            "arquivos_ignorados": ignorados,
        },
        "avisos": avisos,
        "nodes": nodes,
    }

    if MODO_LOCAL:
        destino = f"cluster={cluster_id}/dashboard.json"
    else:
        destino = (s3, bucket, f"{GOLD_ANALISTA}/cluster={cluster_id}/dashboard.json")

    publicado = gravar_json(destino, painel)

    print(f"Gold publicada: {publicado} | {len(nodes)} nodes com telemetria")
    return {"destino": publicado, "nodes": len(nodes), "linhas": lidos}


def lambda_handler_analista(event, context):
    """Entrada usada quando a ETL roda como função Lambda."""
    if MODO_LOCAL:
        return executar_analista()

    # Verifica as variáveis obrigatórias antes de tentar acessar a AWS.
    if not os.getenv("BUCKET_NAME"):
        raise ValueError("Configure a variável BUCKET_NAME na Lambda.")

    if not os.getenv("ANALISTA_CLUSTER_ID") and not os.getenv("GESTOR_CLUSTER_ID"):
        raise ValueError("Configure ANALISTA_CLUSTER_ID (ou GESTOR_CLUSTER_ID).")

    if not os.getenv("DB_HOST") and not os.getenv("HOSTNAMES"):
        raise ValueError("Configure HOSTNAMES (ou as variáveis do MySQL).")

    return executar_analista()


# Este bloco só roda quando o arquivo é executado direto (python arquivo.py).
# Quando outro script importa este módulo, nada aqui é executado.
if __name__ == "__main__":
    if MODO_LOCAL:
        executar_analista()
    else:
        executar()
