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
def carregar_silver(s3, bucket, hosts, agora):
    partes = []
    inicio = agora - pd.Timedelta(days=45)
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
                        for c in COLUNAS:
                            df[c] = pd.to_numeric(df[c], errors="coerce") if c in df else np.nan
                            df[c] = df[c].where(np.isfinite(df[c]) & (df[c] >= 0))
                            if c.endswith("_PERCENT"):
                                df[c] = df[c].where(df[c] <= 100)
                        df["DISCO_USED"] = df["DISCO_USED"].where(
                            (df["DISCO_TOTAL"] > 0) & (df["DISCO_USED"] <= df["DISCO_TOTAL"]))
                        df["RAM_AVAILABLE"] = df["RAM_AVAILABLE"].where(
                            (df["RAM_TOTAL"] > 0) & (df["RAM_AVAILABLE"] <= df["RAM_TOTAL"]))
                        partes.append(df[["HOSTNAME", "TIMESTAMP"] + COLUNAS])
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


# 3. BASE HORÁRIA: mínimo de cobertura e um valor por intervalo de coleta.
def medias_horarias(g):
    colunas = ["CPU_PERCENT", "RAM_PERCENT"]
    amostras = g.set_index("TIMESTAMP")[colunas].resample(f"{PASSO}s").mean()
    media = amostras.resample("1h").mean()
    contagem = amostras.resample("1h").count()
    return media.where(contagem >= math.ceil(3600 / PASSO * COBERTURA))


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