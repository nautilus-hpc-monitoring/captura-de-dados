"""ETL do gestor: lê CSVs da Silver e publica o JSON da dashboard na Gold.

MAPA DO ARQUIVO
1. Configuração e nodes: variáveis de ambiente ou cadastro do banco.
2. Leitura da Silver: validação, filtro de datas e remoção de duplicatas.
3. Publicação JSON e reutilização das previsões do período.
4. Gráfico 1: ocupação de disco real e prevista.
5. Gráfico 2: mapa de calor de CPU, RAM, Disco e Swap.
6. Gráfico 3: consumo real e referência das duas semanas anteriores.
7. Gráfico 5: leituras de RAM e contadores de swap disponíveis.
8. KPI: médias dos componentes desde segunda-feira e campeões do operador.
9. Execução e entrada da Lambda.

A captura continua em local2bronze.py; não é feita por esta ETL.
Alertas, SLA e gráfico 4 são calculados pelo site usando o MySQL.
Gestor: python silver2gold.py ou silver2gold.lambda_handler na Lambda.
Analista: MODO_LOCAL=1 no teste local ou silver2gold.lambda_handler_analista na Lambda.
Operador: silver2gold.lambda_handler_operador na Lambda (HOSTNAMES ou OPERADOR_EMPRESA_ID).
"""
import io
import json
import math
import os
import sys
import tempfile
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import boto3
import numpy as np
import pandas as pd
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()
FUSO = "America/Sao_Paulo"
ORIGEM = os.getenv("GESTOR_FUSO_ORIGEM", FUSO)
SILVER = "02-silver/"
GOLD = os.getenv("CLUSTER_GOLD_PREFIX", "03-gold/gestor").rstrip("/")
PASSO = int(os.getenv("COLETA_SEGUNDOS", "10"))
COBERTURA = float(os.getenv("COBERTURA_MINIMA", "0.8"))
COLUNAS = ["CPU_PERCENT", "RAM_PERCENT", "RAM_TOTAL", "RAM_AVAILABLE",
           "SWAP_PERCENT", "SWAP_USED", "SWAP_FREE", "SWAP_IN", "SWAP_OUT",
           "DISCO_TOTAL", "DISCO_USED"]
OPERADOR_GOLD_PREFIX = os.getenv("OPERADOR_GOLD_PREFIX", "03-gold/operador").rstrip("/")
OPERADOR_CLUSTER_ID = int(os.getenv("OPERADOR_CLUSTER_ID", "0"))
OPERADOR_LIMITE_ATENCAO = float(os.getenv("OPERADOR_LIMITE_ATENCAO", "75"))
OPERADOR_LIMITE_CRITICO = float(os.getenv("OPERADOR_LIMITE_CRITICO", "90"))
OPERADOR_COLUNAS = list(COLUNAS) + ["DISCO_PERCENT"]

# ETL Billing (Silver -> Gold Billing)
# Modo fixo real: usa somente telemetria real da Silver.
BILLING_MODO = "real"
BILLING_DIAS_REAL = int(os.getenv("BILLING_DIAS_REAL", "90"))
BILLING_TARIFA_BASE = float(os.getenv("BILLING_TARIFA_BASE", "0.44"))
BILLING_TARIFA_AMPLITUDE = float(os.getenv("BILLING_TARIFA_AMPLITUDE", "0.18"))
BILLING_ADICIONAL_PICO = float(os.getenv("BILLING_ADICIONAL_PICO", "0.16"))
BILLING_GERAR_ALERTAS = True
BILLING_ORIGEM_ALERTA = "[billing-etl]"
BILLING_GOLD_PREFIX = os.getenv("BILLING_GOLD_PREFIX", "03-gold/billing").rstrip("/")
BILLING_PRECO_API = os.getenv("BILLING_PRECO_API", "https://api.energy-charts.info/price")
BILLING_PRECO_BZN = os.getenv("BILLING_PRECO_BZN", "DE-LU")
BILLING_EUR_BRL = float(os.getenv("BILLING_EUR_BRL", "6.00"))
BILLING_PRECO_FALLBACK_KWH = float(os.getenv("BILLING_PRECO_FALLBACK_KWH", "0.85"))
BILLING_PARAMS_URL = os.getenv("BILLING_PARAMS_URL", os.getenv("AUTH_URL", "http://localhost:3333/auth/").rstrip("/") + "/billing-parametros")
BILLING_AUTH_EMAIL = os.getenv("BILLING_AUTH_EMAIL", os.getenv("AUTH_EMAIL", "")).strip()
BILLING_AUTH_SENHA = os.getenv("BILLING_AUTH_SENHA", os.getenv("AUTH_PASSWORD", ""))
BILLING_AUTH_HOSTNAME = os.getenv("BILLING_AUTH_HOSTNAME", os.getenv("AUTH_HOSTNAME", "alpha-node01")).strip()
BILLING_ETL_API_KEY = os.getenv("ETL_API_KEY", "blablabla").strip()
BILLING_HOSTNAMES_DEMO = [
    "alpha-node01", "alpha-node02", "alpha-node03", "alpha-node04", "alpha-node05",
    "beta-node01", "beta-node02", "beta-node03", "beta-node04", "beta-node05",
    "gamma-node01", "gamma-node02", "gamma-node03", "gamma-node04", "gamma-node05",
    "delta-node01", "delta-node02", "delta-node03",
    "epsilon-node01", "epsilon-node02", "epsilon-node03"
]


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


def buscar_hosts_operador():
    if os.getenv("HOSTNAMES"):
        hosts = json.loads(os.environ["HOSTNAMES"])
        if not isinstance(hosts, list) or not hosts or not all(
                isinstance(host, str) and host.strip() for host in hosts):
            raise ValueError("HOSTNAMES deve ser uma lista JSON com nomes de nodes.")
        return {
            host.strip(): {
                "hostname": host.strip(),
                "id_cluster": OPERADOR_CLUSTER_ID,
                "id_ambiente_hpc": None,
                "id_node": None,
                "limites": {}
            }
            for host in hosts
        }

    empresa = os.getenv("OPERADOR_EMPRESA_ID")
    if not empresa:
        raise ValueError("Configure OPERADOR_EMPRESA_ID ou HOSTNAMES.")
    try:
        empresa_id = int(empresa)
    except ValueError as erro:
        raise ValueError("OPERADOR_EMPRESA_ID deve ser um número inteiro.") from erro

    import pymysql
    conexao = pymysql.connect(
        host=os.environ["DB_HOST"], port=int(os.getenv("DB_PORT", "3306")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_DATABASE"], charset="utf8mb4")
    try:
        with conexao.cursor() as cursor:
            cursor.execute(
                "SELECT h.id_ambiente_hpc, c.id_cluster, n.id_node, n.hostname, "
                "co.tipo, cn.limite_atencao, cn.limite_critico, cn.pico_min, cn.pico_max "
                "FROM ambiente_hpc h "
                "JOIN cluster c ON c.fk_ambiente_hpc = h.id_ambiente_hpc "
                "JOIN node n ON n.fk_cluster = c.id_cluster "
                "LEFT JOIN componente_node cn ON cn.fk_node = n.id_node "
                "LEFT JOIN componente co ON co.id_componente = cn.fk_componente "
                "WHERE h.fk_empresa = %s ORDER BY c.id_cluster, n.hostname",
                (empresa_id,))
            linhas = cursor.fetchall()
    finally:
        conexao.close()

    catalogo = {}
    for linha in linhas:
        id_hpc, id_cluster, id_node, hostname, tipo, atencao, critico, pico_min, pico_max = linha
        registro = catalogo.setdefault(hostname, {
            "hostname": hostname,
            "id_cluster": int(id_cluster),
            "id_ambiente_hpc": int(id_hpc),
            "id_node": int(id_node),
            "limites": {}
        })
        if tipo:
            registro["limites"][str(tipo).strip().upper()] = {
                "limite_atencao": atencao,
                "limite_critico": critico,
                "pico_min": pico_min,
                "pico_max": pico_max
            }
    return catalogo


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


def carregar_silver_billing(s3, bucket, agora, inicio=None, colunas=None):
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
                            continue
                        df = df.copy()
                        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"].map(instante), utc=True).dt.tz_convert(FUSO)
                        df = df[df["TIMESTAMP"].between(inicio, agora)].copy()
                        if df.empty:
                            continue
                        for c in colunas:
                            df[c] = pd.to_numeric(df[c], errors="coerce") if c in df else np.nan
                            df[c] = df[c].where(np.isfinite(df[c]) & (df[c] >= 0))
                            if c.endswith("_PERCENT"):
                                df[c] = df[c].where(df[c] <= 100)
                        colunas_saida = ["HOSTNAME", "TIMESTAMP"] + colunas
                        if "EMPRESA_ID" in df.columns:
                            colunas_saida.append("EMPRESA_ID")
                        partes.append(df[colunas_saida])
            finally:
                corpo.close()

    if not partes:
        raise ValueError("Nenhum dado Silver encontrado para billing.")

    df = pd.concat(partes, ignore_index=True)
    return (df.drop_duplicates(["HOSTNAME", "TIMESTAMP"])
            .sort_values(["HOSTNAME", "TIMESTAMP"]))


def carregar_silver_billing_local(agora, inicio=None, colunas=None):
    if inicio is None:
        inicio = agora - pd.Timedelta(days=45)
    if colunas is None:
        colunas = COLUNAS

    pasta = Path(SILVER_LOCAL_DIR)
    if not pasta.exists():
        raise ValueError(f"Pasta local não encontrada: {SILVER_LOCAL_DIR}")

    partes = []
    for caminho in sorted(pasta.rglob("*.csv")):
        try:
            df = pd.read_csv(caminho, sep=';', low_memory=False)
        except Exception:
            continue

        if not {"HOSTNAME", "TIMESTAMP"}.issubset(df.columns):
            continue

        df = df.copy()
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"].map(instante), utc=True).dt.tz_convert(FUSO)
        df = df[df["TIMESTAMP"].between(inicio, agora)].copy()
        if df.empty:
            continue

        for c in colunas:
            df[c] = pd.to_numeric(df[c], errors="coerce") if c in df else np.nan
            df[c] = df[c].where(np.isfinite(df[c]) & (df[c] >= 0))
            if c.endswith("_PERCENT"):
                df[c] = df[c].where(df[c] <= 100)

        colunas_saida = ["HOSTNAME", "TIMESTAMP"] + colunas
        if "EMPRESA_ID" in df.columns:
            colunas_saida.append("EMPRESA_ID")
        partes.append(df[colunas_saida])

    if not partes:
        raise ValueError("Nenhum dado Silver local encontrado para billing.")

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


def numero_limite(valor, padrao):
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return padrao
    return numero if math.isfinite(numero) and 0 <= numero <= 100 else padrao


def calor_com_componentes(g, hoje):
    dados = g.copy()
    if "DISCO_PERCENT" in dados.columns:
        disco_percentual = dados["DISCO_PERCENT"]
    else:
        disco_percentual = dados["DISCO_USED"] / dados["DISCO_TOTAL"].where(
            dados["DISCO_TOTAL"] > 0) * 100
    dados["Disco"] = disco_percentual
    dados["Swap"] = dados["SWAP_PERCENT"]
    detalhe = dados.set_index("TIMESTAMP")[["CPU_PERCENT", "RAM_PERCENT", "Disco", "Swap"]]
    detalhe = detalhe.resample(f"{PASSO}s").mean().resample("1h").mean()
    dias = pd.date_range(hoje - pd.Timedelta(days=7), periods=7, freq="D")
    linhas = []
    for dia in dias:
        faixa = detalhe.reindex(pd.date_range(dia, periods=24, freq="1h"))
        linhas.append([
            {"CPU": registro.CPU_PERCENT, "RAM": registro.RAM_PERCENT,
             "Disco": registro.Disco, "Swap": registro.Swap}
            for registro in faixa.itertuples()
        ])
    return {"dias": [dia.date().isoformat() for dia in dias], "leituras": linhas}


def montar_kpis_operador(resumo, limites=None):
    limites = limites or {}
    valores = (resumo or {}).get("valores") or {}
    kpis = {}
    for chave, coluna in (("CPU", "CPU"), ("RAM", "RAM"),
                          ("DISCO", "Disco"), ("SWAP", "Swap")):
        try:
            valor = float(valores.get(coluna))
        except (TypeError, ValueError):
            valor = float("nan")
        if not math.isfinite(valor) or not 0 <= valor <= 100:
            kpis[chave] = {"valor": None, "status": "sem_dados"}
            continue
        numero = round(valor, 1)
        limite = limites.get(coluna.upper()) or {}
        atencao = numero_limite(limite.get("limite_atencao"), OPERADOR_LIMITE_ATENCAO)
        critico = numero_limite(limite.get("limite_critico"), OPERADOR_LIMITE_CRITICO)
        if critico <= atencao:
            atencao = OPERADOR_LIMITE_ATENCAO
            critico = OPERADOR_LIMITE_CRITICO
        status = "critico" if numero >= critico else "atencao" if numero >= atencao else "normal"
        kpis[chave] = {"valor": numero, "status": status}
    return kpis


def montar_no_operador(s3, bucket, g, host, cluster_id, limites, agora, hoje, semana):
    corte = min(agora, g["TIMESTAMP"].max())
    horas = medias_horarias(g)
    caminho_host = quote(host, safe="")
    chave_disco = f"{GOLD}/cluster={cluster_id}/previsoes-v2/disco/{semana.date()}/{caminho_host}.json"
    chave_consumo = f"{GOLD}/cluster={cluster_id}/previsoes-v2/consumo/{hoje.date()}/{caminho_host}.json"
    prev_disco = congelar(s3, bucket, chave_disco, lambda: disco(g, semana, corte))
    prev_consumo = congelar(s3, bucket, chave_consumo, lambda: consumo(horas, hoje, corte))
    resumo = medias_semana(g, semana, corte)
    return {
        "hostname": host,
        "cluster_id": cluster_id,
        "disco": disco(g, semana, corte, prev_disco) if prev_disco else None,
        "calor": calor_com_componentes(g, hoje),
        "consumo": consumo(horas, hoje, corte, prev_consumo),
        "memoria": memoria(g, corte),
        "medias_semana": resumo,
        "picos": None,
        "kpis": montar_kpis_operador(resumo, limites)
    }


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
    bucket_silver = bucket
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
            continue
        nodes.append(montar_no_operador(s3, bucket, g, host, cluster_id, {}, agora, hoje, semana))
    painel = {"schema_version": 2,
              "metadados": {"cluster_id": cluster_id, "gerado_em": agora,
                            "gerado_em_utc": agora.tz_convert("UTC").isoformat(),
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
    bucket_silver = bucket

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
            serie = grupo[coluna].dropna()
            if serie.empty:
                continue

            resumo[coluna + "_soma"] = resumo.get(coluna + "_soma", 0.0) + float(serie.sum())
            resumo[coluna + "_n"] = resumo.get(coluna + "_n", 0) + len(serie)

            # Máximo da semana e o instante em que ele aconteceu.
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


def acumular_telemetria(acumulador, grupo):
    if grupo.empty:
        return

    acumular_histograma(acumulador, grupo)
    acumular_correlacao(acumulador, grupo)
    acumulador["amostras"] += int(len(grupo))

    maior = grupo["TIMESTAMP"].max()
    if acumulador["ultima_leitura"] is None or maior > acumulador["ultima_leitura"]:
        acumulador["ultima_leitura"] = maior


def processar_chunk(df, hosts, inicio, fim, acumuladores, *, inicio_atual=None, recortes=None):
    """Valida as leituras uma vez e agrega os recortes com as datas originais."""
    df = preparar_chunk(df, hosts, inicio, fim)
    if df.empty:
        return 0

    recortes = recortes or {}
    for host, grupo in df.groupby("HOSTNAME"):
        acumulador = garantir_host(acumuladores, host)
        acumular_semanas(acumulador, grupo)
        acumular_picos(acumulador, grupo)

        atuais = grupo if inicio_atual is None else grupo[grupo["TIMESTAMP"] >= inicio_atual]
        acumular_telemetria(acumulador, atuais)

        for inicio_recorte, por_host in recortes.values():
            leituras = grupo[grupo["TIMESTAMP"] >= inicio_recorte]
            if leituras.empty:
                continue
            recorte = garantir_host(por_host, host)
            acumular_semanas(recorte, leituras)
            acumular_telemetria(recorte, leituras)

    return int(len(df))


def ler_silver_s3(s3, bucket, hosts, inicio, fim, acumuladores, *, inicio_atual=None, recortes=None):
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
                        lidos += processar_chunk(
                            df, hosts, inicio, fim, acumuladores,
                            inicio_atual=inicio_atual, recortes=recortes
                        )
            finally:
                # close() libera a conexão HTTP, mesmo se o CSV estiver corrompido.
                corpo.close()

    print(f"Silver (S3): {lidos} linhas válidas | {ignorados} arquivos fora da janela")
    return lidos, ignorados


def ler_silver_local(hosts, inicio, fim, acumuladores, *, inicio_atual=None, recortes=None):
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
            lidos += processar_chunk(
                df, hosts, inicio, fim, acumuladores,
                inicio_atual=inicio_atual, recortes=recortes
            )

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


def regressao_linear(indices, valores):
    """Ajusta uma reta aos valores e devolve inclinação, intercepto e resíduos.

    Os índices são as posições das semanas no calendário da série.
    Reta: y = intercepto + inclinacao * x
    """
    quantidade = len(valores)
    if quantidade < 2:
        return None  # sem dois pontos não existe reta.

    xs = np.asarray(indices, dtype=float)
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


def media_semanal(soma, quantidade):
    if quantidade <= 0 or pd.isna(soma):
        return None
    return round(float(soma) / quantidade, 1)


def montar_semanas(acumulador, semanas_datas):
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

            media = media_semanal(soma, quantidade)

            medias.append(media)
            amostras.append(int(quantidade))
            if media is not None:
                validos.append((posicao, media))   # usado na regressão.

        # Regressão e anomalias das médias semanais.
        indices = [posicao for posicao, _ in validos]
        valores = [valor for _, valor in validos]
        ajuste = regressao_linear(indices, valores)

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
            "media_geral": _media_das_semanas(acumulador, semanas_datas, coluna),
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
                    round(min(100.0, max(0.0, ajuste["intercepto"] + ajuste["inclinacao"] * posicao)), 1)
                    if ajuste is not None and indices[0] <= posicao <= indices[-1] else None
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


def montar_kpis(acumulador, componentes, semanas_datas, semanas_anteriores, *, inicio, picos=None):
    """Resume os eventos do node nos cartões de KPI da página.

    Calcula o período atual e também o anterior, porque a página mostra a
    diferença entre os dois (+2 picos, +30 min, +9%).
    """
    # Os recortes usam os mesmos instantes de pico acumulados na leitura.
    picos = acumulador["picos"] if picos is None else picos

    # Picos do período atual: calculamos por componente e somamos.
    total_eventos = 0
    duracao_total = 0.0
    duracao_mais_longa = 0.0
    resumo_picos = {}

    for chave in COMPONENTES:
        # Só os instantes dentro do período exibido no gráfico.
        atuais = [t for t in picos[chave] if t >= inicio]
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

    if semanas_anteriores:
        inicio_anterior = semanas_anteriores[0]
        for chave in COMPONENTES:
            antigos = [t for t in picos[chave] if inicio_anterior <= t < inicio]
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
        if registro is None or registro["valor"] <= LIMITE_PICO:
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


def montar_comparacao(acumulador, semanas_datas, semanas_anteriores):
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

    for chave, coluna in COMPONENTES.items():
        atual[chave] = _media_das_semanas(acumulador, semanas_datas, coluna)
        anterior[chave] = _media_das_semanas(acumulador, semanas_anteriores, coluna)

        if atual[chave] is None or anterior[chave] in (None, 0):
            variacao[chave] = None
        else:
            variacao[chave] = round(
                (atual[chave] - anterior[chave]) / anterior[chave] * 100, 1
            )

        # Série semanal do período anterior com as leituras disponíveis.
        valores = []
        for semana in semanas_anteriores:
            resumo = acumulador["semanas"].get(semana, {})
            soma = resumo.get(coluna + "_soma")
            quantidade = resumo.get(coluna + "_n", 0)
            valores.append(media_semanal(soma, quantidade))
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


def inicio_da_semana(data):
    return data.normalize() - pd.Timedelta(days=data.dayofweek)


def montar_node(acumulador, agora, inicio, *, semanas_anteriores=(), picos=None):
    """Monta a série e as estatísticas do mesmo intervalo de coleta."""
    semanas_datas = pd.date_range(inicio_da_semana(inicio), inicio_da_semana(agora), freq="7D").tolist()
    componentes = montar_semanas(acumulador, semanas_datas)

    return {
        "inicio": inicio,
        "fim": agora,
        "telemetria_ate": acumulador["ultima_leitura"],
        "amostras": acumulador["amostras"],
        "semanas": {
            "datas": [dia.date().isoformat() for dia in semanas_datas],
            "rotulos": [rotulo_semana(dia) for dia in semanas_datas],
        },
        "componentes": componentes,
        "kpis": montar_kpis(
            acumulador, componentes, semanas_datas, semanas_anteriores, inicio=inicio, picos=picos
        ),
        "correlacao": montar_correlacao(acumulador),
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
    inicio_atual = inicio_da_semana(agora) - pd.Timedelta(weeks=SEMANAS - 1)
    semanas_anteriores = []
    if CARREGAR_ANTERIOR:
        fim_anterior = inicio_atual - pd.Timedelta(weeks=1)
        semanas_anteriores = pd.date_range(end=fim_anterior, periods=SEMANAS, freq="7D").tolist()

    recortes = {
        "1": (agora - pd.Timedelta(days=30), {}),
        "3": (agora - pd.DateOffset(months=3), {}),
    }
    inicio_historico = semanas_anteriores[0] if semanas_anteriores else inicio_atual
    inicio = min(inicio_historico, *(corte for corte, _ in recortes.values()))
    acumuladores = {}

    if MODO_LOCAL:
        lidos, ignorados = ler_silver_local(
            hosts, inicio, agora, acumuladores, inicio_atual=inicio_atual, recortes=recortes
        )
    else:
        bucket = os.environ["BUCKET_NAME"]
        regiao = os.getenv("AWS_REGION", os.getenv("REGION_NAME", "us-east-1"))
        s3 = boto3.client("s3", region_name=regiao)
        bucket_silver = bucket
        lidos, ignorados = ler_silver_s3(
            s3, bucket_silver, hosts, inicio, agora, acumuladores,
            inicio_atual=inicio_atual, recortes=recortes
        )

    if not acumuladores:
        raise ValueError("Nenhuma leitura válida na Silver para os nodes do cluster.")

    # Monta um bloco por node; sorted garante ordem estável no JSON.
    nodes = []
    for hostname, acumulador in sorted(acumuladores.items()):
        bloco = montar_node(acumulador, agora, inicio_atual, semanas_anteriores=semanas_anteriores)
        bloco["hostname"] = hostname
        semanas_datas = pd.date_range(inicio_atual, periods=SEMANAS, freq="7D").tolist()
        bloco["comparacao"] = montar_comparacao(acumulador, semanas_datas, semanas_anteriores)
        bloco["periodos"] = {}
        for periodo, (corte, por_host) in recortes.items():
            resumo = por_host.get(hostname)
            if resumo is None:
                resumo = novo_acumulador()
            bloco["periodos"][periodo] = montar_node(resumo, agora, corte, picos=acumulador["picos"])
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
            "telemetria_ate": max((node["telemetria_ate"] for node in nodes if node["telemetria_ate"] is not None), default=None),
            "janela_dias": math.ceil((agora - inicio) / pd.Timedelta(days=1)),
            "semanas": SEMANAS,
            "passo_coleta_segundos": PASSO,
            "cobertura_minima": 0.0,
            "limite_pico": LIMITE_PICO,
            "limite_residuo": LIMITE_RESIDUO,
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


def executar_operador():
    if not 0 < PASSO <= 300 or 3600 % PASSO or not 0 < COBERTURA <= 1:
        raise ValueError("COLETA_SEGUNDOS deve dividir 3600; cobertura deve estar entre 0 e 1.")
    if not 0 <= OPERADOR_LIMITE_ATENCAO < OPERADOR_LIMITE_CRITICO <= 100:
        raise ValueError("OPERADOR_LIMITE_ATENCAO deve ser menor que OPERADOR_LIMITE_CRITICO.")
    if not os.getenv("DISCO_VOLUME"):
        raise ValueError("Configure a variável DISCO_VOLUME.")

    catalogo = buscar_hosts_operador()
    if not catalogo:
        raise ValueError("Nenhum node cadastrado para a empresa informada.")

    bucket = os.environ["BUCKET_NAME"]
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", os.getenv("REGION_NAME", "us-east-1")))
    agora = pd.Timestamp.now(tz=FUSO)
    bucket_silver = bucket
    hosts = sorted(catalogo)
    df = carregar_silver(s3, bucket_silver, hosts, agora,
                         inicio=agora - pd.Timedelta(days=45), colunas=OPERADOR_COLUNAS)
    if df.empty:
        raise ValueError("Silver sem leituras válidas no período; publicação cancelada.")

    hoje = agora.normalize()
    semana = hoje - pd.Timedelta(days=hoje.dayofweek)
    todos = []
    por_cluster = {}
    for host in hosts:
        informacao = catalogo[host]
        g = df[df["HOSTNAME"] == host].copy()
        if g.empty:
            continue
        no = montar_no_operador(s3, bucket, g, host, informacao["id_cluster"],
                                informacao["limites"], agora, hoje, semana)
        todos.append(no)
        por_cluster.setdefault(informacao["id_cluster"], []).append(no)

    if not todos:
        raise ValueError("Nenhum node com telemetria válida na Silver.")

    empresa = os.getenv("OPERADOR_EMPRESA_ID")
    metadados = {
        "gerado_em": agora,
        "gerado_em_utc": agora.tz_convert("UTC").isoformat(),
        "telemetria_ate": df["TIMESTAMP"].max(),
        "demonstrativo": False,
        "origem": SILVER,
        "empresa_id": int(empresa) if empresa and empresa.isdigit() else None,
    }

    chaves = []
    for cluster_id, nodes in sorted(por_cluster.items()):
        if cluster_id <= 0:
            continue
        painel = {"schema_version": 2,
                  "metadados": {**metadados, "cluster_id": cluster_id},
                  "nodes": nodes}
        chave = f"{GOLD}/cluster={cluster_id}/dashboard.json"
        gravar(s3, bucket, chave, painel)
        chaves.append(chave)

    consolidado = {
        "schema_version": 2,
        "metadados": {**metadados, "cluster_id": 0, "clusters": sorted(por_cluster)},
        "nodes": todos,
        "avisos": []
    }
    chave_unica = f"{OPERADOR_GOLD_PREFIX}/dashboard.json"
    gravar(s3, bucket, chave_unica, consolidado)
    chaves.append(chave_unica)

    print(f"Gold publicada: s3://{bucket}/{chave_unica} | {len(todos)} nodes em {len(chaves)} chaves")
    return {"chaves": [f"s3://{bucket}/{chave}" for chave in chaves],
            "nodes": len(todos), "clusters": len(por_cluster)}


def lambda_handler_operador(event, context):
    for variavel in ("BUCKET_NAME", "DISCO_VOLUME"):
        if not os.getenv(variavel):
            raise ValueError(f"Configure a variável {variavel} na Lambda.")
    if not os.getenv("HOSTNAMES") and not os.getenv("OPERADOR_EMPRESA_ID"):
        raise ValueError("Configure HOSTNAMES ou OPERADOR_EMPRESA_ID na Lambda.")
    if not os.getenv("HOSTNAMES"):
        for variavel in ("DB_HOST", "DB_USER", "DB_PASSWORD", "DB_DATABASE"):
            if not os.getenv(variavel):
                raise ValueError(f"Configure a variável {variavel} na Lambda.")
    return executar_operador()


# ---------------------------------------------------------------------------
# ETL BILLING: lê Silver e publica JSON na Gold (S3/local), usando parâmetros
# de billing vindos da API do site quando disponíveis.
# ---------------------------------------------------------------------------
def ano_referencia_setembro(agora):
    inicio_ano = pd.Timestamp(year=agora.year, month=9, day=1, tz=FUSO)
    return agora.year if agora >= inicio_ano else agora.year - 1


def janela_billing(modo, agora):
    if modo in ("sintetico", "hibrido"):
        ano = ano_referencia_setembro(agora)
        inicio = pd.Timestamp(year=ano, month=9, day=1, hour=0, minute=0, second=0, tz=FUSO)
        fim = agora.floor("1h")
        return inicio, fim
    return agora - pd.Timedelta(days=BILLING_DIAS_REAL), agora


def limite_demo_projecao(agora):
    # Mantém novembro incompleto para exibir a linha de projeção na apresentação.
    limite = pd.Timestamp(year=agora.year, month=11, day=15, hour=23, minute=0, second=0, tz=FUSO)
    return min(agora.floor("1h"), limite)


def mesclar_registros_billing(registros_sinteticos, registros_reais):
    # Chave por node+hora. Dados reais sempre sobrescrevem o sintético.
    mapa = {}

    for registro in registros_sinteticos:
        instante = pd.Timestamp(registro["timestamp"]).floor("h")
        chave = (int(registro["id_node"]), instante.isoformat())
        mapa[chave] = registro

    for registro in registros_reais:
        instante = pd.Timestamp(registro["timestamp"]).floor("h")
        chave = (int(registro["id_node"]), instante.isoformat())
        mapa[chave] = registro

    registros = list(mapa.values())
    registros.sort(key=lambda item: (int(item["id_empresa"]), int(item["id_node"]), pd.Timestamp(item["timestamp"])))
    return registros


def expandir_df_para_hosts_demo(df, hosts_demo):
    if df.empty:
        return df

    base_hosts = sorted(df["HOSTNAME"].dropna().astype(str).unique().tolist())
    if not base_hosts:
        return df

    partes = []
    for idx, host_destino in enumerate(hosts_demo):
        host_origem = base_hosts[idx % len(base_hosts)]
        base = df[df["HOSTNAME"].astype(str) == host_origem].copy()
        if base.empty:
            continue

        fator = 0.78 + ((idx % 9) * 0.11)
        offset = -8 + ((idx % 7) * 2.2)
        ajuste_hora = np.sin((base["TIMESTAMP"].dt.hour.astype(float) + idx) * (2 * np.pi / 24.0)) * (3 + (idx % 4))

        cpu = pd.to_numeric(base["CPU_PERCENT"], errors="coerce").fillna(0)
        base["CPU_PERCENT"] = (cpu * fator + offset + ajuste_hora).clip(3, 100).round(2)
        base["HOSTNAME"] = host_destino
        partes.append(base)

    if not partes:
        return df

    expandido = pd.concat(partes, ignore_index=True)
    return expandido.sort_values(["HOSTNAME", "TIMESTAMP"]).reset_index(drop=True)


def catalogo_billing_de_dataframe(df):
    empresa_padrao = int(os.getenv("BILLING_EMPRESA_PADRAO", "1"))
    hosts = BILLING_HOSTNAMES_DEMO
    empresas_por_host = {}
    if "EMPRESA_ID" in df.columns:
        validos = df[["HOSTNAME", "EMPRESA_ID"]].dropna().copy()
        if not validos.empty:
            validos["HOSTNAME"] = validos["HOSTNAME"].astype(str)
            validos["EMPRESA_ID"] = pd.to_numeric(validos["EMPRESA_ID"], errors="coerce")
            validos = validos.dropna()
            if not validos.empty:
                validos["EMPRESA_ID"] = validos["EMPRESA_ID"].astype(int)
                empresas_por_host = (
                    validos.groupby("HOSTNAME")["EMPRESA_ID"]
                    .agg(lambda serie: int(serie.mode().iloc[0]) if not serie.mode().empty else int(serie.iloc[0]))
                    .to_dict()
                )

    catalogo = {}
    for indice, hostname in enumerate(sorted(set([str(h).strip() for h in hosts if str(h).strip()])), start=1):
        catalogo[hostname] = {
            "id_node": indice,
            "hostname": hostname,
            "id_cluster": 0,
            "id_ambiente_hpc": None,
            "id_empresa": int(empresas_por_host.get(hostname, empresa_padrao)),
        }
    return catalogo


def parametros_billing_padrao(catalogo):
    parametros = {}
    for info in catalogo.values():
        id_node = int(info["id_node"])
        parametros[id_node] = {
            "potencia_idle_kw": round(0.2200 + (id_node % 5) * 0.0700 + ((id_node - 1) // 5) * 0.0300, 4),
            "potencia_max_kw": round(0.6500 + (id_node % 5) * 0.1500 + ((id_node - 1) // 5) * 0.0800, 4),
            "fator_gamma": round(1.080 + (id_node % 4) * 0.080, 3),
            "tarifa_alpha": round(0.8800 + (id_node % 6) * 0.0700, 4),
            "tarifa_beta": round(((id_node % 3) - 1) * 0.004500, 6),
            "encargos_kwh": round(0.090000 + (id_node % 4) * 0.018000, 6),
            "tarifa_fallback_kwh": round(0.780000 + (id_node % 5) * 0.040000, 6),
        }
    return parametros


def carregar_parametros_billing_api(catalogo):
    if not catalogo:
        return {"parametros": {}, "empresa_id": None}

    if not BILLING_AUTH_EMAIL or not BILLING_AUTH_SENHA:
        print("Billing: credenciais de API ausentes; usando parâmetros locais padrão.")
        return {"parametros": {}, "empresa_id": None}

    if not BILLING_ETL_API_KEY:
        print("Billing: ETL_API_KEY ausente; usando parâmetros locais padrão.")
        return {"parametros": {}, "empresa_id": None}

    hostnames = sorted({str(info.get("hostname", "")).strip() for info in catalogo.values() if str(info.get("hostname", "")).strip()})
    if not hostnames:
        return {"parametros": {}, "empresa_id": None}

    payload = {
        "email": BILLING_AUTH_EMAIL,
        "senha": BILLING_AUTH_SENHA,
        "hostname": BILLING_AUTH_HOSTNAME,
        "hostnames": hostnames,
    }
    headers = {"x-etl-api-key": BILLING_ETL_API_KEY}

    try:
        resposta = requests.post(BILLING_PARAMS_URL, json=payload, headers=headers, timeout=(5, 20))
    except requests.RequestException as erro:
        print(f"Billing: falha ao consultar parâmetros no site ({type(erro).__name__}); usando parâmetros locais padrão.")
        return {"parametros": {}, "empresa_id": None}

    if resposta.status_code != 200:
        print(f"Billing: endpoint de parâmetros retornou HTTP {resposta.status_code}; usando parâmetros locais padrão.")
        return {"parametros": {}, "empresa_id": None}

    try:
        dados = resposta.json()
    except ValueError:
        print("Billing: resposta inválida do endpoint de parâmetros; usando parâmetros locais padrão.")
        return {"parametros": {}, "empresa_id": None}

    empresa_id_api = None
    try:
        empresa_id_lida = int(dados.get("empresa_id"))
        if empresa_id_lida > 0:
            empresa_id_api = empresa_id_lida
    except (TypeError, ValueError):
        empresa_id_api = None

    linhas = dados.get("parametros")
    if not isinstance(linhas, list):
        print("Billing: endpoint sem lista de parâmetros; usando parâmetros locais padrão.")
        return {"parametros": {}, "empresa_id": empresa_id_api}

    parametros_por_host = {}
    for linha in linhas:
        hostname = str(linha.get("hostname", "")).strip()
        if not hostname:
            continue
        try:
            parametro = {
                "potencia_idle_kw": float(linha["potencia_idle_kw"]),
                "potencia_max_kw": float(linha["potencia_max_kw"]),
                "fator_gamma": float(linha["fator_gamma"]),
                "tarifa_alpha": float(linha["tarifa_alpha"]),
                "tarifa_beta": float(linha["tarifa_beta"]),
                "encargos_kwh": float(linha["encargos_kwh"]),
                "tarifa_fallback_kwh": float(linha["tarifa_fallback_kwh"]),
            }
        except (KeyError, TypeError, ValueError):
            continue

        if parametro["potencia_idle_kw"] < 0:
            continue
        if parametro["potencia_max_kw"] < parametro["potencia_idle_kw"]:
            continue
        if parametro["fator_gamma"] <= 0:
            continue
        if parametro["tarifa_alpha"] < 0 or parametro["encargos_kwh"] < 0 or parametro["tarifa_fallback_kwh"] < 0:
            continue

        parametros_por_host[hostname] = parametro

    if not parametros_por_host:
        print("Billing: nenhum parâmetro válido retornado pelo endpoint; usando parâmetros locais padrão.")
        return {"parametros": {}, "empresa_id": empresa_id_api}

    parametros = {}
    for info in catalogo.values():
        hostname = str(info.get("hostname", "")).strip()
        id_node = int(info.get("id_node", 0))
        parametro = parametros_por_host.get(hostname)
        if id_node > 0 and parametro:
            parametros[id_node] = parametro

    if parametros:
        print(f"Billing: {len(parametros)} nodes com parâmetros vindos do site.")
    return {"parametros": parametros, "empresa_id": empresa_id_api}


def carregar_tarifas_energia_externa(inicio, fim):
    inicio_data = pd.Timestamp(inicio).tz_convert("UTC").date().isoformat()
    fim_data = pd.Timestamp(fim).tz_convert("UTC").date().isoformat()

    resposta = requests.get(
        BILLING_PRECO_API,
        params={
            "bzn": BILLING_PRECO_BZN,
            "start": inicio_data,
            "end": fim_data,
        },
        timeout=(5, 20),
    )
    resposta.raise_for_status()
    dados = resposta.json()

    instantes = dados.get("unix_seconds") or []
    precos = dados.get("price") or []
    if not instantes or not precos:
        raise ValueError("API de energia sem dados de preço.")

    mapa = {}
    for unix, preco in zip(instantes, precos):
        if preco is None:
            continue
        try:
            preco_eur_mwh = float(preco)
        except (TypeError, ValueError):
            continue

        preco_eur_mwh = max(0.0, preco_eur_mwh)
        preco_brl_kwh = (preco_eur_mwh / 1000.0) * BILLING_EUR_BRL

        instante = pd.Timestamp(unix, unit="s", tz="UTC").tz_convert(FUSO).floor("h")
        mapa[instante.isoformat()] = round(preco_brl_kwh, 6)

    if not mapa:
        raise ValueError("API de energia retornou preços inválidos.")
    return mapa


def montar_mapa_tarifas(inicio, fim):
    try:
        return carregar_tarifas_energia_externa(inicio, fim)
    except Exception as erro:
        print(f"Billing: API externa de energia indisponível ({type(erro).__name__}); usando fallback de tarifa.")
        return {}


def tarifa_base_hora(instante_hora, mapa_tarifas):
    chave = pd.Timestamp(instante_hora).floor("h").isoformat()
    tarifa = mapa_tarifas.get(chave)
    if tarifa is not None and tarifa > 0:
        return float(tarifa)
    return BILLING_PRECO_FALLBACK_KWH


def tarifa_horaria(parametros, instante_hora, mapa_tarifas):
    tarifa_mercado = tarifa_base_hora(instante_hora, mapa_tarifas)
    tarifa = (
        parametros["tarifa_alpha"] * tarifa_mercado
        + parametros["tarifa_beta"]
        + parametros["encargos_kwh"]
    )
    piso = parametros["tarifa_fallback_kwh"] * 0.20
    return max(piso, tarifa)


def consumo_e_custo(cpu_percent, parametros, instante_hora, mapa_tarifas):
    cpu = max(0.0, min(100.0, float(cpu_percent)))
    consumo = parametros["potencia_idle_kw"] + (
        parametros["potencia_max_kw"] - parametros["potencia_idle_kw"]
    ) * ((cpu / 100.0) ** parametros["fator_gamma"])
    tarifa = tarifa_horaria(parametros, instante_hora, mapa_tarifas)
    custo = consumo * tarifa
    return round(consumo, 4), round(tarifa, 6), round(custo, 2)


def gerar_cpu_sintetica(hostname, id_node, instante_hora):
    ruido_base = zlib.crc32(f"{hostname}-{instante_hora.strftime('%Y%m%d%H')}".encode("utf-8")) % 1000
    ruido = (ruido_base / 1000.0 - 0.5)
    valor = (
        35
        + (12 + (id_node % 5) * 4) * math.sin(2 * math.pi * (instante_hora.hour / 24.0))
        + (8 + (id_node % 4) * 3) * math.sin(2 * math.pi * (instante_hora.weekday() / 7.0) + (id_node / 5.0))
        + ruido * (10 + (id_node % 6) * 4)
        + (16 if id_node % 5 == 0 else 10 if id_node % 5 == 4 else -6 if id_node % 5 == 1 else 0)
    )
    return round(max(3.0, min(100.0, valor)), 2)


def montar_registros_billing_reais(df, catalogo, parametros, mapa_tarifas):
    registros = []
    for hostname, grupo in df.groupby("HOSTNAME"):
        info = catalogo.get(str(hostname))
        if not info:
            continue
        id_node = info["id_node"]
        parametro = parametros.get(id_node)
        if not parametro:
            continue
        serie = (grupo.set_index("TIMESTAMP")["CPU_PERCENT"]
                 .resample("1h")
                 .mean()
                 .dropna())
        for instante_hora, cpu in serie.items():
            consumo, tarifa, custo = consumo_e_custo(cpu, parametro, instante_hora, mapa_tarifas)
            registros.append({
                "timestamp": instante_hora,
                "media_cpu_pct": round(float(cpu), 2),
                "consumo_kwh": consumo,
                "tarifa_kwh": tarifa,
                "custo_estimado": custo,
                "id_node": id_node,
                "hostname": info["hostname"],
                "id_empresa": info["id_empresa"],
            })
    return registros


def montar_registros_billing_sinteticos(catalogo, parametros, inicio, fim, mapa_tarifas):
    registros = []
    horas = pd.date_range(inicio, fim, freq="1h")
    for host, info in sorted(catalogo.items()):
        id_node = info["id_node"]
        parametro = parametros.get(id_node)
        if not parametro:
            continue
        for instante_hora in horas:
            cpu = gerar_cpu_sintetica(host, id_node, instante_hora)
            consumo, tarifa, custo = consumo_e_custo(cpu, parametro, instante_hora, mapa_tarifas)
            registros.append({
                "timestamp": instante_hora,
                "media_cpu_pct": cpu,
                "consumo_kwh": consumo,
                "tarifa_kwh": tarifa,
                "custo_estimado": custo,
                "id_node": id_node,
                "hostname": info["hostname"],
                "id_empresa": info["id_empresa"],
            })
    return registros


def sintetizar_alertas_billing(df_horas):
    if df_horas.empty or not BILLING_GERAR_ALERTAS:
        base = df_horas.copy()
        base["aviso_hora"] = 0
        base["critico_hora"] = 0
        return base

    estat = df_horas.groupby("id_node", as_index=False).agg(
        media_custo=("custo_estimado", "mean"),
        desvio_custo=("custo_estimado", "std"),
        media_consumo=("consumo_kwh", "mean"),
        desvio_consumo=("consumo_kwh", "std"),
        media_tarifa=("tarifa_kwh", "mean"),
        desvio_tarifa=("tarifa_kwh", "std"),
    )
    for coluna in ("desvio_custo", "desvio_consumo", "desvio_tarifa"):
        estat[coluna] = estat[coluna].fillna(0.0).clip(lower=0.02)

    df = df_horas.merge(estat, on="id_node", how="left")
    df["pressao"] = ((df["consumo_kwh"] - df["media_consumo"]) / df["desvio_consumo"]) + (
        (df["tarifa_kwh"] - df["media_tarifa"]) / df["desvio_tarifa"]
    )

    critico_cond = (
        (df["pressao"] >= 2.4)
        | (df["custo_estimado"] >= (df["media_custo"] + df["desvio_custo"] * 1.20))
        | (df["media_cpu_pct"] >= 90)
    )
    aviso_cond = (
        critico_cond
        | (df["pressao"] >= 1.3)
        | (df["custo_estimado"] >= (df["media_custo"] + df["desvio_custo"] * 0.70))
        | (df["media_cpu_pct"] >= 75)
    )

    chave = df.apply(lambda r: f"{int(r.id_node)}-{r.timestamp.strftime('%Y%m%d%H')}".encode("utf-8"), axis=1)
    modulo = chave.map(lambda texto: zlib.crc32(texto) % 12)

    df["critico_hora"] = ((critico_cond & (modulo <= 2))).astype(int)
    df["aviso_hora"] = ((aviso_cond & (modulo <= 6))).astype(int)
    df.loc[df["critico_hora"] == 1, "aviso_hora"] = 0
    return df


def mes_ref(data):
    return f"{int(data.year):04d}-{int(data.month):02d}"


def calcular_metricas_periodo(payload_mes):
    custos = payload_mes["historicoDiario"]["custos"]
    consumo = payload_mes["periodoMensal"]["consumoKwh"]
    impacto = payload_mes["decomposicaoDiaria"]["impactoTarifa"]
    return {
        "acumuladoMes": round(float(sum(custos)), 2),
        "projecaoFechamento": round(float(sum(custos)), 2),
        "consumoKwh": round(float(consumo), 4),
        "impactoTarifa": round(float(sum(impacto)), 2),
    }


def gravar_billing_saida(s3, bucket, chave, dados, estado_saida):
    if estado_saida["modo"] == "local":
        caminho = Path(GOLD_LOCAL_DIR) / chave
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_text(
            json.dumps(limpar_json(dados), ensure_ascii=False, allow_nan=False),
            encoding="utf-8"
        )
        return

    try:
        gravar(s3, bucket, chave, dados)
    except ClientError as erro:
        codigo = erro.response.get("Error", {}).get("Code")
        if codigo not in ("AccessDenied", "AllAccessDisabled"):
            raise

        estado_saida["modo"] = "local"
        estado_saida["motivo"] = "Sem permissão de escrita no S3; usando Gold local para billing."
        print(f"Billing: {estado_saida['motivo']}")
        caminho = Path(GOLD_LOCAL_DIR) / chave
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_text(
            json.dumps(limpar_json(dados), ensure_ascii=False, allow_nan=False),
            encoding="utf-8"
        )


def montar_payload_mes(df_mes, metricas_mes_anterior):
    if df_mes.empty:
        return None

    diarios = (df_mes.groupby("dia", as_index=False)
               .agg(custo_dia=("custo_estimado", "sum"),
                    consumo_dia=("consumo_kwh", "sum"),
                    tarifa_media_dia=("tarifa_kwh", "mean"),
                    aviso=("aviso_hora", "sum"),
                    critico=("critico_hora", "sum"))
               .sort_values("dia"))

    if diarios.empty:
        return None

    tarifa_base = diarios["tarifa_media_dia"].where(diarios["tarifa_media_dia"] > 0).min()
    if pd.isna(tarifa_base):
        tarifa_base = max(0.0001, diarios["tarifa_media_dia"].mean() * 0.75)

    impacto_consumo = []
    impacto_tarifa = []
    for _, linha in diarios.iterrows():
        efeito_consumo = min(float(linha.custo_dia) * 0.92, float(linha.consumo_dia) * float(tarifa_base))
        efeito_tarifa = max(0.0, float(linha.custo_dia) - efeito_consumo)
        impacto_consumo.append(round(efeito_consumo, 2))
        impacto_tarifa.append(round(efeito_tarifa, 2))

    ranking = (df_mes.groupby("hostname", as_index=False)
               .agg(custo_total=("custo_estimado", "sum"),
                    consumo_total=("consumo_kwh", "sum"),
                    aviso=("aviso_hora", "sum"),
                    critico=("critico_hora", "sum"))
               .sort_values("custo_total", ascending=False))

    primeiro = diarios.iloc[0]["dia"]
    ultimo = diarios.iloc[-1]["dia"]
    chave_mes = mes_ref(primeiro)
    dias_no_mes = pd.Period(chave_mes, freq="M").days_in_month
    ultimo_dia_mes = pd.Timestamp(year=int(primeiro.year), month=int(primeiro.month), day=int(dias_no_mes)).date()
    ultimo_dia_real = pd.Timestamp(ultimo).date()
    dias_projecao = []
    if ultimo_dia_real < ultimo_dia_mes:
        faixa_projecao = pd.date_range(ultimo_dia_real + pd.Timedelta(days=1), ultimo_dia_mes, freq="D")
        dias_projecao = [dia.strftime("%d/%m") for dia in faixa_projecao.tolist()]

    return {
        "periodoMensal": {
            "descricaoMes": chave_mes,
            "rotuloAcumulado": f"{primeiro.strftime('%d/%m')} a {ultimo.strftime('%d/%m')}",
            "consumoKwh": round(float(diarios["consumo_dia"].sum()), 0)
        },
        "historicoDiario": {
            "dias": [dia.strftime("%d/%m") for dia in diarios["dia"].tolist()],
            "custos": [round(float(valor), 2) for valor in diarios["custo_dia"].tolist()],
            "diasProjecao": dias_projecao
        },
        "alertasDiarios": {
            "aviso": [int(v) for v in diarios["aviso"].tolist()],
            "critico": [int(v) for v in diarios["critico"].tolist()]
        },
        "decomposicaoDiaria": {
            "impactoConsumo": impacto_consumo,
            "impactoTarifa": impacto_tarifa
        },
        "rankingNodes": {
            "labels": ranking["hostname"].astype(str).tolist(),
            "custos": [round(float(v), 2) for v in ranking["custo_total"].tolist()],
            "consumosKwh": [round(float(v), 0) for v in ranking["consumo_total"].tolist()],
            "alertasAvisoPorNode": [int(v) for v in ranking["aviso"].tolist()],
            "alertasCriticoPorNode": [int(v) for v in ranking["critico"].tolist()]
        },
        "metricasMesAnterior": metricas_mes_anterior
    }


def publicar_billing_no_s3(s3, bucket, registros, inicio, fim):
    if not registros:
        return {"arquivos": 0, "empresas": 0, "meses": 0, "modo_saida": "s3", "aviso": None}

    df = pd.DataFrame(registros)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["dia"] = df["timestamp"].dt.normalize()
    serie_mes = df["timestamp"]
    if getattr(serie_mes.dt, "tz", None) is not None:
        serie_mes = serie_mes.dt.tz_localize(None)
    df["mes"] = serie_mes.dt.to_period("M").astype(str)
    df = sintetizar_alertas_billing(df)
    estado_saida = {"modo": "s3", "motivo": None}

    total_arquivos = 0
    total_meses = 0
    empresas = sorted(df["id_empresa"].dropna().astype(int).unique().tolist())

    for id_empresa in empresas:
        base_empresa = df[df["id_empresa"].astype(int) == int(id_empresa)].copy()
        meses = sorted(base_empresa["mes"].dropna().unique().tolist())
        metricas_por_mes = {}

        for chave_mes in meses:
            df_mes = base_empresa[base_empresa["mes"] == chave_mes].copy()
            indice = meses.index(chave_mes)
            mes_anterior = meses[indice - 1] if indice > 0 else None
            metricas_mes_anterior = metricas_por_mes.get(mes_anterior)
            payload = montar_payload_mes(df_mes, metricas_mes_anterior)
            if not payload:
                continue

            metricas_por_mes[chave_mes] = calcular_metricas_periodo(payload)
            chave_resumo = f"{BILLING_GOLD_PREFIX}/empresa={id_empresa}/mes={chave_mes}/resumo.json"
            gravar_billing_saida(s3, bucket, chave_resumo, payload, estado_saida)
            total_arquivos += 1
            total_meses += 1

        chave_meses = f"{BILLING_GOLD_PREFIX}/empresa={id_empresa}/meses.json"
        gravar_billing_saida(s3, bucket, chave_meses, {
            "empresa": int(id_empresa),
            "meses": meses,
            "gerado_em": pd.Timestamp.now(tz=FUSO)
        }, estado_saida)
        total_arquivos += 1

    return {
        "arquivos": total_arquivos,
        "empresas": len(empresas),
        "meses": total_meses,
        "modo_saida": estado_saida["modo"],
        "aviso": estado_saida["motivo"]
    }


def executar_billing():
    agora = pd.Timestamp.now(tz=FUSO)
    ano_ref = ano_referencia_setembro(agora)
    inicio = pd.Timestamp(year=ano_ref, month=9, day=1, hour=0, minute=0, second=0, tz=FUSO)
    fim = limite_demo_projecao(agora)

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", os.getenv("REGION_NAME", "us-east-1")))
    bucket = os.environ["BUCKET_NAME"]
    mapa_tarifas = montar_mapa_tarifas(inicio, fim)

    try:
        df = carregar_silver_billing(s3, bucket, agora, inicio=inicio, colunas=COLUNAS)
    except ClientError as erro:
        if erro.response.get("Error", {}).get("Code") in ("AccessDenied", "AllAccessDisabled"):
            print("Billing: sem permissão de listagem no S3; usando Silver local.")
            df = carregar_silver_billing_local(agora, inicio=inicio, colunas=COLUNAS)
        else:
            raise

    if df.empty:
        raise ValueError("Sem hosts na Silver para montar o Billing.")

    df = expandir_df_para_hosts_demo(df, BILLING_HOSTNAMES_DEMO)
    catalogo = catalogo_billing_de_dataframe(df)
    df_reais_ate_limite = df[df["TIMESTAMP"] <= fim].copy()
    if df_reais_ate_limite.empty:
        print("Billing: sem dados reais até 15/11; usando histórico sintético para o período da apresentação.")

    parametros = parametros_billing_padrao(catalogo)
    resposta_parametros_api = carregar_parametros_billing_api(catalogo)
    parametros_api = resposta_parametros_api.get("parametros", {})
    empresa_id_api = resposta_parametros_api.get("empresa_id")
    if isinstance(empresa_id_api, int) and empresa_id_api > 0:
        for info in catalogo.values():
            info["id_empresa"] = empresa_id_api
        print(f"Billing: empresa autenticada na API = {empresa_id_api}; IDs de empresa sincronizados no catálogo.")
    if parametros_api:
        parametros.update(parametros_api)

    registros_sinteticos = montar_registros_billing_sinteticos(catalogo, parametros, inicio, fim, mapa_tarifas)
    registros_reais = montar_registros_billing_reais(df_reais_ate_limite, catalogo, parametros, mapa_tarifas)
    registros = mesclar_registros_billing(registros_sinteticos, registros_reais)

    resultado = publicar_billing_no_s3(s3, bucket, registros, inicio, fim)
    return {
        "modo": BILLING_MODO,
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
        "nodes": len(catalogo),
        "registros_processados": len(registros),
        "arquivos_publicados": resultado["arquivos"],
        "empresas": resultado["empresas"],
        "meses": resultado["meses"],
        "modo_saida": resultado.get("modo_saida", "s3"),
        "aviso": resultado.get("aviso"),
    }


def lambda_handler_billing(event, context):
    for variavel in ("BUCKET_NAME",):
        if not os.getenv(variavel):
            raise ValueError(f"Configure a variável {variavel} na Lambda.")

    return executar_billing()


def executar_todas_dashboards():
    pipelines = [
        ("gestor", executar),
        ("operador", executar_operador),
        ("analista", executar_analista),
        ("categorizacao", executar_categorizacao),
        ("billing", executar_billing),
    ]

    resumo = {"sucesso": {}, "falhas": {}}
    for nome, funcao in pipelines:
        try:
            print(f"\n--- ETL {nome}: iniciando ---")
            retorno = funcao()
            resumo["sucesso"][nome] = retorno
            print(f"--- ETL {nome}: concluída ---")
        except Exception as erro:
            resumo["falhas"][nome] = f"{type(erro).__name__}: {erro}"
            print(f"--- ETL {nome}: falhou ({type(erro).__name__}) ---")

    print("\nResumo ETL:")
    print(json.dumps(resumo, ensure_ascii=False, default=str, indent=2))
    return resumo


def executar_por_alvo(alvo):
    alvo = (alvo or "todas").strip().lower()
    if alvo in ("todas", "all"):
        return executar_todas_dashboards()

    mapa = {
        "gestor": executar,
        "operador": executar_operador,
        "analista": executar_analista,
        "categorizacao": executar_categorizacao,
        "billing": executar_billing,
    }
    if alvo not in mapa:
        raise ValueError("Alvo inválido. Use: todas, gestor, operador, analista, categorizacao ou billing.")
    retorno = mapa[alvo]()
    print(json.dumps({alvo: retorno}, ensure_ascii=False, default=str, indent=2))
    return {"sucesso": {alvo: retorno}, "falhas": {}}


# Este bloco só roda quando o arquivo é executado direto (python arquivo.py).
# Quando outro script importa este módulo, nada aqui é executado.
if __name__ == "__main__":
    alvo_cli = sys.argv[1] if len(sys.argv) > 1 else "todas"
    resultado = executar_por_alvo(alvo_cli)
    if resultado.get("falhas"):
        raise SystemExit(1)
