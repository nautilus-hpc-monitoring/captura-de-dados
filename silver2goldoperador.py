import os
import re
import json
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from config_aws import s3_client


# =========================================================
# CONFIGURACOES
# =========================================================

PREFIXO_ORIGEM = "02-silver/"
PREFIXO_DESTINO = "03-gold/operador/"

JANELA_GRAFICO_MINUTOS = 5

ARQUIVOS_POR_NODE = 8

MINUTOS_SEM_DADOS = 5

# Métrica exibida no dashboard -> coluna na camada Silver
METRICAS = {
    "CPU": "CPU_PERCENT",
    "RAM": "RAM_PERCENT",
    "DISCO": "DISCO_PERCENT",
    "SWAP": "SWAP_PERCENT"
}

# Limites (%) por métrica: acima de "atencao" -> atenção; acima de "critico" -> crítico.
LIMITES = {
    metrica: {"atencao": 75, "critico": 90}
    for metrica in METRICAS
}

ORDEM_STATUS = {"normal": 0, "atencao": 1, "critico": 2}


# =========================================================
# 1. LEITURA DOS DADOS RECENTES DA SILVER
# =========================================================

def listar_chaves(prefixo):
    bucket = os.getenv("BUCKET_NAME")

    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

    paginator = s3_client.get_paginator("list_objects_v2")

    chaves = []

    for pagina in paginator.paginate(Bucket=bucket, Prefix=prefixo):
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]

            if chave.lower().endswith(".csv"):
                chaves.append(chave)

    return chaves


def selecionar_chaves_recentes(chaves):
    """
    Mantém só os ARQUIVOS_POR_NODE arquivos mais recentes de cada node.
    """

    por_host = {}

    for chave in chaves:
        encontrado = re.search(r"hostname=([^/]+)/", chave)
        host = encontrado.group(1) if encontrado else "desconhecido"
        por_host.setdefault(host, []).append(chave)

    selecionadas = []

    for host, lista in por_host.items():
        selecionadas.extend(sorted(lista)[-ARQUIVOS_POR_NODE:])

    return selecionadas


def carregar_dados_silver():
    """Baixa para a memória apenas os CSVs recentes de cada node."""

    bucket = os.getenv("BUCKET_NAME")
    chaves = selecionar_chaves_recentes(listar_chaves(PREFIXO_ORIGEM))

    if not chaves:
        raise ValueError(f"Nenhum CSV encontrado em {PREFIXO_ORIGEM}")

    dataframes = []

    for chave in chaves:
        resposta = s3_client.get_object(Bucket=bucket, Key=chave)
        dataframes.append(pd.read_csv(resposta["Body"], sep=";"))

    df = pd.concat(dataframes, ignore_index=True)

    for coluna in ["TIMESTAMP", "HOSTNAME"]:
        if coluna not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente nos CSVs: {coluna}")

    for coluna in METRICAS.values():
        if coluna not in df.columns:
            df[coluna] = np.nan

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")

    for coluna in METRICAS.values():
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

        df.loc[~df[coluna].between(0, 100), coluna] = np.nan

    df["HOSTNAME"] = df["HOSTNAME"].astype("string")

    df = df.dropna(subset=["TIMESTAMP", "HOSTNAME"])
    df = df.drop_duplicates(subset=["HOSTNAME", "TIMESTAMP"], keep="first")

    return df.sort_values(["HOSTNAME", "TIMESTAMP"]).reset_index(drop=True)


# =========================================================
# 2. KPIs E GRAFICOS DE CADA NODE
# =========================================================

def classificar_valor(metrica, valor):
    """normal, atencao, critico ou sem_dados (quando não há leitura)."""

    if valor is None or pd.isna(valor):
        return "sem_dados"

    limites = LIMITES[metrica]

    if valor > limites["critico"]:
        return "critico"

    if valor > limites["atencao"]:
        return "atencao"

    return "normal"


def numero(valor):
    return None if pd.isna(valor) else round(float(valor), 1)


def montar_node(hostname, dados_host, agora):
    ultima_leitura = dados_host["TIMESTAMP"].max()
    inicio_janela = ultima_leitura - pd.Timedelta(minutes=JANELA_GRAFICO_MINUTOS)

    janela = dados_host[dados_host["TIMESTAMP"] > inicio_janela]

    minutos_sem_coleta = round(
        (agora - ultima_leitura).total_seconds() / 60,
        1
    )

    kpis = {}
    status_metricas = []

    for metrica, coluna in METRICAS.items():
        leituras = janela[coluna].dropna()

        valor = numero(leituras.iloc[-1]) if not leituras.empty else None
        status = classificar_valor(metrica, valor)

        kpis[metrica] = {"valor": valor, "status": status}
        status_metricas.append(status)

    if minutos_sem_coleta > MINUTOS_SEM_DADOS:
        status_node = "sem_dados"
    else:
        validos = [s for s in status_metricas if s in ORDEM_STATUS]
        status_node = (
            max(validos, key=ORDEM_STATUS.get) if validos else "sem_dados"
        )

    graficos = {
        "inicio": inicio_janela.isoformat(),
        "fim": ultima_leitura.isoformat(),
        "labels": janela["TIMESTAMP"].dt.strftime("%H:%M:%S").tolist()
    }

    for metrica, coluna in METRICAS.items():
        graficos[metrica] = [numero(v) for v in janela[coluna]]

    return {
        "hostname": str(hostname),
        "ultima_coleta": ultima_leitura.isoformat(),
        "minutos_sem_coleta": minutos_sem_coleta,
        "status": status_node,
        "kpis": kpis,
        "graficos": graficos
    }


# =========================================================
# 3. GERACAO E ENVIO DO JSON PARA GOLD
# =========================================================

def processar_camada_gold_operador():
    df = carregar_dados_silver()

    if df.empty:
        raise ValueError("A Silver não contém dados válidos.")

    print(f"Total de registros lidos: {len(df)}")

    agora = pd.Timestamp(datetime.now())

    nodes = [
        montar_node(hostname, dados_host, agora)
        for hostname, dados_host in df.groupby("HOSTNAME")
    ]

    relatorio = {
        "metadados": {
            "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
            "origem": PREFIXO_ORIGEM,
            "quantidade_registros": int(len(df)),
            "quantidade_nodes": len(nodes),
            "janela_grafico_minutos": JANELA_GRAFICO_MINUTOS,
            "minutos_sem_dados": MINUTOS_SEM_DADOS
        },
        "limites": LIMITES,
        "nodes": nodes
    }

    bucket = os.getenv("BUCKET_NAME")

    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

    nome_arquivo = (
        "operador_"
        + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        + ".json"
    )

    chave_destino = PREFIXO_DESTINO + nome_arquivo

    conteudo_json = json.dumps(
        relatorio,
        ensure_ascii=False,
        indent=2,
        allow_nan=False
    ).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=chave_destino,
        Body=conteudo_json,
        ContentType="application/json; charset=utf-8"
    )

    print(f"JSON enviado para: {chave_destino}")

    return relatorio


if __name__ == "__main__":
    relatorio = processar_camada_gold_operador()
    print("ETL do Operador concluído!")

    for node in relatorio["nodes"]:
        print(f"  {node['hostname']}: {node['status']} "
              f"(última coleta há {node['minutos_sem_coleta']} min)")