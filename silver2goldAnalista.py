
import os
import json
from datetime import datetime, timezone

import pandas as pd
import numpy as np

from config_aws import s3_client


# =========================================================
# CONFIGURACOES
# =========================================================

PREFIXO_ORIGEM = "02-silver/"
PREFIXO_DESTINO = "03-gold/analista/"

LIMITE_ANOMALIA = 80
LIMITE_VARIACAO = 20

COMPONENTES = {
    "CPU": "CPU_PERCENT",
    "RAM": "RAM_PERCENT",
    "DISCO": "DISCO_PERCENT"
}

COLUNAS_OBRIGATORIAS = [
    "TIMESTAMP",
    "HOSTNAME",
    "CPU_PERCENT",
    "RAM_PERCENT",
    "DISCO_PERCENT"
]


# =========================================================
# 1. LEITURA DOS DADOS DA SILVER
# =========================================================

def listar_chaves(prefixo):
    """Lista os arquivos CSV existentes em um prefixo do S3."""

    bucket = os.getenv("BUCKET_NAME")

    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

    paginator = s3_client.get_paginator("list_objects_v2")

    chaves = []

    for pagina in paginator.paginate(
        Bucket=bucket,
        Prefix=prefixo
    ):
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]

            if chave.lower().endswith(".csv"):
                chaves.append(chave)

    return chaves


def carregar_dados_silver():
    """Baixa os CSVs da Silver diretamente para a memória."""

    bucket = os.getenv("BUCKET_NAME")
    chaves = listar_chaves(PREFIXO_ORIGEM)

    if not chaves:
        raise ValueError(
            f"Nenhum CSV encontrado em {PREFIXO_ORIGEM}"
        )

    dataframes = []

    for chave in chaves:
        resposta = s3_client.get_object(
            Bucket=bucket,
            Key=chave
        )

        df_temp = pd.read_csv(
            resposta["Body"],
            sep=";"
        )

        dataframes.append(df_temp)

        print(f"Arquivo lido: {chave}")

    df = pd.concat(dataframes, ignore_index=True)

    # Confere se as colunas utilizadas existem.
    faltantes = [
        coluna
        for coluna in COLUNAS_OBRIGATORIAS
        if coluna not in df.columns
    ]

    if faltantes:
        raise ValueError(
            f"Colunas ausentes nos CSVs: {faltantes}"
        )

    df["TIMESTAMP"] = pd.to_datetime(
        df["TIMESTAMP"],
        errors="coerce"
    )

    for coluna in COMPONENTES.values():
        df[coluna] = pd.to_numeric(
            df[coluna],
            errors="coerce"
        )

        # Percentuais válidos devem estar entre 0 e 100.
        df.loc[
            ~df[coluna].between(0, 100),
            coluna
        ] = np.nan

    df["HOSTNAME"] = df["HOSTNAME"].astype("string")

    df = df.dropna(
        subset=["TIMESTAMP", "HOSTNAME"]
    )

    df = df.drop_duplicates(
        subset=["HOSTNAME", "TIMESTAMP"],
        keep="first"
    )

    return df.sort_values("TIMESTAMP").reset_index(drop=True)


# =========================================================
# 2. MEDIAS SEMANAIS E MENSAIS
# =========================================================

def calcular_medias_periodicas(df):
    """Calcula as médias de CPU, RAM e disco por semana e mês."""

    resultado = {
        "semanal": [],
        "mensal": []
    }

    for nome_periodo, frequencia in [
        ("semanal", "W"),
        ("mensal", "MS")
    ]:
        agrupado = (
            df.set_index("TIMESTAMP")
            .groupby("HOSTNAME")
        )

        for hostname, dados_host in agrupado:
            medias = (
                dados_host[list(COMPONENTES.values())]
                .resample(frequencia)
                .mean()
                .dropna(how="all")
            )

            for timestamp, linha in medias.iterrows():
                resultado[nome_periodo].append({
                    "hostname": str(hostname),
                    "periodo_inicio": timestamp.isoformat(),
                    "medias": {
                        componente: (
                            round(float(linha[coluna]), 2)
                            if pd.notna(linha[coluna])
                            else None
                        )
                        for componente, coluna
                        in COMPONENTES.items()
                    }
                })

    return resultado


# =========================================================
# 3. COMPARACAO ENTRE PERIODOS
# =========================================================

def calcular_comparacao(df, dias):
    """
    Compara os últimos N dias disponíveis com os N dias anteriores.
    A referência é o timestamp mais recente dos dados.
    """

    fim_atual = df["TIMESTAMP"].max()
    inicio_atual = fim_atual - pd.Timedelta(days=dias)
  
    inicio_anterior = (
        inicio_atual - pd.Timedelta(days=dias)
    )

    periodo_atual = df[
        (df["TIMESTAMP"] > inicio_atual)
        & (df["TIMESTAMP"] <= fim_atual)
    ]

    periodo_anterior = df[
        (df["TIMESTAMP"] > inicio_anterior)
        & (df["TIMESTAMP"] <= inicio_atual)
    ]

    comparacao = {}

    for componente, coluna in COMPONENTES.items():
        media_atual = periodo_atual[coluna].mean()
        media_anterior = periodo_anterior[coluna].mean()

        if pd.isna(media_atual):
            variacao = None
        elif pd.isna(media_anterior):
            variacao = None
        elif media_anterior == 0:
            variacao = (
                0.0 if media_atual == 0 else None
            )
        else:
            variacao = (
                (media_atual - media_anterior)
                / abs(media_anterior)
            ) * 100

        comparacao[componente] = {
            "media_atual": (
                round(float(media_atual), 2)
                if pd.notna(media_atual)
                else None
            ),
            "media_anterior": (
                round(float(media_anterior), 2)
                if pd.notna(media_anterior)
                else None
            ),
            "variacao_percentual": (
                round(float(variacao), 2)
                if variacao is not None
                else None
            ),
            "ultrapassou_limite_variacao": (
                abs(variacao) > LIMITE_VARIACAO
                if variacao is not None
                else None
            )
        }

    return {
        "dias_por_periodo": dias,
        "periodo_atual": {
            "inicio": inicio_atual.isoformat(),
            "fim": fim_atual.isoformat()
        },
        "periodo_anterior": {
            "inicio": inicio_anterior.isoformat(),
            "fim": inicio_atual.isoformat()
        },
        "componentes": comparacao
    }


# =========================================================
# 4. PICOS E PERCENTIS
# =========================================================

def calcular_estatisticas(df):
    """Calcula pico, data do pico e percentis de cada componente."""

    resultado = {}

    for componente, coluna in COMPONENTES.items():
        dados_validos = df.dropna(subset=[coluna])

        if dados_validos.empty:
            resultado[componente] = None
            continue

        indice_pico = dados_validos[coluna].idxmax()
        pico = dados_validos.loc[indice_pico]

        valores = dados_validos[coluna]

        resultado[componente] = {
            "pico": {
                "valor": round(float(pico[coluna]), 2),
                "timestamp": pico["TIMESTAMP"].isoformat(),
                "hostname": str(pico["HOSTNAME"])
            },
            "percentis": {
                "P50": round(float(valores.quantile(0.50)), 2),
                "P90": round(float(valores.quantile(0.90)), 2),
                "P95": round(float(valores.quantile(0.95)), 2),
                "P99": round(float(valores.quantile(0.99)), 2)
            },
            "media_geral": round(float(valores.mean()), 2),
            "minimo": round(float(valores.min()), 2),
            "maximo": round(float(valores.max()), 2)
        }

    return resultado


# =========================================================
# 5. ANOMALIAS E DURACAO DOS EVENTOS
# =========================================================

def detectar_anomalias(df):
    """
    Considera anomalia cada leitura acima de 80%.

    Agrupa leituras próximas do mesmo host e componente
    em eventos. Uma lacuna superior a 30 segundos separa
    eventos diferentes.
    """

    leituras = []

    for componente, coluna in COMPONENTES.items():
        anormais = df[df[coluna] > LIMITE_ANOMALIA]

        for _, linha in anormais.iterrows():
            leituras.append({
                "hostname": str(linha["HOSTNAME"]),
                "componente": componente,
                "timestamp": linha["TIMESTAMP"],
                "valor": float(linha[coluna])
            })

    if not leituras:
        return {
            "limite_percentual": LIMITE_ANOMALIA,
            "quantidade_leituras_anomalas": 0,
            "quantidade_eventos": 0,
            "duracao_total_segundos": 0,
            "eventos": []
        }

    anomalias = pd.DataFrame(leituras)
    anomalias = anomalias.sort_values("timestamp")

    eventos = []

    for (hostname, componente), grupo in anomalias.groupby(
        ["hostname", "componente"]
    ):
        grupo = grupo.sort_values("timestamp")

        evento_atual = []

        for _, leitura in grupo.iterrows():
            if evento_atual:
                diferenca = (
                    leitura["timestamp"]
                    - evento_atual[-1]["timestamp"]
                ).total_seconds()

                if diferenca > 30:
                    eventos.append(
                        criar_evento(
                            hostname,
                            componente,
                            evento_atual
                        )
                    )
                    evento_atual = []

            evento_atual.append(leitura)

        if evento_atual:
            eventos.append(
                criar_evento(
                    hostname,
                    componente,
                    evento_atual
                )
            )

    duracao_total = sum(
        evento["duracao_segundos"]
        for evento in eventos
    )

    return {
        "limite_percentual": LIMITE_ANOMALIA,
        "quantidade_leituras_anomalas": len(anomalias),
        "quantidade_eventos": len(eventos),
        "duracao_total_segundos": duracao_total,
        "eventos": eventos
    }


def criar_evento(hostname, componente, leituras):
    """Resume um conjunto de leituras anômalas em um evento."""

    inicio = leituras[0]["timestamp"]
    fim = leituras[-1]["timestamp"]

    # A leitura final também conta como parte do evento.
    # Para uma única leitura, a duração reportada é zero.
    duracao = int((fim - inicio).total_seconds())

    return {
        "hostname": hostname,
        "componente": componente,
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
        "duracao_segundos": duracao,
        "quantidade_leituras": len(leituras),
        "valor_maximo": round(
            max(leitura["valor"] for leitura in leituras),
            2
        )
    }


# =========================================================
# 6. COMPONENTE MAIS INSTAVEL
# =========================================================

def calcular_componente_mais_instavel(df):
    """
    Mede a variação relativa entre o menor e o maior valor
    observado em cada componente.

    Variacao = (maximo - minimo) / minimo * 100.
    Se o mínimo for zero, a variação relativa é indefinida.
    """

    resultado = {}

    for componente, coluna in COMPONENTES.items():
        valores = df[coluna].dropna()

        if valores.empty:
            resultado[componente] = None
            continue

        minimo = float(valores.min())
        maximo = float(valores.max())

        if minimo == 0:
            variacao = None
        else:
            variacao = ((maximo - minimo) / abs(minimo)) * 100

        resultado[componente] = {
            "minimo": round(minimo, 2),
            "maximo": round(maximo, 2),
            "variacao_percentual": (
                round(variacao, 2)
                if variacao is not None
                else None
            ),
            "ultrapassou_limite_variacao": (
                variacao > LIMITE_VARIACAO
                if variacao is not None
                else None
            )
        }

    candidatos = {
        componente: dados["variacao_percentual"]
        for componente, dados in resultado.items()
        if dados is not None
        and dados["variacao_percentual"] is not None
    }

    mais_instavel = (
        max(candidatos, key=candidatos.get)
        if candidatos
        else None
    )

    return {
        "limite_variacao_percentual": LIMITE_VARIACAO,
        "componentes": resultado,
        "componente_mais_instavel": mais_instavel
    }


# =========================================================
# 7. GERACAO E ENVIO DO JSON PARA GOLD
# =========================================================

def processar_camada_gold_analista():
    df = carregar_dados_silver()

    if df.empty:
        raise ValueError("A Silver não contém dados válidos.")

    print(f"Total de registros válidos: {len(df)}")

    relatorio = {
        "metadados": {
            "gerado_em_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "origem": PREFIXO_ORIGEM,
            "quantidade_registros": int(len(df)),
            "primeira_leitura": df["TIMESTAMP"].min().isoformat(),
            "ultima_leitura": df["TIMESTAMP"].max().isoformat()
        },
        "limites": {
            "anomalia_percentual": LIMITE_ANOMALIA,
            "variacao_percentual": LIMITE_VARIACAO
        },
        "medias_periodicas": calcular_medias_periodicas(df),
        "comparacao_semanal": calcular_comparacao(df, 7),
        "comparacao_mensal": calcular_comparacao(df, 30),
        "estatisticas": calcular_estatisticas(df),
        "anomalias": detectar_anomalias(df),
        "instabilidade": calcular_componente_mais_instavel(df)
    }

    nome_arquivo = (
        "analista_"
        + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        + ".json"
    )

    chave_destino = PREFIXO_DESTINO + nome_arquivo

    bucket = os.getenv("BUCKET_NAME")

    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

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
    relatorio = processar_camada_gold_analista()
    print("ETL da Analista concluído!")
    print(
        "Eventos de anomalia:",
        relatorio["anomalias"]["quantidade_eventos"]
    )
