# ETL Silver -> Gold: gestor, analista e operador.


# ======================================================================
# SECAO GESTOR — origem: silver2gold.py
# ======================================================================

import io
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config_aws import *

import shutil

PREFIXO_ORIGEM_GESTOR = '02-silver/'
PREFIXO_DESTINO_GESTOR = '03-gold/'

def listar_chaves_gestor(prefixo):
    paginator = s3_client.get_paginator('list_objects_v2')
    params = {
        'Bucket': os.getenv('BUCKET_NAME'),
        'Prefix': prefixo
    }
    page_iterator = paginator.paginate(**params)
    chaves = []
    for pagina in page_iterator:
        for obj in pagina.get('Contents', []):
            chave = obj['Key']
            if chave.endswith('.csv'):
                chaves.append(chave)
    return chaves

def baixar_arquivo(chave_origem):
    caminho_local = os.path.join('silver_local', os.path.basename(chave_origem))
    os.makedirs(os.path.dirname(caminho_local), exist_ok=True)
    s3_client.download_file(os.getenv('BUCKET_NAME'), chave_origem, caminho_local)
    return caminho_local

def processar_camada_gold_gestor():
    chaves_silver = listar_chaves_gestor(PREFIXO_ORIGEM_GESTOR)
    if not chaves_silver:
        print("Nenhum arquivo encontrado na camada Silver para processar.")
        return

    dfs = []
    for chave in chaves_silver:
        caminho = baixar_arquivo(chave)
        df_temp = pd.read_csv(caminho, sep=';')
        dfs.append(df_temp)


    # ==========================================
    # GESTOR
    # ==========================================


    # Consolida todos os dados da Silver em um único DataFrame global para análise do Gestor
    df_global = pd.concat(dfs, ignore_index=True)
    df_global['TIMESTAMP'] = pd.to_datetime(df_global['TIMESTAMP'], errors='coerce')
    df_global.dropna(subset=['TIMESTAMP'], inplace=True)

    agora = datetime.now()
    ultimas_24h = agora - timedelta(hours=24)
    ultimos_30_dias = agora - timedelta(days=30)

    # Totais das ultimas 24h, usando a classificacao compartilhada abaixo.
    status_24h = df_global.loc[df_global['TIMESTAMP'] >= ultimas_24h].apply(classificar_alerta, axis=1)
    total_critico = int((status_24h == 'critico').sum())
    total_aviso = int((status_24h == 'aviso').sum())

    # GRÁFICO 1: Incidentes por Node (Últimos 30 dias)
    df_30d = df_global[df_global['TIMESTAMP'] >= ultimos_30_dias]
    if not df_30d.empty:
        df_30d['STATUS_INCIDENTE'] = df_30d.apply(classificar_alerta, axis=1)
        df_incidentes = df_30d[df_30d['STATUS_INCIDENTE'] != 'ok']
        grafico_historico = df_incidentes.groupby('HOSTNAME').size().reset_index(name='total_incidentes')
        grafico_historico = grafico_historico.sort_values(by='total_incidentes', ascending=False).head(10)
    else:
        grafico_historico = pd.DataFrame(columns=['HOSTNAME', 'total_incidentes'])

    # GRÁFICO 2: Projeção de Piora (Janelas de 30 dias)
    # Dividindo os últimos 90 dias em janelas de 30 dias para regressão linear
    janela_1 = df_global[(df_global['TIMESTAMP'] >= agora - timedelta(days=90)) & (df_global['TIMESTAMP'] < agora - timedelta(days=60))]
    janela_2 = df_global[(df_global['TIMESTAMP'] >= agora - timedelta(days=60)) & (df_global['TIMESTAMP'] < agora - timedelta(days=30))]
    janela_3 = df_global[df_global['TIMESTAMP'] >= agora - timedelta(days=30)]

    def contar_por_node(df):
        if df.empty:
            return pd.Series(dtype=int)
        df['inc'] = df.apply(classificar_alerta, axis=1) != 'ok'
        return df[df['inc']].groupby('HOSTNAME').size()

    j1 = contar_por_node(janela_1)
    j2 = contar_por_node(janela_2)
    j3 = contar_por_node(janela_3)

    nodes_unicos = set(list(j1.index) + list(j2.index) + list(j3.index))
    projecoes = []

    for node in nodes_unicos:
        v1 = j1.get(node, 0)
        v2 = j2.get(node, 0)
        v3 = j3.get(node, 0)
        janelas_y = [v1, v2, v3]
        
        # Regressão linear simples para previsão dos próximos 30 dias
        n = len(janelas_y)
        xm = (n - 1) / 2
        ym = sum(janelas_y) / n
        num = sum((i - xm) * (v - ym) for i, v in enumerate(janelas_y))
        den = sum((i - xm) ** 2 for i in range(n))
        
        a = (num / den) if den != 0 else 0
        b = ym - a * xm
        previsto = max(0, a * n + b)
        variacao = previsto - v3

        projecoes.append({
            'nome': node,
            'janela_antiga_1': v1,
            'janela_antiga_2': v2,
            'ultimos30': v3,
            'previsto': round(previsto, 1),
            'variacao': round(variacao, 1)
        })


    df_projecoes = pd.DataFrame(projecoes)
    if not df_projecoes.empty:
        df_projecoes = df_projecoes.sort_values(by='variacao', ascending=False)

    # ==========================================
    # Gravando os arquivos consolidados na Gold
    # ==========================================
    os.makedirs('gold_local', exist_ok=True)
    
    path_kpis = 'gold_local/kpis_resumo.csv'
    path_hist = 'gold_local/grafico_historico.csv'
    path_proj = 'gold_local/grafico_projecao.csv'

    # DataFrame de KPIs resumidos
    df_kpis = pd.DataFrame([{
        'total_alertas_24h': total_critico + total_aviso,
        'alertas_aviso': total_aviso,
        'alertas_critico': total_critico,
        'atualizado_em': agora.strftime('%Y-%m-%d %H:%M:%S')
    }])

    df_kpis.to_csv(path_kpis, sep=';', index=False)
    grafico_historico.to_csv(path_hist, sep=';', index=False)
    df_projecoes.to_csv(path_proj, sep=';', index=False)

    # Upload para o S3 (Camada Gold)
    momento_str = agora.strftime('%Y%m%d_%H%M%S')
    upload_file(path_kpis, os.getenv('BUCKET_NAME'), f'{PREFIXO_DESTINO_GESTOR}kpis/kpis_{momento_str}.csv')
    upload_file(path_hist, os.getenv('BUCKET_NAME'), f'{PREFIXO_DESTINO_GESTOR}historico/historico_{momento_str}.csv')
    upload_file(path_proj, os.getenv('BUCKET_NAME'), f'{PREFIXO_DESTINO_GESTOR}projecao/projecao_{momento_str}.csv')

    print("ETL Silver -> Gold executada com sucesso!")


# ======================================================================
# SECAO ANALISTA — origem: silver2goldAnalista.py
# ======================================================================


import os
import json
from datetime import datetime, timezone

import pandas as pd
import numpy as np

from config_aws import s3_client


# =========================================================
# CONFIGURACOES
# =========================================================

PREFIXO_ORIGEM_ANALISTA = "02-silver/"
PREFIXO_DESTINO_ANALISTA = "03-gold/analista/"

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

def listar_chaves_analista(prefixo):
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


def carregar_dados_silver_analista():
    """Baixa os CSVs da Silver diretamente para a memória."""

    bucket = os.getenv("BUCKET_NAME")
    chaves = listar_chaves_analista(PREFIXO_ORIGEM_ANALISTA)

    if not chaves:
        raise ValueError(
            f"Nenhum CSV encontrado em {PREFIXO_ORIGEM_ANALISTA}"
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
    df = carregar_dados_silver_analista()

    if df.empty:
        raise ValueError("A Silver não contém dados válidos.")

    print(f"Total de registros válidos: {len(df)}")

    relatorio = {
        "metadados": {
            "gerado_em_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "origem": PREFIXO_ORIGEM_ANALISTA,
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

    chave_destino = PREFIXO_DESTINO_ANALISTA + nome_arquivo

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



# ======================================================================
# SECAO OPERADOR — origem: silver2goldoperador.py
# ======================================================================

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

PREFIXO_ORIGEM_OPERADOR = "02-silver/"
PREFIXO_DESTINO_OPERADOR = "03-gold/operador/"

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

def listar_chaves_operador(prefixo):
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


def carregar_dados_silver_operador():
    """Baixa para a memória apenas os CSVs recentes de cada node."""

    bucket = os.getenv("BUCKET_NAME")
    chaves = selecionar_chaves_recentes(listar_chaves_operador(PREFIXO_ORIGEM_OPERADOR))

    if not chaves:
        raise ValueError(f"Nenhum CSV encontrado em {PREFIXO_ORIGEM_OPERADOR}")

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
    df = carregar_dados_silver_operador()

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
            "origem": PREFIXO_ORIGEM_OPERADOR,
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

    chave_destino = PREFIXO_DESTINO_OPERADOR + nome_arquivo

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



# ======================================================================
# INTEGRACAO DAS TRES ROTINAS
# ======================================================================

def upload_file(file_name, bucket, object_name):
    # Upload do gestor; propaga falhas para nao indicar sucesso indevidamente.
    s3_client.upload_file(file_name, bucket, object_name)
    return True


def classificar_alerta(linha):
    # Gestor: reutiliza os limites de atencao/critico da secao OPERADOR.
    estados = [classificar_valor(metrica, linha.get(coluna))
               for metrica, coluna in METRICAS.items()]
    if 'critico' in estados:
        return 'critico'
    if 'atencao' in estados:
        return 'aviso'
    return 'ok'


def processar_camada_gold():
    """Executa gestor, analista e operador em sequencia."""
    try:
        processar_camada_gold_gestor()
        analista = processar_camada_gold_analista()
        operador = processar_camada_gold_operador()
        if os.getenv("INTERPRETACAO_ATIVA", "false").lower() == "true":
            try:
                from silver2gold_interpretacao import (
                    processar_camada_gold_interpretacao,
                )
                processar_camada_gold_interpretacao()
            except Exception as e:
                print(f"Interpretação falhou: {e}")
        return {'analista': analista, 'operador': operador}
    finally:
        shutil.rmtree('silver_local', ignore_errors=True)
        shutil.rmtree('gold_local', ignore_errors=True)


if __name__ == '__main__':
    processar_camada_gold()
