"""Entrada Gold da equipe: gestor (João) e analista (Isa).

GOLD_DASHBOARD=gestor (padrão), analista ou ambas.
As duas ETLs ficam neste arquivo, em blocos ETL JOÃO e ETL ISA.
Cada uma mantém seus cálculos, suas leituras e seus caminhos JSON.

MAPA DO ARQUIVO
1. Configuração e nodes: variáveis de ambiente ou cadastro do banco.
2. Leitura da Silver: validação, filtro de datas e remoção de duplicatas.
3. Publicação JSON e reutilização das previsões do período.
4. Gráfico 1: ocupação de disco real e prevista.
5. Gráfico 2: mapa de calor de CPU e RAM.
6. Gráfico 3: consumo real e referência das duas semanas anteriores.
7. Gráfico 5: leituras de RAM e ocupação da swap.
8. KPI: médias dos componentes desde segunda-feira.
9. Execução da ETL João.
10. ETL Isa: modelo, Silver, anomalias e episódios.
11. ETL Isa: categorias, histórico e consolidação diária.
12. ETL Isa: relatório e dashboard categórica.
13. Execução conjunta e entrada da Lambda.

A captura continua em local2bronze.py; não é feita por esta ETL.
Alertas, SLA e gráfico 4 são calculados pelo site usando o MySQL.
Local: python silver2gold.py. Lambda: silver2gold.lambda_handler.
Na Lambda compartilhada use ETL_MODULO=silver2gold.
"""
import io
import json
import math
import os
import tempfile
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
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


# ETL JOÃO — 1. CADASTRO: nodes informados na configuração ou consultados no banco.
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


# ETL JOÃO — 2. SILVER: limpeza e filtro antes de juntar os arquivos na memória.
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


# ETL JOÃO — Previsão emitida uma vez por período. Reexecuções reutilizam a mesma versão.
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


# ETL JOÃO — 3. BASE HORÁRIA: mínimo de cobertura e um valor por intervalo de coleta.
def medias_horarias(g):
    colunas = ["CPU_PERCENT", "RAM_PERCENT"]
    amostras = g.set_index("TIMESTAMP")[colunas].resample(f"{PASSO}s").mean()
    media = amostras.resample("1h").mean()
    contagem = amostras.resample("1h").count()
    return media.where(contagem >= math.ceil(3600 / PASSO * COBERTURA))


# ETL JOÃO — 4. GRÁFICO 1: último disco do dia e regressão dos 30 dias anteriores à semana.
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


# ETL JOÃO — 5. GRÁFICO 2: sete dias completos; cada dia possui 24 horas.
def calor(horas, hoje):
    dias = pd.date_range(hoje - pd.Timedelta(days=7), periods=7, freq="D")
    linhas = []
    for d in dias:
        h = horas.reindex(pd.date_range(d, periods=24, freq="1h"))
        linhas.append([{"CPU": r.CPU_PERCENT, "RAM": r.RAM_PERCENT}
                       for r in h.itertuples()])
    return {"dias": [d.date().isoformat() for d in dias], "leituras": linhas}


# ETL JOÃO — 6. GRÁFICO 3: referência das mesmas horas de 7 e 14 dias atrás.
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


# ETL JOÃO — 7. GRÁFICO 5: RAM e ocupação da swap, disponíveis também no Windows.
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


# ETL JOÃO — 8. KPI 2: média de cada componente desde segunda até a última coleta.
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


# ETL JOÃO — 9. EXECUÇÃO: publica somente o contrato consumido pela dashboard atual.
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


# ETL JOÃO — Configuração obrigatória do gestor na Lambda.
def validar_configuracao_gestor():
    for variavel in ("BUCKET_NAME", "GESTOR_CLUSTER_ID", "HOSTNAMES", "DISCO_VOLUME"):
        if not os.getenv(variavel):
            raise ValueError(f"Configure a variável {variavel} na Lambda.")

















# ============================================================
# ETL ISA — INTERPRETAÇÃO E DASHBOARD DO ANALISTA
# Cálculos, histórico e caminhos de saída originais preservados.
# ============================================================

def _env_int_interpretacao(nome, padrao):
    valor = os.getenv(nome)
    return int(valor) if valor not in (None, "") else padrao


def _env_float_interpretacao(nome, padrao):
    valor = os.getenv(nome)
    return float(valor) if valor not in (None, "") else padrao


# ETL ISA — CONFIGURAÇÃO — janela, critérios, métricas e caminhos originais.
MODELO_VERSAO = os.getenv("MODELO_VERSAO", "v1")
HORAS_JANELA = _env_int_interpretacao("HORAS_JANELA", 12)
GAP_MINUTOS = _env_int_interpretacao("GAP_MINUTOS", 10)
MIN_REGISTROS = _env_int_interpretacao("MIN_REGISTROS", 6)
LIMIAR_Z = _env_float_interpretacao("LIMIAR_Z", 3.0)

FUSO_HORARIO = ZoneInfo("America/Sao_Paulo")
HORARIO_CONSOLIDACAO = 5

METRICAS_USO = [
    "CPU_PERCENT",
    "LOAD_AVG_1",
    "RAM_PERCENT",
    "SWAP_PERCENT",
    "DISCO_PERCENT",
    "CPU_IOWAIT_PERCENT",
]

PISO_MAD = 0.5
COLUNAS_CONSTANTES = ["CPU_COUNT_LOGICA", "RAM_TOTAL", "DISCO_TOTAL"]

PREFIXO_INTERPRETACAO = "03-gold/interpretacao/"
PREFIXO_DASHBOARD = "03-gold/dashboard-categorica/"

CHAVE_HISTORICO = PREFIXO_INTERPRETACAO + "historico_episodios.json"
CHAVE_CONSOLIDACAO = PREFIXO_DASHBOARD + "consolidacao_diaria.json"
CHAVE_DASHBOARD = PREFIXO_DASHBOARD + "dashboard.json"


def _piso_mad_interpretacao(metrica):
    return 1.0 if metrica == "LOAD_AVG_1" else PISO_MAD


def _diretorio_cache_interpretacao():
    return os.path.join(
        tempfile.gettempdir(),
        "nautilus_interpretacao",
        MODELO_VERSAO,
    )


def _baixar_artefato_interpretacao(nome):
    from config_aws import s3_client

    diretorio = _diretorio_cache_interpretacao()
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


# ETL ISA — MODELO — carrega modelo, colunas e baseline já treinados.
def carregar_artefatos_interpretacao():
    # Carrega a dependência do modelo somente ao executar a ETL Isa.
    import joblib

    caminho_modelo = _baixar_artefato_interpretacao("isolation_forest.joblib")
    caminho_colunas = _baixar_artefato_interpretacao("colunas.json")
    caminho_baseline = _baixar_artefato_interpretacao("baseline.json")

    modelo = joblib.load(caminho_modelo)

    with open(caminho_colunas, "r", encoding="utf-8") as arquivo:
        colunas = json.load(arquivo)

    with open(caminho_baseline, "r", encoding="utf-8") as arquivo:
        baseline = json.load(arquivo)

    if not isinstance(colunas, list) or not colunas:
        raise ValueError("colunas.json inválido ou vazio.")

    if not isinstance(baseline, dict) or not baseline:
        raise ValueError("baseline.json inválido ou vazio.")

    return modelo, colunas, baseline


def _obter_bucket_interpretacao():
    bucket = os.getenv("BUCKET_NAME")

    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

    return bucket


def _ler_json_s3_interpretacao(chave, padrao):
    from config_aws import s3_client

    try:
        resposta = s3_client.get_object(
            Bucket=_obter_bucket_interpretacao(),
            Key=chave,
        )

        conteudo = resposta["Body"].read().decode("utf-8")
        return json.loads(conteudo)

    except ClientError as erro:
        codigo = erro.response.get("Error", {}).get("Code")

        if codigo in ("NoSuchKey", "404", "NotFound"):
            return padrao

        raise


def _gravar_json_s3_interpretacao(chave, dados):
    from config_aws import s3_client

    conteudo = json.dumps(
        dados,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")

    s3_client.put_object(
        Bucket=_obter_bucket_interpretacao(),
        Key=chave,
        Body=conteudo,
        ContentType="application/json; charset=utf-8",
    )

# ETL ISA — SILVER — mantém a leitura e a janela de dados originais.
def carregar_dados_silver_interpretacao(colunas):
    from config_aws import s3_client

    bucket = _obter_bucket_interpretacao()

    limite = datetime.now(timezone.utc) - timedelta(hours=2 * HORAS_JANELA)

    paginator = s3_client.get_paginator("list_objects_v2")
    chaves = []

    for pagina in paginator.paginate(
        Bucket=bucket,
        Prefix="02-silver/",
    ):
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]

            if not chave.lower().endswith(".csv"):
                continue

            modificado = objeto.get("LastModified")

            if modificado is None or modificado >= limite:
                chaves.append(chave)

    obrigatorias = ["TIMESTAMP", "HOSTNAME"] + list(colunas)

    if not chaves:
        return pd.DataFrame(columns=obrigatorias)

    dataframes = []

    for chave in sorted(chaves):
        resposta = s3_client.get_object(
            Bucket=bucket,
            Key=chave,
        )

        dataframes.append(
            pd.read_csv(resposta["Body"], sep=";")
        )

    df = pd.concat(dataframes, ignore_index=True)

    faltantes = [
        coluna
        for coluna in obrigatorias
        if coluna not in df.columns
    ]

    if faltantes:
        raise ValueError(
            f"Colunas ausentes nos CSVs da Silver: {faltantes}"
        )

    timestamps = pd.to_datetime(
        df["TIMESTAMP"],
        errors="coerce",
    )

    if timestamps.dt.tz is None:
        timestamps = timestamps.dt.tz_localize(
            FUSO_HORARIO,
            ambiguous="NaT",
            nonexistent="shift_forward",
        )
    else:
        timestamps = timestamps.dt.tz_convert(FUSO_HORARIO)

    df["TIMESTAMP"] = timestamps

    for coluna in colunas:
        df[coluna] = pd.to_numeric(
            df[coluna],
            errors="coerce",
        )

    df = df.dropna(subset=obrigatorias)

    df = df.drop_duplicates(
        subset=["HOSTNAME", "TIMESTAMP"],
        keep="first",
    )

    df["HOSTNAME"] = df["HOSTNAME"].astype("string")

    if df.empty:
        return df.sort_values(
            ["HOSTNAME", "TIMESTAMP"]
        ).reset_index(drop=True)

    limite_dados = (
        df["TIMESTAMP"].max()
        - timedelta(hours=HORAS_JANELA)
    )

    df = df[df["TIMESTAMP"] > limite_dados]

    return df.sort_values(
        ["HOSTNAME", "TIMESTAMP"]
    ).reset_index(drop=True)


# ETL ISA — VALIDAÇÃO — compara os nodes com o baseline.
def validar_compatibilidade_interpretacao(df, baseline):
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


# ETL ISA — ANOMALIAS — aplica predict e decision_function do modelo.
def detectar_anomalias_interpretacao(df, modelo, colunas):
    df = df.copy()

    if df.empty:
        df["anomalia"] = pd.Series(dtype="int64")
        df["score"] = pd.Series(dtype="float64")
        return df

    matriz = df[list(colunas)]

    df["anomalia"] = modelo.predict(matriz)
    df["score"] = modelo.decision_function(matriz)

    return df


# ETL ISA — EPISÓDIOS — agrupa anomalias por node e proximidade no tempo.
def _criar_episodio_interpretacao(hostname, registros, max_node):
    if len(registros) < MIN_REGISTROS:
        return None

    inicio = registros["TIMESTAMP"].iloc[0]
    fim = registros["TIMESTAMP"].iloc[-1]

    if max_node is None or pd.isna(max_node):
        status = "fechado"
    else:
        limite_aberto = max_node - timedelta(minutes=GAP_MINUTOS)

        status = (
            "aberto"
            if fim >= limite_aberto
            else "fechado"
        )

    return {
        "episodio_id": f"{hostname}|{inicio.isoformat()}",
        "hostname": str(hostname),
        "inicio": inicio,
        "fim": fim,
        "duracao_segundos": int(
            (fim - inicio).total_seconds()
        ),
        "quantidade_registros_anomalos": int(len(registros)),
        "status": status,
        "registros": registros.copy(),
    }


def consolidar_episodios_interpretacao(df):
    if df.empty or "anomalia" not in df.columns:
        return []

    max_por_node = (
        df.groupby("HOSTNAME")["TIMESTAMP"].max().to_dict()
    )

    episodios = []

    for hostname, grupo in df.groupby("HOSTNAME"):
        anomalos = grupo[
            grupo["anomalia"] == -1
        ].sort_values("TIMESTAMP")

        if anomalos.empty:
            continue

        max_node = max_por_node.get(hostname)
        inicio_bloco = 0

        for indice in range(1, len(anomalos)):
            diferenca = (
                anomalos.iloc[indice]["TIMESTAMP"]
                - anomalos.iloc[indice - 1]["TIMESTAMP"]
            )

            if diferenca > timedelta(minutes=GAP_MINUTOS):
                episodio = _criar_episodio_interpretacao(
                    hostname,
                    anomalos.iloc[inicio_bloco:indice],
                    max_node,
                )

                if episodio is not None:
                    episodios.append(episodio)

                inicio_bloco = indice

        episodio = _criar_episodio_interpretacao(
            hostname,
            anomalos.iloc[inicio_bloco:],
            max_node,
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


# ETL ISA — CATEGORIAS — calcula desvios e aplica as assinaturas originais.
def calcular_zscores_interpretacao(registros, baseline_node):
    mediana_baseline = baseline_node.get("mediana", {})
    mad_baseline = baseline_node.get("mad", {})

    resultado = {}

    for metrica in METRICAS_USO:
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
                _piso_mad_interpretacao(metrica),
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


def categorizar_episodio_interpretacao(zscores):
    nomes = {
        0: "Não classificado",
        1: "Pressão de processamento (CPU e Load)",
        2: "Pressão de memória (RAM, SWAP, IOWAIT)",
        3: "Ocupação de disco com SWAP",
        4: "Pressão combinada de CPU e RAM",
    }

    destaque = [
        metrica
        for metrica in METRICAS_USO
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


def _numero_interpretacao(valor):
    if valor is None:
        return None

    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(numero):
        return None

    return round(numero, 2)


# ETL ISA — RELATÓRIO — monta episódios, métricas de destaque e avisos.
def executar_interpretacao(df, modelo, colunas, baseline):
    df_validado, avisos = validar_compatibilidade_interpretacao(
        df,
        baseline,
    )

    df_detectado = detectar_anomalias_interpretacao(
        df_validado,
        modelo,
        colunas,
    )

    episodios = consolidar_episodios_interpretacao(
        df_detectado
    )

    episodios_json = []

    for episodio in episodios:
        hostname = str(episodio["hostname"])
        baseline_node = baseline.get(hostname, {})

        zscores = calcular_zscores_interpretacao(
            episodio["registros"],
            baseline_node,
        )

        categoria = categorizar_episodio_interpretacao(
            zscores
        )

        metricas_destaque = [
            {
                "metrica": item["metrica"],
                "z": _numero_interpretacao(item["z"]),
                "valor_mediano": _numero_interpretacao(
                    item["valor_mediano"]
                ),
                "normal_mediana": _numero_interpretacao(
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
                "status": episodio["status"],
                "quantidade_registros_anomalos": int(
                    episodio["quantidade_registros_anomalos"]
                ),
                "categoria": {
                    "codigo": int(categoria["codigo"]),
                    "nome": categoria["nome"],
                },
                "severidade_z": _numero_interpretacao(
                    categoria["severidade_z"]
                ),
                "cobertura_assinatura": _numero_interpretacao(
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
                FUSO_HORARIO
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


# ETL ISA — HISTÓRICO — mantém a união dos episódios entre execuções.
def _normalizar_data_episodio_interpretacao(valor):
    data = pd.Timestamp(valor)

    if data.tzinfo is None:
        data = data.tz_localize(FUSO_HORARIO)
    else:
        data = data.tz_convert(FUSO_HORARIO)

    return data


def _mesclar_episodios_interpretacao(historico, novos):
    resultado = list(historico)

    for novo in novos:
        inicio_novo = _normalizar_data_episodio_interpretacao(
            novo["inicio"]
        )

        fim_novo = _normalizar_data_episodio_interpretacao(
            novo["fim"]
        )

        correspondencia = None

        for indice, existente in enumerate(resultado):
            if existente["hostname"] != novo["hostname"]:
                continue

            inicio_existente = _normalizar_data_episodio_interpretacao(
                existente["inicio"]
            )

            fim_existente = _normalizar_data_episodio_interpretacao(
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
            _normalizar_data_episodio_interpretacao(
                existente["inicio"]
            ),
            inicio_novo,
        )

        fim_final = max(
            _normalizar_data_episodio_interpretacao(
                existente["fim"]
            ),
            fim_novo,
        )

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
        }

    resultado.sort(
        key=lambda episodio: (
            episodio["hostname"],
            episodio["inicio"],
        )
    )

    return resultado


def atualizar_historico_interpretacao(novos):
    historico = _ler_json_s3_interpretacao(
        CHAVE_HISTORICO,
        [],
    )

    if not isinstance(historico, list):
        raise ValueError("Histórico de episódios inválido.")

    atualizado = _mesclar_episodios_interpretacao(
        historico,
        novos,
    )

    _gravar_json_s3_interpretacao(
        CHAVE_HISTORICO,
        atualizado,
    )

    return atualizado


# ETL ISA — CONSOLIDAÇÃO — mantém o fechamento diário após 05h.
def _criar_consolidacao_diaria_interpretacao(historico, data):
    episodios_dia = []

    for episodio in historico:
        inicio = _normalizar_data_episodio_interpretacao(
            episodio["inicio"]
        )

        if inicio.date().isoformat() == data:
            episodios_dia.append(episodio)

    nodes = {}

    for episodio in episodios_dia:
        hostname = episodio["hostname"]
        categoria = episodio["categoria"]["nome"]

        if hostname not in nodes:
            nodes[hostname] = {}

        nodes[hostname][categoria] = (
            nodes[hostname].get(categoria, 0) + 1
        )

    resultado_nodes = []

    for hostname, categorias in sorted(nodes.items()):
        resultado_nodes.append(
            {
                "hostname": hostname,
                "total_episodios": sum(categorias.values()),
                "categorias": [
                    {
                        "nome": nome,
                        "quantidade_episodios": quantidade,
                    }
                    for nome, quantidade in sorted(
                        categorias.items(),
                        key=lambda item: (
                            -item[1],
                            item[0],
                        ),
                    )
                ],
            }
        )

    return {
        "data_referencia": data,
        "atualizado_em": datetime.now(
            FUSO_HORARIO
        ).isoformat(),
        "total_episodios": len(episodios_dia),
        "nodes": resultado_nodes,
    }


def processar_consolidacao_diaria_interpretacao(historico):
    estado = _ler_json_s3_interpretacao(
        CHAVE_CONSOLIDACAO,
        {
            "ultima_data_processada": None,
            "dias": {},
        },
    )

    agora = datetime.now(FUSO_HORARIO)

    if agora.hour < HORARIO_CONSOLIDACAO:
        return estado

    data_alvo = agora.date() - timedelta(days=1)
    ultima_data = estado.get("ultima_data_processada")

    if ultima_data is None:
        proxima_data = data_alvo
    else:
        proxima_data = (
            datetime.strptime(
                ultima_data,
                "%Y-%m-%d",
            ).date()
            + timedelta(days=1)
        )

    while proxima_data <= data_alvo:
        data_texto = proxima_data.isoformat()

        estado["dias"][data_texto] = (
            _criar_consolidacao_diaria_interpretacao(
                historico,
                data_texto,
            )
        )

        estado["ultima_data_processada"] = data_texto

        proxima_data += timedelta(days=1)

    _gravar_json_s3_interpretacao(
        CHAVE_CONSOLIDACAO,
        estado,
    )

    return estado


# ETL ISA — AGREGAÇÕES — categorias, nodes e eventos do mapa de calor.
def _agregar_categorias_interpretacao(historico):
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


def _agregar_nodes_interpretacao(historico):
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


def _criar_eventos_temporais_interpretacao(historico):
    eventos = []

    for episodio in historico:
        inicio = _normalizar_data_episodio_interpretacao(
            episodio["inicio"]
        )

        eventos.append(
            {
                "episodio_id": episodio["episodio_id"],
                "hostname": episodio["hostname"],
                "categoria": episodio["categoria"]["nome"],
                "data": inicio.date().isoformat(),
                "dia_semana": inicio.strftime("%A"),
                "dia_semana_numero": inicio.weekday(),
                "hora": inicio.hour,
                "quantidade": 1,
            }
        )

    return eventos


# ETL ISA — DASHBOARD — monta o contrato JSON original do analista.
def _criar_dashboard_interpretacao(historico, consolidacao):
    categorias = _agregar_categorias_interpretacao(
        historico
    )

    nodes = _agregar_nodes_interpretacao(
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

    eventos = _criar_eventos_temporais_interpretacao(
        historico
    )

    data_cards = consolidacao.get(
        "ultima_data_processada"
    )

    cards_diarios = (
        consolidacao.get("dias", {}).get(
            data_cards,
            {
                "data_referencia": data_cards,
                "atualizado_em": None,
                "total_episodios": 0,
                "nodes": [],
            },
        )
        if data_cards
        else {
            "data_referencia": None,
            "atualizado_em": None,
            "total_episodios": 0,
            "nodes": [],
        }
    )

    return {
        "schema_version": "2.0",
        "metadados": {
            "gerado_em": datetime.now(
                FUSO_HORARIO
            ).isoformat(),
            "fuso_horario": "America/Sao_Paulo",
            "total_episodios_historicos": total_episodios,
            "total_nodes": len(nodes),
            "total_categorias": len(categorias),
        },
        "indicadores": {
            "node_mais_problematico": node_mais_problematico,
            "categoria_mais_comum": categoria_mais_comum,
            "total_episodios": total_episodios,
        },
        "categorias": categorias,
        "nodes": nodes,
        "mapa_calor": {
            "eventos": eventos,
            "granularidades": [
                "data",
                "dia_semana",
                "hora",
            ],
        },
        "consolidacao_diaria": cards_diarios,
    }


# ETL ISA — PUBLICAÇÃO — executa o fluxo original e grava seus arquivos.
def processar_camada_gold_interpretacao():
    modelo, colunas, baseline = carregar_artefatos_interpretacao()

    df = carregar_dados_silver_interpretacao(
        colunas
    )

    relatorio = executar_interpretacao(
        df,
        modelo,
        colunas,
        baseline,
    )

    historico = atualizar_historico_interpretacao(
        relatorio["episodios"]
    )

    consolidacao = processar_consolidacao_diaria_interpretacao(
        historico
    )

    dashboard = _criar_dashboard_interpretacao(
        historico,
        consolidacao,
    )

    _gravar_json_s3_interpretacao(
        CHAVE_INTERPRETACAO_LATEST,
        relatorio,
    )

    _gravar_json_s3_interpretacao(
        CHAVE_DASHBOARD,
        dashboard,
    )

    metadados = relatorio["metadados"]

    print(
        "Interpretação: "
        f"{metadados['quantidade_registros']} registros, "
        f"{metadados['quantidade_anomalias']} anomalias, "
        f"{metadados['quantidade_episodios']} episódios."
    )

    print(
        f"Histórico: {len(historico)} episódios."
    )

    print(
        f"Dashboard atualizada: {CHAVE_DASHBOARD}"
    )

    print(
        "Última consolidação diária: "
        f"{consolidacao.get('ultima_data_processada')}"
    )

    if metadados["avisos"]:
        print(
            f"Avisos: {len(metadados['avisos'])}"
        )

    return dashboard


CHAVE_INTERPRETACAO_LATEST = (
    PREFIXO_INTERPRETACAO + "latest.json"
)


# ETL ISA — Execução pelo mesmo arquivo e pela mesma entrada Lambda.
def executar_analista():
    return processar_camada_gold_interpretacao()


# EXECUÇÃO CONJUNTA — Cada ETL mantém suas próprias leituras e publicações.
def executar_gold(dashboard=None):
    dashboard = dashboard or os.getenv("GOLD_DASHBOARD", "gestor")
    if dashboard == "gestor":
        return executar()
    if dashboard == "analista":
        return executar_analista()
    if dashboard == "ambas":
        return {"gestor": executar(), "analista": executar_analista()}
    raise ValueError("GOLD_DASHBOARD deve ser gestor, analista ou ambas.")


def lambda_handler(event, context):
    """Mantém gestor como padrão; ambas precisa das configurações das duas ETLs."""
    dashboard = os.getenv("GOLD_DASHBOARD", "gestor")
    if dashboard in ("gestor", "ambas"):
        validar_configuracao_gestor()
    return executar_gold(dashboard)


if __name__ == "__main__":
    executar_gold()
