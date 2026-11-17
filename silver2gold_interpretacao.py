import json
import math
import os
import tempfile
from datetime import datetime, timezone, timedelta

import joblib
import pandas as pd


def _env_int_interpretacao(nome, padrao):
    valor = os.getenv(nome)
    return int(valor) if valor not in (None, "") else padrao


def _env_float_interpretacao(nome, padrao):
    valor = os.getenv(nome)
    return float(valor) if valor not in (None, "") else padrao


MODELO_VERSAO = os.getenv("MODELO_VERSAO", "v1")
HORAS_JANELA = _env_int_interpretacao("HORAS_JANELA", 12)
GAP_MINUTOS = _env_int_interpretacao("GAP_MINUTOS", 10)
MIN_REGISTROS = _env_int_interpretacao("MIN_REGISTROS", 6)
LIMIAR_Z = _env_float_interpretacao("LIMIAR_Z", 3.0)

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


def carregar_artefatos_interpretacao():
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


def carregar_dados_silver_interpretacao(colunas):
    from config_aws import s3_client

    bucket = os.getenv("BUCKET_NAME")
    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

    limite = datetime.now(timezone.utc) - timedelta(hours=2 * HORAS_JANELA)
    paginator = s3_client.get_paginator("list_objects_v2")
    chaves = []

    for pagina in paginator.paginate(Bucket=bucket, Prefix="02-silver/"):
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
        resposta = s3_client.get_object(Bucket=bucket, Key=chave)
        dataframes.append(pd.read_csv(resposta["Body"], sep=";"))

    df = pd.concat(dataframes, ignore_index=True)

    faltantes = [coluna for coluna in obrigatorias if coluna not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes nos CSVs da Silver: {faltantes}")

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
    for coluna in colunas:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    df = df.dropna(subset=obrigatorias)
    df = df.drop_duplicates(subset=["HOSTNAME", "TIMESTAMP"], keep="first")
    df["HOSTNAME"] = df["HOSTNAME"].astype("string")

    if df.empty:
        return df.sort_values(["HOSTNAME", "TIMESTAMP"]).reset_index(drop=True)

    limite_dados = df["TIMESTAMP"].max() - timedelta(hours=HORAS_JANELA)
    df = df[df["TIMESTAMP"] > limite_dados]

    return df.sort_values(["HOSTNAME", "TIMESTAMP"]).reset_index(drop=True)


def validar_compatibilidade_interpretacao(df, baseline):
    avisos = []

    if df.empty:
        return df.copy(), avisos

    hosts = df["HOSTNAME"].astype(str).unique()
    fora_do_baseline = sorted(host for host in hosts if host not in baseline)
    for host in fora_do_baseline:
        avisos.append(f"Node sem chave no baseline: {host}")

    df = df[df["HOSTNAME"].astype(str).isin(baseline.keys())].copy()
    if df.empty:
        return df, avisos

    colunas_presentes = [
        coluna for coluna in COLUNAS_CONSTANTES if coluna in df.columns
    ]
    for coluna in COLUNAS_CONSTANTES:
        if coluna not in df.columns:
            avisos.append(f"Coluna constante ausente nos dados: {coluna}")

    for host, grupo in df.groupby("HOSTNAME"):
        baseline_node = baseline.get(str(host))
        if not baseline_node:
            continue

        mediana_baseline = baseline_node.get("mediana", {})
        for coluna in colunas_presentes:
            if coluna not in mediana_baseline:
                avisos.append(f"Coluna constante ausente no baseline: {coluna}")
                continue

            valores = grupo[coluna].dropna()
            if valores.empty:
                continue

            atual = float(valores.median())
            base = float(mediana_baseline[coluna])

            if base == 0:
                divergente = abs(atual - base) > 1e-9
            else:
                divergente = abs(atual - base) / abs(base) > 0.01

            if divergente:
                avisos.append(
                    f"Mediana de {coluna} divergente para {host}: "
                    f"atual={atual:.2f}, baseline={base:.2f}"
                )

    return df.reset_index(drop=True), avisos


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


def _criar_episodio_interpretacao(hostname, registros, max_node):
    if len(registros) < MIN_REGISTROS:
        return None

    inicio = registros["TIMESTAMP"].iloc[0]
    fim = registros["TIMESTAMP"].iloc[-1]

    if max_node is None or pd.isna(max_node):
        status = "fechado"
    else:
        limite_aberto = max_node - timedelta(minutes=GAP_MINUTOS)
        status = "aberto" if fim >= limite_aberto else "fechado"

    return {
        "episodio_id": f"{hostname}|{inicio.isoformat()}",
        "hostname": str(hostname),
        "inicio": inicio,
        "fim": fim,
        "duracao_segundos": int((fim - inicio).total_seconds()),
        "quantidade_registros_anomalos": int(len(registros)),
        "status": status,
        "registros": registros.copy(),
    }


def consolidar_episodios_interpretacao(df):
    if df.empty or "anomalia" not in df.columns:
        return []

    max_por_node = df.groupby("HOSTNAME")["TIMESTAMP"].max().to_dict()
    episodios = []

    for hostname, grupo in df.groupby("HOSTNAME"):
        anomalos = grupo[grupo["anomalia"] == -1].sort_values("TIMESTAMP")
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
        key=lambda episodio: (episodio["hostname"], episodio["inicio"]),
    )


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

        valores = pd.to_numeric(registros[metrica], errors="coerce").dropna()
        if valores.empty:
            resultado[metrica] = {
                "z": None,
                "valor_mediano": None,
                "normal_mediana": float(mediana_baseline[metrica]),
            }
            continue

        mediana = float(mediana_baseline[metrica])
        mad = float(mad_baseline[metrica])
        denominador = 1.4826 * max(mad, _piso_mad_interpretacao(metrica))

        z_serie = (valores - mediana) / denominador
        z_mediana = float(z_serie.median())
        valor_mediano = float(valores.median())

        resultado[metrica] = {
            "z": z_mediana if math.isfinite(z_mediana) else None,
            "valor_mediano": (
                valor_mediano if math.isfinite(valor_mediano) else None
            ),
            "normal_mediana": mediana if math.isfinite(mediana) else None,
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
    destaque.sort(key=lambda metrica: zscores[metrica]["z"], reverse=True)

    if "DISCO_PERCENT" in destaque:
        codigo = 3
        assinatura = ["DISCO_PERCENT", "SWAP_PERCENT"]
    elif "RAM_PERCENT" in destaque and (
        "SWAP_PERCENT" in destaque or "CPU_IOWAIT_PERCENT" in destaque
    ):
        codigo = 2
        assinatura = ["RAM_PERCENT", "SWAP_PERCENT", "CPU_IOWAIT_PERCENT"]
    elif "RAM_PERCENT" in destaque and "CPU_PERCENT" in destaque:
        codigo = 4
        assinatura = ["CPU_PERCENT", "RAM_PERCENT"]
    elif "CPU_PERCENT" in destaque and "LOAD_AVG_1" in destaque:
        codigo = 1
        assinatura = ["CPU_PERCENT", "LOAD_AVG_1"]
    else:
        codigo = 0
        assinatura = []

    severidade_z = max(
        (zscores[metrica]["z"] for metrica in destaque),
        default=0.0,
    )

    if codigo == 0:
        cobertura = 0.0
    else:
        cobertura = len(set(assinatura) & set(destaque)) / len(assinatura)

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


def executar_interpretacao(df, modelo, colunas, baseline):
    df_validado, avisos = validar_compatibilidade_interpretacao(df, baseline)
    df_detectado = detectar_anomalias_interpretacao(
        df_validado, modelo, colunas
    )
    episodios = consolidar_episodios_interpretacao(df_detectado)

    episodios_json = []
    for episodio in episodios:
        hostname = str(episodio["hostname"])
        baseline_node = baseline.get(hostname, {})
        zscores = calcular_zscores_interpretacao(
            episodio["registros"], baseline_node
        )
        categoria = categorizar_episodio_interpretacao(zscores)

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
                "duracao_segundos": int(episodio["duracao_segundos"]),
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

    if df_detectado.empty or "anomalia" not in df_detectado.columns:
        quantidade_anomalias = 0
    else:
        quantidade_anomalias = int(
            (df_detectado["anomalia"] == -1).sum()
        )

    relatorio = {
        "schema_version": "1.0",
        "metadados": {
            "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
            "modelo_versao": MODELO_VERSAO,
            "janela_horas": int(HORAS_JANELA),
            "quantidade_registros": int(len(df_detectado)),
            "quantidade_anomalias": quantidade_anomalias,
            "quantidade_episodios": len(episodios_json),
            "avisos": avisos,
        },
        "episodios": episodios_json,
    }

    return relatorio


def gravar_json_interpretacao(relatorio):
    from config_aws import s3_client

    bucket = os.getenv("BUCKET_NAME")
    if not bucket:
        raise ValueError("A variável BUCKET_NAME não foi configurada.")

    conteudo = json.dumps(
        relatorio,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    chave = f"03-gold/interpretacao/interpretacao_{timestamp}.json"

    s3_client.put_object(
        Bucket=bucket,
        Key=chave,
        Body=conteudo,
        ContentType="application/json; charset=utf-8",
    )
    s3_client.put_object(
        Bucket=bucket,
        Key="03-gold/interpretacao/latest.json",
        Body=conteudo,
        ContentType="application/json; charset=utf-8",
    )

    return chave


def processar_camada_gold_interpretacao():
    modelo, colunas, baseline = carregar_artefatos_interpretacao()
    df = carregar_dados_silver_interpretacao(colunas)
    relatorio = executar_interpretacao(df, modelo, colunas, baseline)
    chave = gravar_json_interpretacao(relatorio)

    metadados = relatorio["metadados"]
    print(
        "Interpretação: "
        f"{metadados['quantidade_registros']} registros, "
        f"{metadados['quantidade_anomalias']} anomalias, "
        f"{metadados['quantidade_episodios']} episódios."
    )
    if metadados["avisos"]:
        print(f"Avisos: {len(metadados['avisos'])}")
    print(f"JSON enviado para: {chave}")

    return relatorio
