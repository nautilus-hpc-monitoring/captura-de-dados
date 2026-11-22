"""ETL do analista: lê os CSVs da camada Silver e publica o JSON histórico na Gold.

MAPA DO ARQUIVO (leia nesta ordem)
 1. Configuração: variáveis de ambiente e constantes do cálculo.
 2. Funções pequenas e reutilizáveis: hosts, datas, horas e limpeza de JSON.
 3. Acumuladores: a "memória" que guarda os números de cada node enquanto os
    arquivos são lidos. Isso evita carregar 6 meses de dados de uma vez na RAM.
 4. Leitura da Silver: S3 (produção) ou pasta local (teste sem AWS).
 5. Cálculos: semanas, percentis, picos, regressão, anomalias e correlação.
 6. Montagem do JSON, publicação na Gold e entradas de execução.

QUEM CONSOME ESTE ARQUIVO
    public/dashboard-analista.html + public/js/dashboard-analista.js, via
    GET /dashanalista/clusters/:id/dados (src/controllers/dashAnalistaController.js).

O QUE ESTA ETL **NÃO** FAZ
    - Não coleta dados (isso é local2bronze.py) e não limpa a Bronze (bronze2silver.py).
    - Não calcula alertas nem SLA (isso vem do MySQL no site).
    - Não coleta IOPS: a pipeline guarda apenas o percentual de uso do disco
      (DISCO_PERCENT) e o tempo de espera de I/O da CPU (CPU_IOWAIT_PERCENT).
      Por isso a correlação usa esses dois campos, e não IOPS x espera.

EXECUÇÃO
    Local (com AWS):     $env:MODO_LOCAL="0"; $env:BUCKET_NAME="..."; python silver2goldAnalista.py
    Local (sem AWS):     $env:MODO_LOCAL="1"; python silver2goldAnalista.py
    Lambda:              silver2goldAnalista.lambda_handler(event, context)

    No modo local, SILVER_LOCAL_DIR e GOLD_LOCAL_DIR podem ser relativos: nesse
    caso são resolvidos a partir da pasta deste arquivo (captura-de-dados).
"""

# ---------------------------------------------------------------------------
# 1. IMPORTS E CONFIGURAÇÃO
# ---------------------------------------------------------------------------
# "import" traz para o arquivo código que já existe em outro lugar.
# Cada linha abaixo habilita um recurso que será usado mais adiante.
import io            # transforma o fluxo de bytes do S3 em texto legível (CSV).
import json          # converte dicionários Python em texto JSON e vice-versa.
import math          # funções matemáticas (isfinite, ceil).
import os            # lê variáveis de ambiente e monta caminhos de arquivo.
from pathlib import Path  # lista arquivos de uma pasta no modo local.

import boto3         # SDK oficial da AWS: é quem fala com o S3.
import numpy as np   # arrays e matemática vetorial (usado por pandas).
import pandas as pd  # tabelas: ler CSV, filtrar linhas, agrupar, resumir.
from botocore.exceptions import ClientError  # erros devolvidos pela AWS.
from dotenv import load_dotenv  # carrega o arquivo .env para dentro do os.environ.

# load_dotenv() lê o arquivo ".env" que fica AO LADO deste script e
# disponibiliza as variáveis para os.getenv(...). Assim o comando funciona
# mesmo se você executar a ETL da raiz do projeto. Se o arquivo não existir,
# nada acontece (não dá erro) e as variáveis do shell continuam valendo.
load_dotenv(Path(__file__).with_name(".env"))

FUSO = "America/Sao_Paulo"  # fuso usado em todas as datas publicadas.

# os.getenv("NOME", "padrao") devolve o valor da variável de ambiente ou o
# padrão informado. Assim o script funciona sem configuração obrigatória.
ORIGEM = os.getenv("GESTOR_FUSO_ORIGEM", FUSO)          # fuso em que os CSVs foram gravados.
SILVER = os.getenv("SILVER_PREFIX", "02-silver/")        # pasta de origem no bucket.
GOLD = os.getenv("S3_KEY", "03-gold/analista").rstrip("/")  # pasta de destino.
PASSO = int(os.getenv("COLETA_SEGUNDOS", "10"))          # segundos entre duas amostras.
COBERTURA = float(os.getenv("COBERTURA_MINIMA", "0.8"))  # fração mínima de amostras por semana.

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
COLUNAS = [
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
def buscar_hosts(cluster_id):
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

    for coluna in COLUNAS:
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


def executar():
    """Fluxo principal da ETL."""
    # Validações de configuração antes de qualquer trabalho pesado.
    if not 0 < PASSO <= 300 or 3600 % PASSO:
        raise ValueError("COLETA_SEGUNDOS deve estar entre 1 e 300 e dividir 3600.")
    if not 0 < COBERTURA <= 1:
        raise ValueError("COBERTURA_MINIMA deve estar entre 0 e 1.")
    if SEMANAS <= 0:
        raise ValueError("ANALISTA_SEMANAS deve ser maior que zero.")

    # No modo local, o cluster pode ser informado ou ficar em 0.
    cluster_id = inteiro_env("ANALISTA_CLUSTER_ID", inteiro_env("GESTOR_CLUSTER_ID", 0))
    if not MODO_LOCAL and cluster_id <= 0:
        raise ValueError("ANALISTA_CLUSTER_ID (ou GESTOR_CLUSTER_ID) inválido.")

    hosts = buscar_hosts(cluster_id)
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
        destino = (s3, bucket, f"{GOLD}/cluster={cluster_id}/dashboard.json")

    publicado = gravar_json(destino, painel)

    print(f"Gold publicada: {publicado} | {len(nodes)} nodes com telemetria")
    return {"destino": publicado, "nodes": len(nodes), "linhas": lidos}


def lambda_handler(event, context):
    """Entrada usada quando a ETL roda como função Lambda."""
    if MODO_LOCAL:
        return executar()

    # Verifica as variáveis obrigatórias antes de tentar acessar a AWS.
    for variavel in ("BUCKET_NAME",):
        if not os.getenv(variavel):
            raise ValueError(f"Configure a variável {variavel} na Lambda.")

    if not os.getenv("ANALISTA_CLUSTER_ID") and not os.getenv("GESTOR_CLUSTER_ID"):
        raise ValueError("Configure ANALISTA_CLUSTER_ID (ou GESTOR_CLUSTER_ID).")

    if not os.getenv("DB_HOST") and not os.getenv("HOSTNAMES"):
        raise ValueError("Configure HOSTNAMES (ou as variáveis do MySQL).")

    return executar()


# Este bloco só roda quando o arquivo é executado direto (python arquivo.py).
# Quando outro script importa este módulo, nada aqui é executado.
if __name__ == "__main__":
    executar()
