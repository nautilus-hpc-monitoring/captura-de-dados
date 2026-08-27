import csv
import exibicao as ex
from datetime import datetime

cabecalho = [
    'TIMESTAMP',
    'USUARIO',

    'CPU_PERCENT',
    'CPU_USER_PERCENT',
    'CPU_SYSTEM_PERCENT',
    'CPU_IDLE_PERCENT',
    'CPU_IOWAIT_PERCENT',

    'CPU_INTERRUPTS',
    
    'CPU_FREQ_ATUAL',

    'LOAD_AVG_1',
    'LOAD_AVG_5',
    'LOAD_AVG_15',

    'RAM_PERCENT',
    'RAM_TOTAL',
    'RAM_AVAILABLE',
    'RAM_USED',
    'RAM_LIVRE',

    'SWAP_PERCENT',
    'SWAP_USADA',
    'SWAP_LIVRE',
    'SWAP_IN',
    'SWAP_OUT',

    'DISCO_PERCENT',
    'DISCO_USADO',
    'DISCO_LIVRE'
]

# Funções gerais
def media_interv_min(lista: list, minutos: int, key: str) -> float:
    soma = 0
    cont = 0

    ult_data = datetime.strptime(
        lista[-1]["TIMESTAMP"],
        '%Y-%m-%d %H:%M:%S'
    )

    for registro in reversed(lista):
        data = datetime.strptime(
            registro["TIMESTAMP"],
            '%Y-%m-%d %H:%M:%S'
        )

        diferenca = (
            ult_data - data
        ).total_seconds()

        if diferenca > minutos * 60:
            break

        soma += float(registro[key])
        cont += 1

    return soma / cont if cont != 0 else 0


def media_uso_componente(lista: list, key: str) -> float:
    soma = 0
    cont = 0

    for registro in lista:
        soma += float(registro[key])
        cont += 1

    return soma / cont if cont != 0 else 0


# CPU
def media_cpu_geral(lista: list):
    return media_uso_componente(
        lista,
        "CPU_PERCENT"
    )


def media_cpu_30_min(lista: list):
    return media_interv_min(
        lista,
        30,
        "CPU_PERCENT"
    )


def media_cpu_1_dia(lista: list):
    return media_interv_min(
        lista,
        1440,
        "CPU_PERCENT"
    )


def media_cpu_user(lista: list):
    return media_uso_componente(
        lista,
        "CPU_USER_PERCENT"
    )


def media_cpu_system(lista: list):
    return media_uso_componente(
        lista,
        "CPU_SYSTEM_PERCENT"
    )


def media_cpu_idle(lista: list):
    return media_uso_componente(
        lista,
        "CPU_IDLE_PERCENT"
    )


def media_cpu_iowait(lista: list):
    return media_uso_componente(
        lista,
        "CPU_IOWAIT_PERCENT"
    )


def interrupcoes_por_segundo(lista: list):
    if len(lista) < 2:
        return 0

    interrupcoes_inicio = float(
        lista[0]["CPU_INTERRUPTS"]
    )

    interrupcoes_fim = float(
        lista[-1]["CPU_INTERRUPTS"]
    )

    data_inicio = datetime.strptime(
        lista[0]["TIMESTAMP"],
        '%Y-%m-%d %H:%M:%S'
    )

    data_fim = datetime.strptime(
        lista[-1]["TIMESTAMP"],
        '%Y-%m-%d %H:%M:%S'
    )

    segundos = (
        data_fim - data_inicio
    ).total_seconds()

    if segundos == 0:
        return 0

    return (
        interrupcoes_fim - interrupcoes_inicio
    ) / segundos


def media_frequencia_cpu(lista: list):
    return media_uso_componente(
        lista,
        "CPU_FREQ_ATUAL"
    )


def media_load_1(lista: list):
    return media_uso_componente(
        lista,
        "LOAD_AVG_1"
    )


def media_load_5(lista: list):
    return media_uso_componente(
        lista,
        "LOAD_AVG_5"
    )


def media_load_15(lista: list):
    return media_uso_componente(
        lista,
        "LOAD_AVG_15"
    )


# RAM
def media_ram_percent(lista: list):
    return media_uso_componente(
        lista,
        "RAM_PERCENT"
    )


def media_ram_disponivel(lista: list):
    return media_uso_componente(
        lista,
        "RAM_AVAILABLE"
    )


def media_ram_usada(lista: list):
    return media_uso_componente(
        lista,
        "RAM_USED"
    )


# SWAP
def media_swap_percent(lista: list):
    return media_uso_componente(
        lista,
        "SWAP_PERCENT"
    )


def swap_in_por_segundo(lista: list):
    if len(lista) < 2:
        return 0

    swap_inicio = float(
        lista[0]["SWAP_IN"]
    )

    swap_fim = float(
        lista[-1]["SWAP_IN"]
    )

    data_inicio = datetime.strptime(
        lista[0]["TIMESTAMP"],
        '%Y-%m-%d %H:%M:%S'
    )

    data_fim = datetime.strptime(
        lista[-1]["TIMESTAMP"],
        '%Y-%m-%d %H:%M:%S'
    )

    segundos = (
        data_fim - data_inicio
    ).total_seconds()

    if segundos == 0:
        return 0

    return (
        swap_fim - swap_inicio
    ) / segundos


def swap_out_por_segundo(lista: list):
    if len(lista) < 2:
        return 0

    swap_inicio = float(
        lista[0]["SWAP_OUT"]
    )

    swap_fim = float(
        lista[-1]["SWAP_OUT"]
    )

    data_inicio = datetime.strptime(
        lista[0]["TIMESTAMP"],
        '%Y-%m-%d %H:%M:%S'
    )

    data_fim = datetime.strptime(
        lista[-1]["TIMESTAMP"],
        '%Y-%m-%d %H:%M:%S'
    )

    segundos = (
        data_fim - data_inicio
    ).total_seconds()

    if segundos == 0:
        return 0

    return (
        swap_fim - swap_inicio
    ) / segundos


# Disco
def media_disco_percent(lista: list):
    return media_uso_componente(
        lista,
        "DISCO_PERCENT"
    )


def media_disco_livre(lista: list):
    return media_uso_componente(
        lista,
        "DISCO_LIVRE"
    )


def media_disco_usado(lista: list):
    return media_uso_componente(
        lista,
        "DISCO_USADO"
    )


def crescimento_espaco_disco(lista: list):
    if len(lista) < 2:
        return 0

    usado_inicio = float(
        lista[0]["DISCO_USADO"]
    )

    usado_fim = float(
        lista[-1]["DISCO_USADO"]
    )

    if usado_inicio == 0:
        return 0

    return (
        (usado_fim - usado_inicio)
        * 100
        / usado_inicio
    )


# Leitura do CSV
def ler_csv(nome_arquivo: str) -> list:
    dados = []

    with open(nome_arquivo, 'r', encoding='utf-8') as arquivo:
        leitor = csv.reader(
            arquivo,
            delimiter=';'
        )

        for linha in leitor:
            if len(linha) != len(cabecalho):
                continue

            registro = {}

            for i in range(len(cabecalho)):
                registro[cabecalho[i]] = linha[i]

            dados.append(registro)

    return dados


def main():
    dados = ler_csv('raphael.csv')

    if len(dados) == 0:
        print("Nenhum dado encontrado.")
        return

    ex.titulo("RELATÓRIO DE MONITORAMENTO")

    print(f"Registros: {len(dados)}")
    print(f"Início: {dados[0]['TIMESTAMP']}")
    print(f"Fim:    {dados[-1]['TIMESTAMP']}")

    # CPU
    ex.titulo("CPU")

    ex.resultado(
        "Uso médio geral:",
        media_cpu_geral(dados),
        "%"
    )

    ex.resultado(
        "Uso médio últimos 30 min:",
        media_cpu_30_min(dados),
        "%"
    )

    ex.resultado(
        "Uso médio último dia:",
        media_cpu_1_dia(dados),
        "%"
    )

    ex.resultado(
        "CPU em user:",
        media_cpu_user(dados),
        "%"
    )

    ex.resultado(
        "CPU em system:",
        media_cpu_system(dados),
        "%"
    )

    ex.resultado(
        "CPU ociosa:",
        media_cpu_idle(dados),
        "%"
    )

    ex.resultado(
        "CPU aguardando I/O:",
        media_cpu_iowait(dados),
        "%"
    )

    ex.resultado(
        "Interrupções por segundo:",
        interrupcoes_por_segundo(dados),
        "int/s"
    )

    ex.resultado(
        "Frequência média:",
        media_frequencia_cpu(dados),
        "MHz"
    )

    ex.resultado(
        "Load Average 1 min:",
        media_load_1(dados)
    )

    ex.resultado(
        "Load Average 5 min:",
        media_load_5(dados)
    )

    ex.resultado(
        "Load Average 15 min:",
        media_load_15(dados)
    )

    # RAM
    ex.titulo("RAM")

    ex.resultado(
        "Uso médio:",
        media_ram_percent(dados),
        "%"
    )

    ex.resultado(
        "RAM disponível média:",
        media_ram_disponivel(dados),
        "bytes"
    )

    ex.resultado(
        "RAM usada média:",
        media_ram_usada(dados),
        "bytes"
    )

    # SWAP
    ex.titulo("SWAP")

    ex.resultado(
        "Uso médio:",
        media_swap_percent(dados),
        "%"
    )

    ex.resultado(
        "Swap IN:",
        swap_in_por_segundo(dados),
        "bytes/s"
    )

    ex.resultado(
        "Swap OUT:",
        swap_out_por_segundo(dados),
        "bytes/s"
    )

    # DISCO
    ex.titulo("DISCO")

    ex.resultado(
        "Uso médio:",
        media_disco_percent(dados),
        "%"
    )

    ex.resultado(
        "Espaço usado médio:",
        media_disco_usado(dados),
        "bytes"
    )

    ex.resultado(
        "Espaço livre médio:",
        media_disco_livre(dados),
        "bytes"
    )

    ex.resultado(
        "Crescimento:",
        crescimento_espaco_disco(dados),
        "%"
    )

    ex.titulo("FIM")


main()
