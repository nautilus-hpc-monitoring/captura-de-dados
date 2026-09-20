import requests
import psutil as p
import os
import time
import csv
import datetime
import socket

start_msg = '''
=========================================================================
                REALIZE SUA AUTENTICAÇÃO PARA PROSSEGUIR!
=========================================================================
'''

print(start_msg)

email = input('Email: ')
password = input('Senha: ')

host = socket.gethostname()
url_auth = "http://localhost:3000/api/autenticacao"

flags_monitoramento = {}

def authComponentes(componentes):
    for componente in componentes:
        flags_monitoramento[componente['type']] = True

def coletar_cpu():
    return {
        "percent": p.cpu_percent(),
        "times": p.cpu_times_percent(),
        "stats": p.cpu_stats(),
        "freq": p.cpu_freq(),
        "count": p.cpu_count(),
        "load": p.getloadavg()
    }

cabecalho = [
    'TIMESTAMP',
    'USER',

    'CPU_PERCENT',
    'CPU_USER_PERCENT',
    'CPU_SYSTEM_PERCENT',
    'CPU_IDLE_PERCENT',
    'CPU_IOWAIT_PERCENT',
    'CPU_FREQ_ATUAL',
    'CPU_COUNT_LOGICA',
    'LOAD_AVG_1',
    'LOAD_AVG_5',
    'LOAD_AVG_15',

    'RAM_PERCENT',
    'RAM_TOTAL',
    'RAM_AVAILABLE',
    'RAM_USED',

    'SWAP_PERCENT',
    'SWAP_USED',
    'SWAP_FREE',
    'SWAP_IN',
    'SWAP_OUT',

    'DISCO_PERCENT',
    'DISCO_TOTAL',
    'DISCO_USED',
    'DISCO_FREE'
]
usuario = os.environ.get('USER')

def escrita():
    with open('./data.csv', 'w', newline='') as csvfile:
        csv.writer(csvfile, delimiter=';').writerow(cabecalho)

    while True:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if flags_monitoramento.get('CPU', False):
            cpu = coletar_cpu()
        else:
            cpu = None

        if flags_monitoramento.get('RAM', False):
            ram = p.virtual_memory()
        else:
            ram = 0

        if flags_monitoramento.get('SWAP', False):
            swap = p.swap_memory()
            
        else:
            swap = 0

        if flags_monitoramento.get('DISCO', False):
            disco = p.disk_usage('/')
        else: 
            disco = 0
            
        dados = [
            timestamp,
            usuario,

        cpu["percent"],
        cpu["times"].user,
        cpu["times"].system,
        cpu["times"].idle,
        cpu["times"].iowait,
        cpu["freq"].current,
        cpu["count"],
        cpu["load"][0],
        cpu["load"][1],
        cpu["load"][2],

        ram.percent,
        ram.total,
        ram.available,
        ram.used,

        swap.percent,
        swap.used,
        swap.free,
        swap.sin,
        swap.sout,

        disco.percent,
        disco.total,
        disco.used,
        disco.free
    ]

        with open('./data.csv', 'a', newline='') as csvfile:
            csv.writer(csvfile, delimiter=';').writerow(dados)

        time.sleep(10)

try:
    res = requests.post(
        url_auth,
        json={
            "email": email,
            "senha": password,
            "hostname": host
        }
    )
    res.raise_for_status()

    resultado = res.json()
    print(resultado)

    if resultado.get("autenticado"):
        print("\nAutenticação realizada com sucesso!")
        print("Hostname:", resultado["mainframe"]["hostname"])

        componentes = resultado["componentes\n"]
        authComponentes(componentes)

        escrita()

    else:
        print("Falha na autenticação! Email e/ou senha incorretos")

except requests.exceptions.HTTPError as erro:
    print("Erro na autenticação:", erro)

except requests.exceptions.RequestException as erro:
    print("Não foi possível conectar à API:", erro)

