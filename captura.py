import requests
import psutil as p
import os
import time
import csv
import datetime
import socket

email = input('Email: ')
password = input('Senha: ')
host = socket.gethostname()
url_auth = "http://localhost:3000/api/autenticacao"

flags_monitoramento = {}
dados = []

def authComponentes(componentes):
    for componente in componentes:
        flags_monitoramento[componente['type']] = True

cabecalho = [
    'TIMESTAMP',
    'USER',

    'CPU_PERCENT',
    'CPU_USER_PERCENT',
    'CPU_NICE_PERCENT',
    'CPU_SYSTEM_PERCENT',
    'CPU_IDLE_PERCENT',
    'CPU_IOWAIT_PERCENT',
    'CPU_IRQ_PERCENT',
    'CPU_SOFTIRQ_PERCENT',
    'CPU_STEAL_PERCENT',
    'CPU_GUEST_PERCENT',
    'CPU_GUEST_NICE_PERCENT',

    'CPU_FREQ_ATUAL',
    'CPU_FREQ_MIN',
    'CPU_FREQ_MAX',

    'CPU_COUNT_LOGICA',
    'CPU_COUNT_FISICA',

    'CPU_CTX_SWITCHES',
    'CPU_INTERRUPTS',
    'CPU_SOFT_INTERRUPTS',
    'CPU_SYSCALLS',

    'LOAD_AVG_1',
    'LOAD_AVG_5',
    'LOAD_AVG_15',

    'RAM_TOTAL',
    'RAM_AVAILABLE',
    'RAM_PERCENT',
    'RAM_USED',
    'RAM_FREE',
    'RAM_ACTIVE',
    'RAM_INACTIVE',
    'RAM_BUFFERS',
    'RAM_CACHED',
    'RAM_SHARED',
    'RAM_SLAB',

    'SWAP_TOTAL',
    'SWAP_USED',
    'SWAP_FREE',
    'SWAP_PERCENT',
    'SWAP_IN',
    'SWAP_OUT',

    'DISCO_TOTAL',
    'DISCO_USED',
    'DISCO_FREE',
    'DISCO_PERCENT',

    'DISCO_READ_COUNT',
    'DISCO_WRITE_COUNT',
    'DISCO_READ_BYTES',
    'DISCO_WRITE_BYTES',
    'DISCO_READ_TIME',
    'DISCO_WRITE_TIME',
    'DISCO_READ_MERGED_COUNT',
    'DISCO_WRITE_MERGED_COUNT',
    'DISCO_BUSY_TIME'
]
usuario = os.environ.get('USER')


def escrita():
    with open('./data.csv', 'w', newline='') as csvfile:
        csv.writer(csvfile, delimiter=';').writerow(cabecalho)

    while True:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cpu_percent = p.cpu_percent()
        cpu_times = p.cpu_times_percent()
        cpu_stats = p.cpu_stats()
        cpu_freq = p.cpu_freq()

        load = p.getloadavg()

        ram = p.virtual_memory()
        swap = p.swap_memory()
        disco = p.disk_usage('/')

        dados = [
            timestamp,
            usuario,

            cpu_percent,
            cpu_times.user,
            cpu_times.system,
            cpu_times.idle,
            cpu_times.iowait,

            cpu_stats.interrupts,

            cpu_freq.current,

            load[0],
            load[1],
            load[2],

            ram.percent,
            ram.total,
            ram.available,
            ram.used,
            ram.free,

            swap.percent,
            swap.used,
            swap.free,
            swap.sin,
            swap.sout,

            disco.percent,
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

        print("Autenticação realizada com sucesso!")
        print("Hostname:", resultado["mainframe"]["hostname"])

        componentes = resultado["componentes"]

        authComponentes(componentes)

        #escrita()

    else:
        print("Falha na autenticação.")

except requests.exceptions.HTTPError as erro:

    print("Erro na autenticação:", erro)

except requests.exceptions.RequestException as erro:

    print("Não foi possível conectar à API:", erro)

