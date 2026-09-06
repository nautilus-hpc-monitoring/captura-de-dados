import psutil as p
import csv
import time
import os
import requests # add pra comunicação com a api
import socket # add pra pegar o nome do pc
from datetime import datetime

# Configurar as variáveis de ambiente pra API pra cahamar commais facilidade posteriormente
API_BASE_URL = "http://localhost:3333/empresa"
# Diz qual o id da empresa correspondente ao node ou cluster
ID_EMPRESA = 1

def buscar_parametros_cliente(id_empresa):
    # Usa a nossa API pra pegar os "comando_parametro" que foram cadastrados pra empresa
    try:
        resposta = requests.get(f"{API_BASE_URL}/{id_empresa}")
        if resposta.status_code == 200:
            dados = resposta.json()
            # Seleciona os parametros que vieram do model da empresa
            parametros = [item['comando_parametro'] for item in dados if 'comando_parametro' in item]
            print(f"Parâmetros ativos carregados da API: {parametros}")
            return parametros
        else:
            print(f"Houve um erro ao consultar a nossa API: ({resposta.status_code}). Utilizando captura padrão.")
            return []
    except Exception as e:
        print(f"Falha de conexão com a nossa API: {e}")
        return []

# Pega a lista de filtros da API quando coomeça
parametros_ativos = buscar_parametros_cliente(ID_EMPRESA)

usuario = os.environ.get('USER')

while True:
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Esse dicionário dinâmico já guarda os campos de forma padronizada
    registro = {
        'TIMESTAMP': timestamp,
        'USUARIO': usuario
    }

    # Filtro da CPU
    # Se tiver alguma dessas colunas em maiúsculo...
    if any(param in parametros_ativos for param in ['CPU_PERCENT', 'CPU_USER_PERCENT', 'CPU_SYSTEM_PERCENT', 'CPU_IDLE_PERCENT', 'CPU_IOWAIT_PERCENT']):

        # Faz a captura pra cada parâmetro
        if 'CPU_PERCENT' in parametros_ativos:
            registro['CPU_PERCENT'] = p.cpu_percent()
        
        cpu_times = p.cpu_times_percent()
        if 'CPU_USER_PERCENT' in parametros_ativos:
            registro['CPU_USER_PERCENT'] = cpu_times.user
        if 'CPU_SYSTEM_PERCENT' in parametros_ativos:
            registro['CPU_SYSTEM_PERCENT'] = cpu_times.system
        if 'CPU_IDLE_PERCENT' in parametros_ativos:
            registro['CPU_IDLE_PERCENT'] = cpu_times.idle
        if 'CPU_IOWAIT_PERCENT' in parametros_ativos:
            registro['CPU_IOWAIT_PERCENT'] = cpu_times.iowait

    if 'CPU_INTERRUPTS' in parametros_ativos:
        registro['CPU_INTERRUPTS'] = p.cpu_stats().interrupts

    if 'CPU_FREQ_ATUAL' in parametros_ativos:
        registro['CPU_FREQ_ATUAL'] = p.cpu_freq().current

    # Filtro da LOAD AVERAGE
    if any(param in parametros_ativos for param in ['LOAD_AVG_1', 'LOAD_AVG_5', 'LOAD_AVG_15']):
        load = p.getloadavg()
        if 'LOAD_AVG_1' in parametros_ativos:
            registro['LOAD_AVG_1'] = load[0]
        if 'LOAD_AVG_5' in parametros_ativos:
            registro['LOAD_AVG_5'] = load[1]
        if 'LOAD_AVG_15' in parametros_ativos:
            registro['LOAD_AVG_15'] = load[2]

    # Filtro da RAM
    if any(param in parametros_ativos for param in ['RAM_PERCENT', 'RAM_TOTAL', 'RAM_AVAILABLE', 'RAM_USED', 'RAM_FREE']):
        ram = p.virtual_memory()
        if 'RAM_PERCENT' in parametros_ativos:
            registro['RAM_PERCENT'] = ram.percent
        if 'RAM_TOTAL' in parametros_ativos:
            registro['RAM_TOTAL'] = ram.total
        if 'RAM_AVAILABLE' in parametros_ativos:
            registro['RAM_AVAILABLE'] = ram.available
        if 'RAM_USED' in parametros_ativos:
            registro['RAM_USED'] = ram.used
        if 'RAM_FREE' in parametros_ativos:
            registro['RAM_FREE'] = ram.free

    # filtro SWAP
    if any(param in parametros_ativos for param in ['SWAP_PERCENT', 'SWAP_USED', 'SWAP_FREE', 'SWAP_IN', 'SWAP_OUT']):
        swap = p.swap_memory()
        if 'SWAP_PERCENT' in parametros_ativos:
            registro['SWAP_PERCENT'] = swap.percent
        if 'SWAP_USED' in parametros_ativos:
            registro['SWAP_USED'] = swap.used
        if 'SWAP_FREE' in parametros_ativos:
            registro['SWAP_FREE'] = swap.free
        if 'SWAP_IN' in parametros_ativos:
            registro['SWAP_IN'] = swap.sin
        if 'SWAP_OUT' in parametros_ativos:
            registro['SWAP_OUT'] = swap.sout

    # Filtro DISCO
    if any(param in parametros_ativos for param in ['DISCO_PERCENT', 'DISCO_USED', 'DISCO_FREE']):
        disco = p.disk_usage('/')
        if 'DISCO_PERCENT' in parametros_ativos:
            registro['DISCO_PERCENT'] = disco.percent
        if 'DISCO_USED' in parametros_ativos:
            registro['DISCO_USED'] = disco.used
        if 'DISCO_FREE' in parametros_ativos:
            registro['DISCO_FREE'] = disco.free

    # Guarda no arquivo csv com as colunas dinâmicas ou imprime os dados
    nome_maquina = socket.gethostname()
    nome_arquivo = f'./nautilus_coleta_{nome_maquina}.csv'

    arquivo_existe = os.path.exists(nome_arquivo)
    
    with open(nome_arquivo, 'a', newline='', encoding='utf-8') as csvfile:
        escritor = csv.DictWriter(csvfile, fieldnames=registro.keys(), delimiter=';')
        
        # Cria o cabeçalho só se o arquivo for novo
        if not arquivo_existe:
            escritor.writeheader()
        escritor.writerow(registro)

    # Coloquei um retorno no terminal pra hora da apresentação
    print(f"{timestamp} - Captura salva na máquina {nome_maquina}: {registro}")

    time.sleep(1)