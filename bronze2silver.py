import io
import pandas as pd
import numpy as np
from config_aws import *
from local2bronze import upload_file
import shutil 

# Definindo os caminhos corretos da Camada Bronze e Camada Silver
PREFIXO_ORIGEM = '01-bronze/'
PREFIXO_DESTINO = '02-silver/'

def listar_chaves(prefixo):
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

def transformar_chave(chave_origem):
    if chave_origem.startswith(PREFIXO_ORIGEM):
            nova_chave = chave_origem.replace(PREFIXO_ORIGEM, PREFIXO_DESTINO, 1)
    else:
        raise ValueError(f'Chave não começa com {PREFIXO_ORIGEM}: {chave_origem}')

    return nova_chave

def listar_pendentes():
    chaves = {
        'chaves_origem': set(listar_chaves(PREFIXO_ORIGEM)),
        'chaves_destino': set(listar_chaves(PREFIXO_DESTINO))
    }

    pendentes = []

    for chave_origem in chaves['chaves_origem']:
        chave_transformada = transformar_chave(chave_origem)

        if chave_transformada not in chaves['chaves_destino']:
            pendentes.append(chave_origem)

    return sorted(pendentes)

def baixar_arquivo(chave_origem):
    caminho_local = os.path.join('raw_local', os.path.basename(chave_origem))
    diretorio = os.path.dirname(caminho_local)

    os.makedirs(diretorio, exist_ok=True)

    s3_client.download_file(os.getenv('BUCKET_NAME'), chave_origem, caminho_local)

    return caminho_local

def ler_csv(caminho):
    df = pd.read_csv(caminho, sep=';')
    return df

def limpar(df):
    colunas_numericas = [
        'CPU_PERCENT', 'CPU_USER_PERCENT', 'CPU_SYSTEM_PERCENT', 'CPU_IDLE_PERCENT', 'CPU_IOWAIT_PERCENT',
        'CPU_FREQ_ATUAL', 'CPU_COUNT_LOGICA', 'LOAD_AVG_1', 'LOAD_AVG_5', 'LOAD_AVG_15',
        'RAM_PERCENT', 'RAM_TOTAL', 'RAM_AVAILABLE', 'RAM_USED',
        'SWAP_PERCENT', 'SWAP_USED', 'SWAP_FREE', 'SWAP_IN', 'SWAP_OUT',
        'DISCO_PERCENT', 'DISCO_TOTAL', 'DISCO_USED', 'DISCO_FREE'
    ]

    colunas_sem_negativos = ['CPU_FREQ_ATUAL', 'CPU_COUNT_LOGICA', 'LOAD_AVG_1', 'LOAD_AVG_5', 'LOAD_AVG_15']
    colunas_percentual = ['CPU_PERCENT', 'CPU_USER_PERCENT', 'CPU_SYSTEM_PERCENT', 'CPU_IDLE_PERCENT', 'CPU_IOWAIT_PERCENT', 'RAM_PERCENT', 'SWAP_PERCENT', 'DISCO_PERCENT']
    colunas_bytes = ['RAM_TOTAL', 'RAM_AVAILABLE', 'RAM_USED', 'SWAP_USED', 'SWAP_FREE', 'SWAP_IN', 'SWAP_OUT', 'DISCO_TOTAL', 'DISCO_USED', 'DISCO_FREE']

    grupos_verificacao = {
        'CPU': ['CPU_PERCENT', 'CPU_USER_PERCENT', 'CPU_SYSTEM_PERCENT', 'CPU_IDLE_PERCENT', 'CPU_IOWAIT_PERCENT', 'CPU_FREQ_ATUAL', 'CPU_COUNT_LOGICA', 'LOAD_AVG_1', 'LOAD_AVG_5', 'LOAD_AVG_15'],
        'RAM': ['RAM_PERCENT','RAM_TOTAL','RAM_AVAILABLE','RAM_USED'],
        'SWAP': ['SWAP_PERCENT','SWAP_USED','SWAP_FREE','SWAP_IN','SWAP_OUT'],
        'DISCO': ['DISCO_PERCENT','DISCO_TOTAL','DISCO_USED','DISCO_FREE']
    }

    df = df.copy()

    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')

    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce').astype('float64')

    for coluna in colunas_percentual:
        if coluna in df.columns:
            invalido = (df[coluna] < 0) | (df[coluna] > 100)
            df.loc[invalido, coluna] = np.nan

    for coluna in colunas_bytes + colunas_sem_negativos:
        if coluna in df.columns:
            invalido = df[coluna] < 0
            df.loc[invalido, coluna] = np.nan

    for grupo, lista in grupos_verificacao.items():
        if all(col in df.columns for col in lista):
            nao_monitorado = (df[lista] == 0).all(axis=1)
            df.loc[nao_monitorado, lista] = np.nan
 
    df = df.dropna(subset=[c for c in colunas_numericas if c in df.columns], how='all')
    df.dropna(subset=['TIMESTAMP'], inplace=True)
    df.drop_duplicates(subset=['HOSTNAME', 'TIMESTAMP'], keep='first', inplace=True)

    if 'CPU_COUNT_LOGICA' in df.columns:
        df['CPU_COUNT_LOGICA'] = df['CPU_COUNT_LOGICA'].astype('Int64')
    for coluna in colunas_bytes:
        if coluna in df.columns:
            df[coluna] = df[coluna].astype('Int64')

    return df

def gravar_csv(df, chave_origem):
    caminho_saida = os.path.join('silver_local', os.path.basename(transformar_chave(chave_origem)))
    diretorio = os.path.dirname(caminho_saida)

    os.makedirs(diretorio, exist_ok=True)

    df.to_csv(caminho_saida, sep=';', index=False, date_format='%Y-%m-%d %H:%M:%S', na_rep='')

    return caminho_saida

def processar(chave_origem):
    caminho = baixar_arquivo(chave_origem)

    df_lido = ler_csv(caminho)
    df_limpo = limpar(df_lido)

    file_silver = gravar_csv(df_limpo, chave_origem)
    chave_destino = transformar_chave(chave_origem)

    return upload_file(file_silver, os.getenv('BUCKET_NAME'), chave_destino)

if __name__ == '__main__':
    try:
        chaves_pendentes = listar_pendentes()
        print(f"Arquivos encontrados na Bronze para processar: {len(chaves_pendentes)}")

        for chave_atual in chaves_pendentes:
            print(f"Processando e enviando para Silver: {chave_atual}")
            processar(chave_atual)

        print('ETL Bronze -> Silver concluída com sucesso! :)')

    except Exception as e:
        print(f"Erro durante a execução da ETL: {e}")

    finally:
        shutil.rmtree('raw_local', ignore_errors=True)
        shutil.rmtree('silver_local', ignore_errors=True)
        print('Arquivos locais temporários removidos!')