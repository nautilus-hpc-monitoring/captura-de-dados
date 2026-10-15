import io
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config_aws import *
from local2bronze import upload_file
import shutil

PREFIXO_ORIGEM = '02-silver/'
PREFIXO_DESTINO = '03-gold/'

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

def baixar_arquivo(chave_origem):
    caminho_local = os.path.join('silver_local', os.path.basename(chave_origem))
    os.makedirs(os.path.dirname(caminho_local), exist_ok=True)
    s3_client.download_file(os.getenv('BUCKET_NAME'), chave_origem, caminho_local)
    return caminho_local

def processar_camada_gold():
    chaves_silver = listar_chaves(PREFIXO_ORIGEM)
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

    #KPIS
    df_24h = df_global[df_global['TIMESTAMP'] >= ultimas_24h]
    
    # Consideramos critico se CPU ou DISCO > 90%, aviso se > 75%
    def classificar_alerta(row):
        cpu = row.get('CPU_PERCENT', 0)
        disco = row.get('DISCO_PERCENT', 0)
        if cpu > 90 or disco > 90:
            return 'critico'
        elif cpu > 75 or disco > 75:
            return 'aviso'
        return 'ok'

    if not df_24h.empty:
        df_24h['STATUS_ALERTA'] = df_24h.apply(classificar_alerta, axis=1)
        kpi_alertas = df_24h[df_24h['STATUS_ALERTA'] != 'ok']
        total_critico = len(kpi_alertas[kpi_alertas['STATUS_ALERTA'] == 'critico'])
        total_aviso = len(kpi_alertas[kpi_alertas['STATUS_ALERTA'] == 'aviso'])
    else:
        total_critico = 0
        total_aviso = 0

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
    upload_file(path_kpis, os.getenv('BUCKET_NAME'), f'{PREFIXO_DESTINO}kpis/kpis_{momento_str}.csv')
    upload_file(path_hist, os.getenv('BUCKET_NAME'), f'{PREFIXO_DESTINO}historico/historico_{momento_str}.csv')
    upload_file(path_proj, os.getenv('BUCKET_NAME'), f'{PREFIXO_DESTINO}projecao/projecao_{momento_str}.csv')

    print("ETL Silver -> Gold executada com sucesso!")

if __name__ == '__main__':
    try:
        processar_camada_gold()
    finally:
        shutil.rmtree('silver_local', ignore_errors=True)
        shutil.rmtree('gold_local', ignore_errors=True)