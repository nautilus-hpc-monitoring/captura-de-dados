from config_aws import *
import requests
import psutil as p
import time
import csv
from datetime import datetime
import socket
import logging
import shutil
from pathlib import Path
from getpass import getpass
from datetime import timezone
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from boto3.exceptions import S3UploadFailedError
from injecao_incidentes import InjetorIncidentes, gravar_rotulos

host = os.getenv('AUTH_HOSTNAME', 'alpha-node01').strip()
injetor = InjetorIncidentes(host)
url_auth = os.getenv('AUTH_URL', 'http://localhost:8080/auth/').rstrip('/')
url_credenciais = url_auth + '/credenciais'
credenciais_login = None
empresa_id_autenticada = None

def renovar_credenciais():
    global s3_client
    if not credenciais_login:
        return False
    try:
        resposta = requests.post(url_credenciais, json=credenciais_login,
                                 timeout=(5, 15), allow_redirects=False)
        if resposta.status_code != 200:
            logging.warning('Endpoint de credenciais indisponivel (HTTP %s).', resposta.status_code)
            return False
        payload = resposta.json()

        cred = payload['credenciais']

        campos = ('accessKeyId', 'secretAccessKey', 'sessionToken')

        if not all(isinstance(cred.get(k), str) and cred[k] for k in campos):
            return False

        expiracao = datetime.fromisoformat(cred['expiration'].replace('Z', '+00:00'))
        
        if expiracao.tzinfo is None or (expiracao - datetime.now(timezone.utc)).total_seconds() <= 60:
            return False

        novo_cliente = boto.client(
            's3', aws_access_key_id=cred['accessKeyId'],
            aws_secret_access_key=cred['secretAccessKey'],
            aws_session_token=cred['sessionToken'],
            region_name=payload.get('region') or os.getenv('REGION_NAME') or 'us-east-1')

        s3_client = novo_cliente
        return True

    except (requests.exceptions.RequestException, ValueError, KeyError, TypeError, AttributeError):
        logging.warning('Nao foi possivel renovar as credenciais; o lote permanece pendente.')
        return False


def erro_de_acesso(erro):
    codigos = {'ExpiredToken', 'ExpiredTokenException', 'InvalidToken',
               'InvalidAccessKeyId', 'SignatureDoesNotMatch', 'RequestExpired',
               'AccessDenied', 'TokenRefreshRequired'}
    if isinstance(erro, (NoCredentialsError, PartialCredentialsError)):
        return True
    if isinstance(erro, ClientError):
        return erro.response.get('Error', {}).get('Code') in codigos
    # upload_file encapsula ClientError em S3UploadFailedError.
    return isinstance(erro, S3UploadFailedError) and any(
        '(' + codigo + ')' in str(erro) for codigo in codigos)


flags_monitoramento = {
    'CPU': True,
    'RAM': True,
    'SWAP': True,
    'DISCO': True
}

def authComponentes(componentes):
    for componente in componentes:
        flags_monitoramento[componente['componente']['tipo']] = True

def coletar_cpu():
    if flags_monitoramento.get('CPU', False):
        cpu_times = p.cpu_times_percent()
        cpu_freq = p.cpu_freq()

        # O iowait só existe no Linux/macOS. No Windows, usamos 0 para evitar o erro.
        iowait_val = getattr(cpu_times, 'iowait', 0)

        return [
            p.cpu_percent(),
            cpu_times.user,
            cpu_times.system,
            cpu_times.idle,
            iowait_val,
            cpu_freq.current if cpu_freq else 0,
            p.cpu_count(),
            p.getloadavg()[0] if hasattr(p, 'getloadavg') else 0,
            p.getloadavg()[1] if hasattr(p, 'getloadavg') else 0,
            p.getloadavg()[2] if hasattr(p, 'getloadavg') else 0
        ]

    return [None, None, None, None, None, None, None, None, None, None]


def coletar_ram():
    if flags_monitoramento.get('RAM', False):
        ram = p.virtual_memory()

        return [
            ram.percent,
            ram.total,
            ram.available,
            ram.used
        ]

    return [None, None, None, None]


def coletar_swap():
    if flags_monitoramento.get('SWAP', False):
        swap = p.swap_memory()

        return [
            swap.percent,
            swap.used,
            swap.free,
            swap.sin,
            swap.sout
        ]

    return [None, None, None, None, None]


def coletar_disco():
    if flags_monitoramento.get('DISCO', False):
        disco = p.disk_usage('/')

        return [
            disco.percent,
            disco.total,
            disco.used,
            disco.free
        ]

    return [None, None, None, None]

cabecalho = [
    'TIMESTAMP',
    'HOSTNAME',
    'USER',
    'EMPRESA_ID',

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
usuario = os.environ.get('USER') or os.environ.get('USERNAME') or 'Desconhecido'

def upload_file(file_name, bucket, object_name):
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        return True
    except Exception as erro:
        if not erro_de_acesso(erro):
            logging.warning('Falha no upload; lote pendente (%s).', type(erro).__name__)
            return False

    if not renovar_credenciais():
        return False
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        return True
    except Exception as erro:
        logging.warning('Envio ainda indisponivel apos renovacao (%s).', type(erro).__name__)
        return False

INTERVALO = 10  
LOTE = 6 
BILLING_LOCAL_DIR = Path(__file__).resolve().parent / 'silver_local'
SALVAR_BILLING_LOCAL = os.getenv('SALVAR_BILLING_LOCAL', 'true').lower() == 'true'


def salvar_copia_billing_local(caminho_origem, nome_arquivo):
    if not SALVAR_BILLING_LOCAL:
        return
    BILLING_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    destino = BILLING_LOCAL_DIR / nome_arquivo
    shutil.copy2(caminho_origem, str(destino))

def enviar_lote(linhas, rotulos=None):
    momento = datetime.now()
    nome_caminho = f'data_{momento:%Y%m%d_%H%M%S}.csv'
    caminho_local = f'./{nome_caminho}'


    objeto = (
        f"{os.getenv('FILE_KEY')}/"
        f'hostname={host}/ano={momento:%Y}/mes={momento:%m}/dia={momento:%d}/'
        f'{nome_caminho}'
    )

    with open(caminho_local, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow(cabecalho)
        writer.writerows(linhas)

    try:
        if injetor.dry_run:
            injetor.dry_run_dir.mkdir(parents=True, exist_ok=True)
            os.replace(caminho_local, injetor.dry_run_dir / nome_caminho)
            enviado = True
        else:
            enviado = upload_file(caminho_local, os.getenv('BUCKET_NAME'), objeto)
        salvar_copia_billing_local(caminho_local, nome_caminho)
        return upload_file(caminho_local, os.getenv('BUCKET_NAME'), objeto)
    finally:
        if not injetor.dry_run and os.path.exists(caminho_local):
            os.remove(caminho_local)

    if enviado and injetor.ativo and rotulos:
        diretorio = injetor.dry_run_dir if injetor.dry_run else injetor.rotulos_dir
        gravar_rotulos(diretorio, nome_caminho, rotulos)
    return enviado

def escrita():
    dados = []
    rotulos = []

    try:
        while True:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            cpu = coletar_cpu()
            ram = coletar_ram()
            swap = coletar_swap()
            disco = coletar_disco()

            linha = [
                timestamp,
                host,
                usuario,
                empresa_id_autenticada,

                *cpu,
                *ram,
                *swap,
                *disco
            ]

            linha, rotulo = injetor.processar(linha)

            print(linha)
            dados.append(linha)
            rotulos.append(rotulo)

            injetor.salvar_estado()

            if len(dados) >= LOTE:
                if enviar_lote(dados, rotulos):
                    print(f'\nLote de {len(dados)} linhas enviado com sucesso!\n')
                    dados = []
                    rotulos = []
                else:
                    print('Falha no envio, as linhas continuam em dados e vão no próximo lote')

            time.sleep(INTERVALO)

    except KeyboardInterrupt:
        if dados:
            enviar_lote(dados, rotulos)
        print('\nMonitoramento encerrado')

if __name__ == '__main__':
    start_msg = '''
    =========================================================================
                    REALIZE SUA AUTENTICAÇÃO PARA PROSSEGUIR!
    =========================================================================
    '''
    print(start_msg)

    email = input('Email: ')
    password = input('Senha: ')
    credenciais_login = {'email': email, 'senha': password, 'hostname': host}

    try:
        res = requests.post(
            url_auth,
            json={
                'email': email,
                'senha': password,
                'hostname': host
            },
            timeout=(5, 15)
        )
        res.raise_for_status()

        res = res.json()


        if res.get('autenticado'):
            empresa_id_autenticada = res.get('empresa_id')
            print('\nAutenticação realizada com sucesso!')
            escrita()

        else:
            print('Falha na autenticação! Email e/ou senha incorretos')

    except requests.exceptions.HTTPError as erro:
        print('Erro na autenticação:', erro, erro.response.text if erro.response is not None else '')


    except requests.exceptions.RequestException as erro:
        print('Não foi possível conectar à API:', erro)

