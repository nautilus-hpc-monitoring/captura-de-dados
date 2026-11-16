# Padrão das ETLs e da Lambda

## Arquivos necessários

| Arquivo | Função |
|---|---|
| `local2bronze.py` | Captura o notebook e envia os CSVs à Bronze. Não é uma ETL executada pela Lambda. |
| `config_aws.py` | Cria a conexão S3 usada pelo coletor e pela Bronze → Silver original. Sem chaves explícitas, boto3 usa IAM na AWS. |
| `bronze2silver.py` | Mantém as funções originais de limpeza, pendências e upload. Foi acrescentado somente `lambda_handler`. |
| `silver2gold.py` | ETL do gestor: lê Silver, calcula médias/séries/previsões e publica o JSON da dashboard. |
| `lambda_function.py` | Entrada comum. Lê `ETL_MODULO`, importa essa ETL e chama seu `lambda_handler`. Não encadeia etapas. |
| `requirements-lambda.txt` | Dependências comuns para a imagem da Lambda. Inclui requests/psutil porque a Bronze original importa local2bronze. |
| `Dockerfile.lambda` | Empacota os módulos Python da raiz e as dependências para Linux/Python 3.12. |
| `.dockerignore` | Evita enviar `.env`, credenciais locais, Git e caches ao contexto do build. |
| `.gitignore` | Regras de arquivos que não devem entrar no Git. Mantido o existente. |
| `README.md` | Visão geral do projeto e link para este guia. |
| `ETLS.md` | Este guia: padrão para integrantes da equipe e explicação dos arquivos. |

## O que foi retirado por redundância

- `gestor_telemetria.py`: suas funções necessárias já estão em silver2gold.py.
- `silver_to_gold_gestor.py`: era apenas um apelido/encaminhamento para silver2gold.py.
- `pipeline_lambda.py`: encadeamento automático criado anteriormente; não é necessário para expor cada ETL à Lambda.
- `lambda-gold-dashboard/`: pacote específico da Gold, substituído pelo pacote comum.
- `lambda-pipeline/`: pacote do encadeamento, substituído pelo pacote comum.

Os arquivos foram copiados para um backup fora do repositório antes da remoção.

## Bronze → Silver: o que mudou e o que foi preservado

Foi restaurado o conteúdo anterior, preservando as funções e o fluxo local original.
`limpar`, `listar_pendentes`, `processar`, os CSVs e o mecanismo de upload não foram
reescritos. A entrada nova chama essas mesmas funções. Somente durante a invocação
Lambda, usa uma pasta temporária em `/tmp` e restaura o diretório ao terminar.
Esse ajuste é necessário porque a pasta do código da Lambda é somente leitura.
Uma publicação que retorna False gera erro na Lambda; o fluxo local anterior continua igual.

Para executar como antes: `python bronze2silver.py`.
Para Lambda, configure a entrada comum e `ETL_MODULO=bronze2silver`.

## Padrão para os colegas incluírem ETLs

1. Colocar o arquivo Python na raiz do repositório. Sugestões:
   `silver2gold_analista.py`, `silver2gold_operador.py`.
2. Manter a execução real dentro de funções, não iniciá-la ao importar o módulo.
3. Expor a função abaixo, chamando o processamento já existente:

```python
def lambda_handler(event, context):
    return executar()


if __name__ == "__main__":
    executar()
```

4. Adicionar bibliotecas novas a requirements-lambda.txt, se necessário.
5. Reconstruir/publicar a imagem e configurar ETL_MODULO com o nome do arquivo sem `.py`.

Não precisa editar lambda_function.py ou registrar a ETL em uma lista.
O módulo não pode se chamar lambda_function, pois esse nome é da entrada comum.
Cada ETL deve escrever em seu próprio destino Gold para não sobrescrever a dos colegas.
Arquivos auxiliares na raiz também entram na imagem; subpastas próprias exigem COPY no Dockerfile.

## Build e configuração AWS

Na raiz do repositório:

```bash
docker buildx build --platform linux/amd64 --provenance=false --load -f Dockerfile.lambda -t nautilus-etls:sprint3 .
```

Envie a imagem ao ECR e crie/atualize as funções Lambda x86_64 em us-east-1.
A entrada da imagem é `lambda_function.lambda_handler`.
Use funções separadas para Bronze/Silver, Gold gestor e as Golds dos colegas; elas
podem usar a mesma imagem, com variáveis e permissões próprias.

### Função Bronze → Silver

```text
ETL_MODULO=bronze2silver
BUCKET_NAME=nautilus-dados-joao-2026
REGION_NAME=us-east-1
```

Prefixos preservados no código: 01-bronze/ e 02-silver/.
Permissões S3: listar esses prefixos, ler a Bronze e escrever na Silver.
Precisa também de logs no CloudWatch. Não depende de login interativo com IAM válido.

### Função Silver → Gold gestor

```text
ETL_MODULO=silver2gold
BUCKET_NAME=nautilus-dados-joao-2026
GESTOR_CLUSTER_ID=<ID REAL DO SETUP>
HOSTNAMES=["LAPTOP-D2EC7KBR"]
DISCO_VOLUME=/
COLETA_SEGUNDOS=10
SWAP_CONTADORES_DISPONIVEIS=false
```

O setup gera lambda-sprint3-config.json com o ID real; acrescente ETL_MODULO=silver2gold.
Permissões S3: listar/ler Silver, ler/gravar 03-gold/gestor/* e logs.
HOSTNAMES evita conexão ao banco. Sem essa variável, o modo local pode consultar o
cadastro no MySQL, usando as variáveis DB_*.

## Acionamento

O código está pronto para receber invocações, mas não cria agendamentos ou gatilhos.
Comece com evento manual `{}` para cada função, Bronze → Silver primeiro.
Use concorrência reservada 1 nos primeiros testes.
Não configure gatilho para todas as pastas do bucket, evitando ciclos de gravação.
Um evento S3 pode acionar a Bronze/Silver com filtro 01-bronze/ e .csv, mas essa entrada
continua processando a lista original de pendências, não apenas o arquivo do evento.
O encadeamento e o intervalo de 90 segundos ainda serão configurados na AWS.

O setup_sprint3.sh prepara a EC2 e os dados demonstrativos do banco; não configura Lambda.
Subir arquivos no Git também não publica a imagem ou as funções na AWS.

## Validação e limitações

Foram comparadas as funções originais da Bronze/Silver e exercitadas suas chamadas
com S3 simulado, sem iniciar captura ou acessar a AWS. Não foi construído o container.
O notebook Windows não fornece sin/sout reais pelo psutil; o gráfico de swap-out
permanece indisponível. As previsões continuam dependendo de histórico suficiente.
