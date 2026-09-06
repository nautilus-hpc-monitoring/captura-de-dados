import csv
import utils.exibicao as ex
import cpu.cpu as cpu

# Leitura do CSV
def ler_csv(nome_arquivo: str) -> list:
    dados = []

    with open(nome_arquivo, 'r', encoding='utf-8') as arquivo:
        leitor = csv.reader(
            arquivo,
            delimiter=';'
        )

        leitor = csv.DictReader(arquivo)

        for linha in leitor:
            dados.append(linha)

    return dados

def main():
    dados = ler_csv('nautilus_coleta_raphael.csv')

    if len(dados) == 0:
        print("Nenhum dado encontrado.")
        return

    ex.exibir_relatorio(dados)

main()
