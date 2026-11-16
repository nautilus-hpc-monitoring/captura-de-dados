"""Entrada compartilhada das Lambdas da equipe.

Configure ETL_MODULO com o nome do arquivo, sem .py.
Exemplos: bronze2silver, silver2gold ou silver2gold_analista.
"""
import importlib
import os


def lambda_handler(event, context):
    nome_modulo = os.getenv("ETL_MODULO")
    if not nome_modulo or nome_modulo == "lambda_function" or not nome_modulo.isidentifier():
        raise ValueError("Configure ETL_MODULO com o nome da ETL, sem .py.")

    etl = importlib.import_module(nome_modulo)
    executar = getattr(etl, "lambda_handler", None)
    if not callable(executar):
        raise ValueError(f"A ETL {nome_modulo} precisa ter lambda_handler(event, context).")

    return executar(event, context)
