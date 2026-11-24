"""Entrada compartilhada das Lambdas da equipe.

Configure ETL_MODULO para selecionar a ETL.
Use bronze2silver para a Bronze, silver2gold para o gestor,
silver2gold_operador para o operador (silver2goldOperador também é aceito) ou
silver2gold_analista para o analista (silver2goldAnalista também é aceito).
Para outros módulos, use o nome do arquivo sem .py.
"""
import importlib
import os


def lambda_handler(event, context):
    nome_modulo = os.getenv("ETL_MODULO")
    if not nome_modulo or nome_modulo == "lambda_function" or not nome_modulo.isidentifier():
        raise ValueError("Configure ETL_MODULO com o nome da ETL, sem .py.")

    if nome_modulo in ("silver2gold_operador", "silver2goldOperador"):
        from silver2gold import lambda_handler_operador

        return lambda_handler_operador(event, context)

    if nome_modulo in ("silver2gold_analista", "silver2goldAnalista"):
        from silver2gold import lambda_handler_analista

        return lambda_handler_analista(event, context)

    etl = importlib.import_module(nome_modulo)
    executar = getattr(etl, "lambda_handler", None)
    if not callable(executar):
        raise ValueError(f"A ETL {nome_modulo} precisa ter lambda_handler(event, context).")

    return executar(event, context)
