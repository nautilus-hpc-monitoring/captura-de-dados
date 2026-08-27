def titulo(texto: str):
    print("\n" + "=" * 65)
    print(f"{texto:^65}")
    print("=" * 65)

def resultado(nome: str, valor: float, unidade: str = "", casas: int = 2):
    print(f"{nome:<40} {valor:>15.{casas}f} {unidade}")
