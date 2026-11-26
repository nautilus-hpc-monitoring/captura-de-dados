"""Simulacao parametrizada da telemetria HPC nos estados normal e de incidente.

Reproduz as convencoes de gerar_amostra.py: a normalidade e gerada pelas
mesmas formulas (offsets por host, ciclo diario, ruido AR(1) e EMA dos loads)
e os incidentes somam os deltos trapezoidais sobre essa base. Nenhum rotulo e
enviado ao S3.
"""

from __future__ import annotations

import csv
import json
import math
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path

INTERVALO = 10
MB = 1024**2
PERFIS_BASE = ((1, 4), (2, 1), (3, 2), (4, 3), (2, 4))
ROTULOS_CABECALHO = ("TIMESTAMP", "HOSTNAME", "CENARIO", "INTENSIDADE")

CONFIG_PADRAO = {
    "cpu": 75.0,
    "iowait": 3.0,
    "freq": 2500.0,
    "ram": 50.0,
    "swap": 0.1,
    "disco": 40.0,
    "share": 0.18,
    "cpus": 64.0,
    "ram_total": float(128 * 1024**3),
    "swap_total": float(16 * 1024**3),
    "disco_total": float(2 * 1024**4),
}


def limitar(valor: float, minimo: float, maximo: float) -> float:
    return min(maximo, max(minimo, valor))


def trapezio(t: float, ini: float, subida: float, plato: float, descida: float) -> float:
    fim = ini + subida + plato + descida
    return limitar(min((t - ini) / subida, (fim - t) / descida), 0.0, 1.0)


def fmt_num(valor: float) -> str:
    numero = float(valor)
    if numero == 0:
        return "0"
    return format(numero, ".10f").rstrip("0").rstrip(".") or "0"


def num(valor: object) -> float:
    try:
        return float(valor)
    except (TypeError, ValueError):
        return 0.0


def epoch(texto: str) -> float:
    try:
        return datetime.strptime(texto, "%Y-%m-%d %H:%M:%S").timestamp()
    except (TypeError, ValueError):
        return time.time()


def inteiro(nome: str, padrao: int) -> int:
    try:
        return int(os.getenv(nome, str(padrao)))
    except ValueError:
        return padrao


def decimal(nome: str, padrao: float) -> float:
    try:
        return float(os.getenv(nome, str(padrao)))
    except ValueError:
        return padrao


def inter(nome: str, padrao: tuple[float, float]) -> tuple[float, float]:
    minimo = decimal(nome + "_MIN", padrao[0])
    maximo = decimal(nome + "_MAX", padrao[1])
    return (minimo, maximo) if maximo > minimo else (minimo, minimo)


def gravar_rotulos(diretorio: Path, nome_base: str, rotulos: list[tuple[str, str, str, str]]) -> None:
    diretorio.mkdir(parents=True, exist_ok=True)
    caminho = diretorio / nome_base.replace(".csv", "_rotulos.csv")
    cabecalho = not caminho.exists() or caminho.stat().st_size == 0
    with caminho.open("a", newline="", encoding="utf-8") as arquivo:
        escritor = csv.writer(arquivo, delimiter=";", lineterminator="\n", quoting=csv.QUOTE_NONE)
        if cabecalho:
            escritor.writerow(ROTULOS_CABECALHO)
        escritor.writerows(rotulos)


class InjetorIncidentes:
    def __init__(self, host: str) -> None:
        self.host = host
        self.ativo = os.getenv("INJETAR_INCIDENTES", "0") == "1"
        self.seed = inteiro("INJETAR_SEED", 42)
        self.eventos_por_dia = max(1, inteiro("INJETAR_EVENTOS_POR_DIA", 8))
        self.principais = max(1, inteiro("INJETAR_PRINCIPAIS", 5))
        self.secundarios = max(1, inteiro("INJETAR_SECUNDARIOS", 3))
        self.subida = inter("INJETAR_SUBIDA_MIN", (60.0, 120.0))
        self.plato = inter("INJETAR_PLATO_MIN", (30.0, 90.0))
        self.descida = inter("INJETAR_DESCIDA_MIN", (20.0, 40.0))
        self.jitter = inter("INJETAR_JITTER_MIN", (1.0, 8.0))
        self.primeiro_incidente_min = max(0.0, decimal("INJETAR_PRIMEIRO_INCIDENTE_MIN", 5.0))
        permitidos = tuple(
            int(item) for item in os.getenv("INJETAR_CENARIOS", "1,2,3,4").split(",")
            if item.strip().isdigit() and 1 <= int(item) <= 4
        ) or (1, 2, 3, 4)
        self.cenarios = permitidos
        self.rotulos_dir = Path(os.getenv("INJETAR_ROTULOS_DIR", "./rotulos"))
        self.estado_path = Path(os.getenv("INJETAR_ESTADO", f"./estado_injecao_{host}.json"))
        self.dry_run = os.getenv("INJETAR_DRY_RUN", "0") == "1"
        self.dry_run_dir = Path(os.getenv("INJETAR_DRY_RUN_DIR", "./dry-run"))
        self.base = {
            chave: decimal("INJETAR_" + chave.upper(), valor)
            for chave, valor in CONFIG_PADRAO.items()
        }
        self.rng = random.Random(self.seed)
        self.off_cpu = 0.0
        self.off_ram = 0.0
        self.off_disco = 0.0
        self.fase = 0.0
        self.inicio_epoca: float | None = None
        self.ruidos: dict[str, float] = {}
        self.ema: dict[str, float] = {"60": 0.0, "300": 0.0, "900": 0.0}
        self.swap_anterior: float | None = None
        self.eventos: list[tuple[int, float, float, float, float]] = []
        self._carregar_estado()
        if self.ativo and self.inicio_epoca is None:
            self._inicializar(time.time())

    def _carregar_estado(self) -> None:
        if not self.ativo or not self.estado_path.is_file():
            return
        try:
            dados = json.loads(self.estado_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if dados.get("host") != self.host:
            return
        self.inicio_epoca = dados.get("inicio_epoca")
        self.off_cpu = num(dados.get("off_cpu", 0.0))
        self.off_ram = num(dados.get("off_ram", 0.0))
        self.off_disco = num(dados.get("off_disco", 0.0))
        self.fase = num(dados.get("fase", 0.0))
        self.ruidos = {chave: num(valor) for chave, valor in dados.get("ruidos", {}).items()}
        for chave, valor in dados.get("ema", {}).items():
            self.ema[chave] = num(valor)
        self.swap_anterior = dados.get("swap_anterior")
        self.eventos = [tuple(evento) for evento in dados.get("eventos", [])]
        estado_rng = dados.get("rng")
        if isinstance(estado_rng, list) and len(estado_rng) == 3:
            self.rng.setstate((estado_rng[0], tuple(estado_rng[1]), estado_rng[2]))

    def salvar_estado(self) -> None:
        if not self.ativo:
            return
        estado_rng = self.rng.getstate()
        estado = {
            "host": self.host,
            "base": self.base,
            "off_cpu": self.off_cpu,
            "off_ram": self.off_ram,
            "off_disco": self.off_disco,
            "fase": self.fase,
            "inicio_epoca": self.inicio_epoca,
            "ruidos": self.ruidos,
            "ema": self.ema,
            "swap_anterior": self.swap_anterior,
            "eventos": self.eventos,
            "rng": [estado_rng[0], list(estado_rng[1]), estado_rng[2]],
        }
        temporario = self.estado_path.with_suffix(".json.tmp")
        temporario.write_text(json.dumps(estado), encoding="utf-8")
        temporario.replace(self.estado_path)

    def _inicializar(self, agora: float) -> None:
        self.off_cpu = self.rng.gauss(0.0, 3.0)
        self.off_ram = self.rng.gauss(0.0, 3.0)
        self.off_disco = self.rng.gauss(0.0, 2.0)
        self.fase = self.rng.uniform(0.0, 2.0 * math.pi)
        self.inicio_epoca = agora
        self.ruidos = {}
        runq_base = max(0.0, self.base["cpus"] * self.base["cpu"] / 100.0 * 0.9)
        self.ema = {"60": runq_base, "300": runq_base, "900": runq_base}
        self.swap_anterior = self.base["swap"] / 100.0 * self.base["swap_total"]
        self._agendar(agora)

    def _perfil(self) -> tuple[int, int]:
        encontrado = re.search(r"node(\d+)", self.host, re.IGNORECASE)
        indice = int(encontrado.group(1)) - 1 if encontrado else 0
        principal, secundario = PERFIS_BASE[indice % len(PERFIS_BASE)]
        if principal not in self.cenarios:
            principal = self.cenarios[0]
        if secundario not in self.cenarios:
            secundario = self.cenarios[-1]
        return principal, secundario

    def _agendar(self, agora: float) -> None:
        principal, secundario = self._perfil()
        quantidade = self.eventos_por_dia
        n_principal = max(1, round(self.principais * quantidade / (self.principais + self.secundarios)))
        padrao = [principal] * n_principal + [secundario] * (quantidade - n_principal)
        while len(padrao) < quantidade:
            padrao.append(secundario)
        cenarios = padrao[:quantidade]
        self.rng.shuffle(cenarios)

        slot = 86400 / quantidade
        self.eventos = []
        for indice, cenario in enumerate(cenarios):
            ini = agora + self.primeiro_incidente_min * 60 + indice * slot + self.rng.uniform(*self.jitter) * 60
            subida = self.rng.uniform(*self.subida) * 60
            plato = self.rng.uniform(*self.plato) * 60
            descida = self.rng.uniform(*self.descida) * 60
            self.eventos.append((cenario, ini, subida, plato, descida))

    def _ruido(self, nome: str, desvio: float, phi: float = 0.9) -> float:
        y = self.ruidos.get(nome, 0.0)
        y = phi * y + self.rng.gauss(0.0, desvio * math.sqrt(1.0 - phi * phi))
        self.ruidos[nome] = y
        return y

    def _atualizar_ema(self, runq: float) -> tuple[float, float, float]:
        for tau in (60.0, 300.0, 900.0):
            fator = math.exp(-INTERVALO / tau)
            chave = str(int(tau))
            self.ema[chave] = fator * self.ema.get(chave, 0.0) + (1.0 - fator) * runq
        return self.ema["60"], self.ema["300"], self.ema["900"]

    def _simular(self, linha: list[str], s: list[float]) -> list[str]:
        agora = epoch(linha[0])
        if self.inicio_epoca is None:
            self.inicio_epoca = agora
        ciclo = math.sin(self.fase + 2.0 * math.pi * (agora - self.inicio_epoca) / 86400.0)

        r_cpu = self._ruido("cpu", 2.0)
        r_iowait = self._ruido("iowait", 0.8)
        r_share = self._ruido("share", 0.01)
        r_freq = self._ruido("freq", 30.0)
        r_runq = self._ruido("runq", 2.0)
        r_ram = self._ruido("ram", 1.0)
        r_swap = self._ruido("swap", 0.01)
        r_disco = self._ruido("disco", 0.15, 0.999)

        s1, s2, s3, s4 = s
        cpu_base = self.base["cpu"]
        cpus = self.base["cpus"]
        ram_total = self.base["ram_total"]
        swap_total = self.base["swap_total"]
        disco_total = self.base["disco_total"]

        cpu = limitar(
            cpu_base + self.off_cpu + 2.0 * ciclo + r_cpu + 22.0 * s1 + 20.0 * s4 + 18.0 * s2,
            1.0,
            99.5,
        )
        iowait = limitar(self.base["iowait"] + r_iowait + 18.0 * s2, 0.1, 60.0)
        restante = max(cpu - iowait, 0.0)
        share = limitar(self.base["share"] + r_share + 0.08 * s2, 0.05, 0.5)
        sistema = restante * share
        usuario = restante - sistema
        ocioso = 100.0 - cpu
        freq = limitar(self.base["freq"] + 6.0 * (cpu - cpu_base) + r_freq, 1200.0, 3500.0)

        runq = max(
            cpus * cpu / 100.0 * 0.9 + cpus * (0.6 * s1 + 0.3 * s4 + 0.2 * s2) + r_runq,
            0.0,
        )
        load1, load5, load15 = self._atualizar_ema(runq)

        ram_pct = limitar(
            self.base["ram"] + self.off_ram + ciclo + r_ram + 38.0 * s2 + 35.0 * s4,
            5.0,
            99.0,
        )
        ram_usado = ram_pct / 100.0 * ram_total
        ram_disponivel = ram_total - ram_usado

        swap_pct = limitar(
            self.base["swap"] + abs(r_swap) + 35.0 * s2**1.5 + 20.0 * s3,
            0.0,
            100.0,
        )
        swap_usado = swap_pct / 100.0 * swap_total
        swap_livre = swap_total - swap_usado
        anterior = self.swap_anterior if self.swap_anterior is not None else swap_usado
        variacao = swap_usado - anterior
        self.swap_anterior = swap_usado
        swap_out = max(variacao, 0.0) + s2**1.5 * self.rng.uniform(5.0, 25.0) * MB
        swap_in = max(-variacao, 0.0) + s2**1.5 * self.rng.uniform(1.0, 8.0) * MB

        disco_pct = limitar(
            self.base["disco"] + self.off_disco + r_disco + 45.0 * s3,
            1.0,
            99.0,
        )
        disco_usado = disco_pct / 100.0 * disco_total
        disco_livre = disco_total - disco_usado

        nova = list(linha)
        nova[3] = fmt_num(round(cpu, 2))
        nova[4] = fmt_num(round(usuario, 2))
        nova[5] = fmt_num(round(sistema, 2))
        nova[6] = fmt_num(round(ocioso, 2))
        nova[7] = fmt_num(round(iowait, 2))
        nova[8] = fmt_num(round(freq, 1))
        nova[9] = fmt_num(round(cpus))
        nova[10] = fmt_num(round(load1, 2))
        nova[11] = fmt_num(round(load5, 2))
        nova[12] = fmt_num(round(load15, 2))
        nova[13] = fmt_num(round(ram_pct, 2))
        nova[14] = fmt_num(round(ram_total))
        nova[15] = fmt_num(round(ram_disponivel))
        nova[16] = fmt_num(round(ram_usado))
        nova[17] = fmt_num(round(swap_pct, 2))
        nova[18] = fmt_num(round(swap_usado))
        nova[19] = fmt_num(round(swap_livre))
        nova[20] = fmt_num(round(swap_in))
        nova[21] = fmt_num(round(swap_out))
        nova[22] = fmt_num(round(disco_pct, 2))
        nova[23] = fmt_num(round(disco_total))
        nova[24] = fmt_num(round(disco_usado))
        nova[25] = fmt_num(round(disco_livre))
        return nova

    def processar(self, linha: list[str]) -> tuple[list[str], tuple[str, str, str, str]]:
        rotulo = (linha[0], linha[1], "0", "0")
        if not self.ativo:
            return linha, rotulo

        agora = epoch(linha[0])
        if self.inicio_epoca is None:
            self._inicializar(agora)

        s = [0.0, 0.0, 0.0, 0.0]
        for cenario, ini, subida, plato, descida in self.eventos:
            valor = trapezio(agora, ini, subida, plato, descida)
            if valor > s[cenario - 1]:
                s[cenario - 1] = valor

        if max(s) > 0.0:
            primeiro = next(indice + 1 for indice, valor in enumerate(s) if valor > 0.0)
            rotulo = (linha[0], linha[1], str(primeiro), fmt_num(round(max(s), 4)))
        return self._simular(linha, s), rotulo
