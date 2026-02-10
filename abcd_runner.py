#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
abcd_runner.py

Runner diário para Lotofácil ABCD:
- Baixa o XLSX oficial da CAIXA (ou reutiliza se já existir no diretório)
- Lê concursos (Concurso, Data Sorteio, Bola1..Bola15) de forma robusta
- Gera 4 jogos de 15 dezenas (AB/AC/AD/BCD) a partir do histórico recente
- Calcula gate por "gap em concursos" (percentis sobre gaps de eventos de overlap)
- Mantém campanhas concorrentes (teimosinha) em docs/state/abcd_campaigns.json
- Grava snapshot diário em docs/results/YYYY/MM/YYYY-MM-DD.json
- Grava abcd_signal.json na raiz
- (Opcional) Gera email_body.txt SOMENTE quando gate_pass=True

Observação:
Este runner não reimplementa o seu backtest completo; ele é para produção diária,
persistindo estado e histórico no repositório.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from openpyxl import load_workbook

# =========================
# Config
# =========================

CAIXA_URL = "https://servicebus3.caixa.gov.br/portaldeloterias/api/resultados/download?modalidade=Lotof%C3%A1cil"

DOCS_DIR = Path("docs")
STATE_DIR = DOCS_DIR / "state"
RESULTS_DIR = DOCS_DIR / "results"

STATE_CAMPAIGNS = STATE_DIR / "abcd_campaigns.json"

ROOT_SIGNAL_JSON = Path("abcd_signal.json")
EMAIL_BODY_TXT = Path("email_body.txt")

RNG_SEED = 1337  # determinístico entre runs, dado o mesmo histórico

# =========================
# Model
# =========================

@dataclass(frozen=True)
class Draw:
    concurso: int
    data: date
    dezenas: Tuple[int, ...]  # sorted 15 nums
    rateios: Dict[int, float]  # {11..15: valor}

    @property
    def set(self) -> Set[int]:
        return set(self.dezenas)


# =========================
# XLSX helpers
# =========================

def _norm(v: object) -> str:
    if v is None:
        return ""
    return str(v).strip().lower().replace(" ", "").replace("_", "")

def _parse_date_br(s: object) -> Optional[date]:
    if s is None:
        return None
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, date):
        return s
    txt = str(s).strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", txt)
    if m:
        d, mo, y = map(int, m.groups())
        return date(y, mo, d)
    # tenta ISO
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", txt)
    if m:
        y, mo, d = map(int, m.groups())
        return date(y, mo, d)
    return None


def _parse_money_br(v: object) -> float:
    """Converte 'R$49.765,82' / '49.765,82' / 10 / None em float."""
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    # remove prefixo e espaços
    s = s.replace("R$", "").replace("r$", "").strip()
    # remove separador de milhar '.' e troca ',' por '.'
    s = s.replace(".", "").replace(",", ".")
    # remove qualquer coisa não numérica (mantém . e -)
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s) if s else 0.0
    except Exception:
        return 0.0

def _find_header_row(ws, max_scan_rows: int = 25) -> Tuple[int, Dict[str, int]]:
    """
    Encontra header robustamente: Concurso + Bola1..Bola15 (ou Dezena1..Dezena15).
    Retorna (row_index, cols_map) com chaves:
      - concurso
      - data (opcional)
      - bola1..bola15
    """
    for r in range(1, max_scan_rows + 1):
        cols: Dict[str, int] = {}
        for c in range(1, 80):
            key = _norm(ws.cell(r, c).value)
            if key:
                cols[key] = c

        has_concurso = any(k in cols for k in ("concurso", "nconcurso", "numeroconcurso"))
        has_bola1 = any(k in cols for k in ("bola1", "dezena1"))
        has_bola15 = any(k in cols for k in ("bola15", "dezena15"))

        if has_concurso and has_bola1 and has_bola15:
            def pick(*names: str) -> Optional[int]:
                for n in names:
                    if n in cols:
                        return cols[n]
                return None

            out: Dict[str, int] = {}
            out["concurso"] = pick("concurso", "nconcurso", "numeroconcurso") or 1
            data_col = pick("datasorteio", "datadosorteio", "data", "datasorteio:")
            if data_col:
                out["data"] = data_col
            for i in range(1, 16):
                out[f"bola{i}"] = pick(f"bola{i}", f"dezena{i}") or (2 + i)

            # rateios (se existirem)
            # nomes comuns: "Rateio 11 acertos" => rateio11acertos
            for k in range(11, 16):
                col = pick(f"rateio{k}acertos", f"rateio{k}acerto", f"rateio{k}")
                if col:
                    out[f"rateio{k}"] = col

            return r, out

    # debug
    sample = []
    for rr in range(1, 6):
        sample.append([ws.cell(rr, cc).value for cc in range(1, 25)])
    raise RuntimeError(
        "Não consegui localizar o header do XLSX (Concurso/Bola1..Bola15). "
        f"Amostra topo da planilha: {sample}"
    )

def ensure_results_file() -> str:
    """
    Procura resultados_DDMMYYYY.xlsx (hoje), senão (ontem), senão baixa e salva como hoje.
    (No Actions, o 'hoje' é UTC do runner, mas tudo bem — o XLSX é cumulativo.)
    """
    def ddmmyyyy(d: date) -> str:
        return d.strftime("%d%m%Y")

    today = date.today()
    yesterday = date.fromordinal(today.toordinal() - 1)

    f_today = f"resultados_{ddmmyyyy(today)}.xlsx"
    f_yest = f"resultados_{ddmmyyyy(yesterday)}.xlsx"

    if os.path.exists(f_today):
        print(f"[OK] Usando arquivo existente: {f_today}")
        return f_today
    if os.path.exists(f_yest):
        print(f"[OK] Usando arquivo do dia anterior: {f_yest}")
        return f_yest

    print("[DL] Baixando resultados mais recentes da CAIXA...")
    resp = requests.get(CAIXA_URL, timeout=45, verify=False)
    if resp.status_code != 200 or not resp.content:
        raise SystemExit(f"ERRO: download falhou (HTTP {resp.status_code}).")

    with open(f_today, "wb") as f:
        f.write(resp.content)

    print(f"[OK] Arquivo salvo como {f_today}")
    return f_today

def load_draws_from_xlsx(path: str, sheet_preference: str = "LOTOFÁCIL") -> List[Draw]:
    wb = load_workbook(path, data_only=True)
    sheet_name = sheet_preference if sheet_preference in wb.sheetnames else wb.sheetnames[0]
    ws = wb[sheet_name]

    header_row, cols = _find_header_row(ws)

    draws: List[Draw] = []
    r = header_row + 1
    while True:
        conc = ws.cell(r, cols["concurso"]).value
        if conc is None:
            break
        try:
            concurso = int(str(conc).strip())
        except Exception:
            r += 1
            continue

        d = _parse_date_br(ws.cell(r, cols.get("data", 0)).value) if "data" in cols else None
        # fallback: tenta coluna 2
        if d is None:
            d = _parse_date_br(ws.cell(r, 2).value)
        if d is None:
            # sem data, pula linha
            r += 1
            continue

        dezenas: List[int] = []
        ok = True
        for i in range(1, 16):
            v = ws.cell(r, cols[f"bola{i}"]).value
            try:
                n = int(v)
            except Exception:
                ok = False
                break
            if not (1 <= n <= 25):
                ok = False
                break
            dezenas.append(n)

        if ok:
            dezenas_sorted = tuple(sorted(dezenas))
            rateios: Dict[int, float] = {}
            for k in range(11, 16):
                ck = cols.get(f"rateio{k}")
                if ck:
                    rateios[k] = _parse_money_br(ws.cell(r, ck).value)
            draws.append(Draw(concurso=concurso, data=d, dezenas=dezenas_sorted, rateios=rateios))

        r += 1

    draws.sort(key=lambda x: x.concurso)
    return draws

    
def payout_for_hits(draw: Draw, hits: int) -> float:
    return float(draw.rateios.get(hits, 0.0))

def infer_aposta15_custo(draw: Draw, fallback: float = 3.50) -> float:
    # Regra do seu projeto: prêmio de 11 acertos = 2x valor da aposta (15 dezenas)
    v11 = draw.rateios.get(11)
    if v11 and v11 > 0:
        return float(v11) / 2.0
    return float(fallback)



# =========================
# ABCD logic (jogos + gate + campanhas)
# =========================

def _freq_rank(draws: List[Draw], window: int = 200) -> List[int]:
    hist = draws[-window:] if len(draws) > window else draws[:]
    freq = {n: 0 for n in range(1, 26)}
    for d in hist:
        for n in d.dezenas:
            freq[n] += 1
    # mais frequente primeiro; desempate por n menor
    return [n for n, _ in sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))]

def _delay_rank(draws: List[Draw], window: int = 200) -> List[int]:
    hist = draws[-window:] if len(draws) > window else draws[:]
    last_pos = {n: None for n in range(1, 26)}
    for idx, d in enumerate(hist):
        for n in d.dezenas:
            last_pos[n] = idx
    # maior atraso primeiro (None => muito atrasado)
    def delay(n: int) -> int:
        lp = last_pos[n]
        return 10_000 if lp is None else (len(hist) - 1 - lp)
    return sorted(range(1, 26), key=lambda n: (-delay(n), n))

def _pick_unique(base: List[int], k: int, exclude: Set[int]) -> List[int]:
    out = []
    for n in base:
        if n in exclude:
            continue
        out.append(n)
        exclude.add(n)
        if len(out) >= k:
            break
    return out

def build_abcd_games(draws: List[Draw]) -> Dict[str, List[int]]:
    """
    Gera 4 jogos de 15 dezenas:
      AB, AC, AD, BCD

    Heurística determinística baseada em:
      - frequência recente
      - atraso recente
      - mistura (freq + atraso)
    """
    random.seed(RNG_SEED + (draws[-1].concurso if draws else 0))

    freq = _freq_rank(draws, window=200)
    delay = _delay_rank(draws, window=200)

    # mistura simples: interleave
    mixed: List[int] = []
    seen = set()
    for a, b in zip(freq, delay):
        if a not in seen:
            mixed.append(a); seen.add(a)
        if b not in seen:
            mixed.append(b); seen.add(b)
    for n in range(1, 26):
        if n not in seen:
            mixed.append(n)

    def mk_AB() -> List[int]:
        ex: Set[int] = set()
        a = _pick_unique(freq, 8, ex)
        b = _pick_unique(delay, 7, ex)
        return sorted(a + b)

    def mk_AC() -> List[int]:
        ex: Set[int] = set()
        a = _pick_unique(freq, 8, ex)
        c = _pick_unique(mixed, 7, ex)
        return sorted(a + c)

    def mk_AD() -> List[int]:
        ex: Set[int] = set()
        a = _pick_unique(freq, 8, ex)
        # escolhe 7 aleatórios ponderados por mixed (mais no topo)
        pool = [n for n in mixed if n not in ex]
        weights = list(reversed(range(1, len(pool) + 1)))
        chosen = set()
        while len(chosen) < 7 and pool:
            n = random.choices(pool, weights=weights, k=1)[0]
            chosen.add(n)
            idx = pool.index(n)
            pool.pop(idx); weights.pop(idx)
        return sorted(a + sorted(chosen))

    def mk_BCD() -> List[int]:
        ex: Set[int] = set()
        b = _pick_unique(delay, 5, ex)
        c = _pick_unique(mixed, 5, ex)
        d = _pick_unique(freq, 5, ex)
        return sorted(b + c + d)

    return {
        "AB": mk_AB(),
        "AC": mk_AC(),
        "AD": mk_AD(),
        "BCD": mk_BCD(),
    }

def _parse_percentiles(s: str, default: Tuple[float, float]) -> Tuple[float, float]:
    if not s:
        return default
    parts = re.split(r"[,\s;]+", s.strip())
    parts = [p for p in parts if p]
    if len(parts) == 1:
        p = float(parts[0])
        return (p, p)
    p1 = float(parts[0]); p2 = float(parts[1])
    return (min(p1, p2), max(p1, p2))

def _percentile(values: List[int], p: float) -> float:
    """
    Percentil simples sem numpy. p em [0,100].
    """
    if not values:
        return 0.0
    xs = sorted(values)
    if p <= 0:
        return float(xs[0])
    if p >= 100:
        return float(xs[-1])
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return float(xs[f])
    d0 = xs[f] * (c - k)
    d1 = xs[c] * (k - f)
    return float(d0 + d1)


def compute_abcd_gate_stats(
    draws: List[Draw],
    janela_recente: int,
    teimosinha_n: int,
    min_hits: int,
    gate_percentis: Tuple[float, float],
) -> Dict[str, object]:
    """
    Replica a lógica do seu v18:

    - walk-forward: para cada i, gera 4 jogos ABCD usando histórico draws[:i]
    - simula teimosinha_n concursos (i..i+teimosinha_n-1) jogando os 4 jogos
    - sucesso do "dia i" = profit_total > 0.0 (payout_total - custo_total)
    - gaps = diferenças entre índices de sucessos
    - gap_atual = last_eval_idx - last_succ, onde last_eval_idx = len(draws) - teimosinha_n
    - gate PASS se gap_atual estiver entre [P_low, P_high] (inclusive)
    """
    if len(draws) < 10:
        return {"gate": {"pass": False, "reason": "Poucos concursos"}, "gaps": [], "gap_atual": None, "lo": 0.0, "hi": 0.0}

    success_idx: List[int] = []
    gaps: List[int] = []

    # varre todos os dias elegíveis (precisa de i+r existir)
    last_start = len(draws) - (teimosinha_n - 1)
    for i in range(1, last_start):
        history = draws[:i]
        games = build_abcd_games(history, janela_recente=janela_recente)

        total_cost = 0.0
        total_payout = 0.0

        for r in range(teimosinha_n):
            alvo = draws[i + r]
            custo15 = infer_aposta15_custo(alvo, fallback=3.50)
            total_cost += 4.0 * custo15  # 4 jogos (AB/AC/AD/BCD)
            alvo_set = alvo.set

            payout_r = 0.0
            for jogo in games.values():
                k = len(set(jogo) & alvo_set)
                if k >= min_hits:
                    payout_r += payout_for_hits(alvo, k)
            total_payout += payout_r

        if (total_payout - total_cost) > 0.0:
            success_idx.append(i)

    for a, b in zip(success_idx[:-1], success_idx[1:]):
        gaps.append(b - a)

    if not gaps:
        return {"gate": {"pass": False, "reason": "Sem sucessos suficientes"}, "gaps": [], "gap_atual": None, "lo": 0.0, "hi": 0.0}

    p_low, p_high = gate_percentis
    lo = float(_percentile(gaps, p_low))
    hi = float(_percentile(gaps, p_high))

    last_eval_idx = len(draws) - teimosinha_n
    last_succ = None
    for idx_s in reversed(success_idx):
        if idx_s <= last_eval_idx:
            last_succ = idx_s
            break

    gap_now = (last_eval_idx - last_succ) if last_succ is not None else None
    gate_pass = False
    if gap_now is not None:
        gate_pass = (gap_now >= lo and gap_now <= hi)

    return {
        "gate": {
            "metric": "concursos",
            "percentis": gate_percentis,
            "faixa": (round(lo, 2), round(hi, 2)),
            "gap_atual": gap_now,
            "pass": bool(gate_pass),
        },
        "gaps": gaps,
        "gap_atual": gap_now,
        "lo": lo,
        "hi": hi,
    }


def _hits(game: List[int], draw: Draw) -> int:
    return len(set(game) & draw.set)

def _fmt_game(nums: List[int]) -> str:
    return " ".join(f"{n:02d}" for n in sorted(nums))

def _ensure_dirs():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def _load_campaign_state() -> Dict[str, object]:
    if not STATE_CAMPAIGNS.exists():
        return {"version": 1, "updated_at": None, "campaigns": []}
    try:
        return json.loads(STATE_CAMPAIGNS.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "updated_at": None, "campaigns": []}

def _save_campaign_state(state: Dict[str, object]):
    state["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    STATE_CAMPAIGNS.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def _campaign_id(start_concurso: int, created_utc: str) -> str:
    return f"c{start_concurso}_{created_utc.replace(':','').replace('-','')}"

def update_campaigns(draws: List[Draw], teimosinha: int, min_hits_stop: int) -> Dict[str, object]:
    """
    Atualiza campanhas ativas com base nos concursos disponíveis no XLSX.
    Regras:
      - Uma campanha nasce quando gate_pass=True (para o concurso seguinte).
      - Pode haver múltiplas campanhas simultâneas.
      - Para cada campanha, avalia hits nos concursos desde start_concurso,
        até completar teimosinha dias (concursos) ou atingir min_hits_stop.
      - Encerrar se: best_hits >= min_hits_stop OU offset >= teimosinha
    """
    state = _load_campaign_state()
    campaigns = state.get("campaigns", [])
    if not isinstance(campaigns, list):
        campaigns = []

    by_concurso = {d.concurso: d for d in draws}
    max_concurso = draws[-1].concurso if draws else 0

    for c in campaigns:
        if not isinstance(c, dict):
            continue
        if c.get("status") != "active":
            continue

        start = int(c.get("start_concurso", 0))
        games = c.get("games", {})
        if not games or start <= 0:
            c["status"] = "closed"
            c["closed_reason"] = "invalid_state"
            continue

        # avalia do start até max disponível, mas limitado ao offset teimosinha-1
        start_idx = start
        end_idx = min(max_concurso, start + teimosinha - 1)

        best_hits = int(c.get("best_hits", 0))
        best_when = c.get("best_when")
        best_offset = c.get("best_offset")
        daily: List[Dict[str, object]] = c.get("daily", [])
        if not isinstance(daily, list):
            daily = []

        # evita reprocessar concursos já processados
        processed = {int(x.get("concurso")) for x in daily if isinstance(x, dict) and x.get("concurso") is not None}

        for conc in range(start_idx, end_idx + 1):
            if conc in processed:
                continue
            d = by_concurso.get(conc)
            if not d:
                continue  # ainda não chegou no XLSX
            # hits: melhor entre 4 jogos
            hits_map = {k: _hits(v, d) for k, v in games.items()}
            day_best = max(hits_map.values()) if hits_map else 0

            entry = {
                "concurso": conc,
                "data": d.data.isoformat(),
                "best_hits": day_best,
                "hits": hits_map,
                "offset": conc - start,
            }
            daily.append(entry)

            if day_best > best_hits:
                best_hits = day_best
                best_when = d.data.isoformat()
                best_offset = conc - start

        c["daily"] = daily
        c["best_hits"] = best_hits
        c["best_when"] = best_when
        c["best_offset"] = best_offset

        # encerra?
        # 1) atingiu min_hits_stop
        if best_hits >= min_hits_stop:
            c["status"] = "closed"
            c["closed_reason"] = f"hit_stop>={min_hits_stop}"
            c["closed_at_concurso"] = max((x.get("concurso", 0) for x in daily), default=None)
        else:
            # 2) expirou janela completa e já temos dados suficientes
            # se já processou o último concurso da janela, fecha
            if any(isinstance(x, dict) and int(x.get("concurso", 0)) == end_idx for x in daily):
                c["status"] = "closed"
                c["closed_reason"] = f"teimosinha_end({teimosinha})"
                c["closed_at_concurso"] = end_idx

    state["campaigns"] = campaigns
    _save_campaign_state(state)
    return state

def maybe_start_campaign(state: Dict[str, object], gate_pass: bool, last_concurso: int, games: Dict[str, List[int]], teimosinha: int, min_hits_stop: int) -> Optional[Dict[str, object]]:
    """
    Se gate_pass=True: cria nova campanha para start_concurso = last_concurso+1.
    Não bloqueia campanhas existentes (pode haver mais de uma ativa).
    """
    if not gate_pass:
        return None

    start = last_concurso + 1
    created_utc = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    cid = _campaign_id(start, created_utc)

    campaign = {
        "id": cid,
        "status": "active",
        "created_at": created_utc,
        "start_concurso": start,
        "teimosinha_n": teimosinha,
        "min_hits_stop": min_hits_stop,
        "games": {k: list(v) for k, v in games.items()},
        "best_hits": 0,
        "best_when": None,
        "best_offset": None,
        "daily": [],
    }

    campaigns = state.get("campaigns", [])
    if not isinstance(campaigns, list):
        campaigns = []
    campaigns.append(campaign)
    state["campaigns"] = campaigns
    _save_campaign_state(state)
    return campaign

def write_daily_snapshot(run_date: date, payload: Dict[str, object]) -> Path:
    y = f"{run_date:%Y}"
    m = f"{run_date:%m}"
    d = f"{run_date:%Y-%m-%d}"
    out_dir = RESULTS_DIR / y / m
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{d}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path

def build_email_body(sig: Dict[str, object]) -> str:
    """
    Produz email_body.txt (texto simples) com:
      - resumo do gate
      - 4 jogos
      - campanhas ativas (se houver)
    """
    lines = []
    lines.append("Lotofácil ABCD — Sinal diário")
    lines.append("")
    lines.append(f"gate_pass (real): {sig.get('gate_pass')}")
    lines.append(f"Último concurso: {sig.get('last_concurso')} | Data: {sig.get('last_data')}")
    lines.append(f"Percentis: {sig.get('gate_percentis')} | Faixa: {sig.get('gate_faixa')} | Gap atual: {sig.get('gate_gap_atual')}")
    lines.append("")

    lines.append("4 jogos (15 dezenas):")
    lines.append("Jogos retornados pelo JSON:")
    for k, v in (sig.get("jogos", {}) or {}).items():
        lines.append(f"{k}: {v}")
    lines.append("")

    camps = sig.get("campaigns_active", []) or []
    if camps:
        lines.append("Campanhas ativas:")
        for c in camps:
            lines.append(f"- {c.get('id')} | start={c.get('start_concurso')} | best_hits={c.get('best_hits')} | best_when={c.get('best_when')} | status={c.get('status')}")
    else:
        lines.append("Campanhas ativas: (nenhuma)")

    return "\n".join(lines)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--teimosinha", type=int, default=37)
    ap.add_argument("--min_hits_stop", type=int, default=14)
    ap.add_argument("--gate_percentis", type=str, default="25,75")
    ap.add_argument("--overlap_event", type=int, default=9)
    ap.add_argument("--force_email", action="store_true", help="Cria email_body.txt mesmo com gate_pass=False (para teste).")
    args = ap.parse_args()

    _ensure_dirs()

    xlsx = ensure_results_file()
    print(f"Resultados XLSX: {xlsx} | Aba: LOTOFÁCIL")

    draws = load_draws_from_xlsx(xlsx, sheet_preference="LOTOFÁCIL")
    if not draws:
        raise SystemExit("ERRO: não carreguei nenhum concurso do XLSX.")

    last = draws[-1]
    print("")
    print("=== SINAL DIÁRIO ABCD ===")
    print(f"Último concurso: {last.concurso} | Data: {last.data.isoformat()}")

    percentis = _parse_percentiles(args.gate_percentis, default=(25.0, 75.0))
    stats_gate = compute_abcd_gate_stats(
        draws=draws,
        janela_recente=200,
        teimosinha_n=int(args.teimosinha),
        min_hits=int(args.min_hits_stop),
        gate_percentis=percentis,
    )
    gate = stats_gate.get("gate", {})
    print(f"[GATE ABCD] metric=concursos | percentis={tuple(gate['percentis'])} | faixa=({gate['lo']}, {gate['hi']}) conc | gap_atual={gate['gap_atual']} conc | PASS={gate['gate_pass']}")

    games = build_abcd_games(draws)
    print("Jogos sugeridos (15 dezenas):")
    for k, v in games.items():
        print(f"  {k}: {_fmt_game(v)}")

    # Atualiza campanhas existentes com base no XLSX atual
    state = update_campaigns(draws, teimosinha=int(args.teimosinha), min_hits_stop=int(args.min_hits_stop))

    # Se gate_pass, cria nova campanha (para o próximo concurso)
    new_campaign = maybe_start_campaign(
        state=state,
        gate_pass=bool(gate["gate_pass"]),
        last_concurso=int(last.concurso),
        games=games,
        teimosinha=int(args.teimosinha),
        min_hits_stop=int(args.min_hits_stop),
    )

    # lista campanhas ativas para o JSON/email
    campaigns_active = [c for c in (state.get("campaigns", []) or []) if isinstance(c, dict) and c.get("status") == "active"]
    # adiciona a recém-criada (já estará ativa no state)
    _ = new_campaign

    sig = {
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "last_concurso": last.concurso,
        "last_data": last.data.isoformat(),
        "gate_pass": bool(gate["gate_pass"]),
        "gate_metric": "concursos",
        "gate_percentis": gate["percentis"],
        "gate_faixa": [gate["lo"], gate["hi"]],
        "gate_gap_atual": gate["gap_atual"],
        "gate_event_overlap": gate["event_overlap"],
        "jogos": {k: _fmt_game(v) for k, v in games.items()},
        "jogos_raw": {k: list(map(int, v)) for k, v in games.items()},
        "campaigns_active": [
            {
                "id": c.get("id"),
                "status": c.get("status"),
                "created_at": c.get("created_at"),
                "start_concurso": c.get("start_concurso"),
                "teimosinha_n": c.get("teimosinha_n"),
                "min_hits_stop": c.get("min_hits_stop"),
                "best_hits": c.get("best_hits"),
                "best_when": c.get("best_when"),
                "best_offset": c.get("best_offset"),
            }
            for c in campaigns_active
        ],
        "campaigns_total": len(state.get("campaigns", []) or []),
    }

    ROOT_SIGNAL_JSON.write_text(json.dumps(sig, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"JSON: {ROOT_SIGNAL_JSON}")

    # Snapshot diário (UTC date)
    snap = write_daily_snapshot(run_date=date.today(), payload=sig)
    print(f"Snapshot: {snap}")

    # Email body: somente quando PASS=True (ou force_email p/ teste)
    if sig["gate_pass"] or args.force_email:
        EMAIL_BODY_TXT.write_text(build_email_body(sig), encoding="utf-8")
        print(f"Email body: {EMAIL_BODY_TXT}")
    else:
        if EMAIL_BODY_TXT.exists():
            EMAIL_BODY_TXT.unlink()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())