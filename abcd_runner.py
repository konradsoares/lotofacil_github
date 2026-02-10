# abcd_runner.py
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from openpyxl import load_workbook


SCRIPT = "fechamento_simples_v11_54_abcd_gate_plus_FIXED19.py"

DOCS_DIR = Path("docs")
RESULTS_DIR = DOCS_DIR / "results"
STATE_DIR = DOCS_DIR / "state"
STATE_PATH = STATE_DIR / "abcd_campaigns.json"

EMAIL_BODY_PATH = Path("email_body.txt")

# -------- XLSX parsing (CAIXA download format is stable enough) --------

def _norm_header(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _find_header_row(ws) -> Tuple[int, Dict[str, int]]:
    """
    Finds a header row that contains 'Concurso' and 15 ball columns.
    Returns (row_index_1based, col_map) where col_map keys:
      concurso, data, b1..b15
    """
    # Search first 50 rows for a plausible header
    for r in range(1, 51):
        row = [ws.cell(row=r, column=c).value for c in range(1, 60)]
        norm = [_norm_header(str(v)) if v is not None else "" for v in row]

        # Concurso col
        try:
            c_conc = norm.index("concurso") + 1
        except ValueError:
            continue

        # Data col (sometimes "data sorteio" or "data")
        c_date = None
        for cand in ("data sorteio", "data"):
            if cand in norm:
                c_date = norm.index(cand) + 1
                break

        # ball cols: "bola 1".."bola 15" OR "b 1" etc.
        ball_cols = {}
        for i in range(1, 16):
            patterns = [
                f"bola {i}",
                f"bolas {i}",
                f"dezena {i}",
                f"{i}ª dezena",
                f"b{i}",
                f"b {i}",
            ]
            found = None
            for p in patterns:
                if p in norm:
                    found = norm.index(p) + 1
                    break
            if found is None:
                ball_cols = {}
                break
            ball_cols[f"b{i}"] = found

        if ball_cols:
            col_map = {"concurso": c_conc}
            if c_date:
                col_map["data"] = c_date
            col_map.update(ball_cols)
            return r, col_map

    raise RuntimeError("Não consegui localizar o header do XLSX (Concurso/Bola1..Bola15).")

@dataclass
class Draw:
    concurso: int
    data: Optional[str]
    nums: Set[int]

def load_draws_from_xlsx(xlsx_path: str, sheet_name: str = "LOTOFÁCIL") -> List[Draw]:
    wb = load_workbook(xlsx_path, data_only=True)
    if sheet_name not in wb.sheetnames:
        # fallback: first sheet
        ws = wb[wb.sheetnames[0]]
    else:
        ws = wb[sheet_name]

    header_row, cols = _find_header_row(ws)

    draws: List[Draw] = []
    r = header_row + 1
    while True:
        v_conc = ws.cell(row=r, column=cols["concurso"]).value
        if v_conc is None:
            break
        try:
            concurso = int(v_conc)
        except Exception:
            r += 1
            continue

        # date
        dval = ws.cell(row=r, column=cols.get("data", cols["concurso"] + 1)).value if "data" in cols else None
        data_str = None
        if isinstance(dval, datetime):
            data_str = dval.date().isoformat()
        elif isinstance(dval, date):
            data_str = dval.isoformat()
        elif isinstance(dval, str) and dval.strip():
            # may be "07/02/2026"
            s = dval.strip()
            m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
            if m:
                data_str = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            else:
                data_str = s

        nums: Set[int] = set()
        ok = True
        for i in range(1, 16):
            v = ws.cell(row=r, column=cols[f"b{i}"]).value
            if v is None:
                ok = False
                break
            try:
                nums.add(int(v))
            except Exception:
                ok = False
                break

        if ok and len(nums) == 15:
            draws.append(Draw(concurso=concurso, data=data_str, nums=nums))

        r += 1

    draws.sort(key=lambda d: d.concurso)
    return draws

# -------- campaigns state --------

def load_state() -> Dict:
    if not STATE_PATH.exists():
        return {"version": 1, "updated_at": None, "campaigns": []}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))

def save_state(state: Dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def campaign_key(start_concurso: int, created_on: str) -> str:
    return f"c_{start_concurso}_{created_on.replace('-','')}"

def ensure_snapshot_dirs(ymd: str) -> Path:
    yyyy, mm, _ = ymd.split("-")
    outdir = RESULTS_DIR / yyyy / mm
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def write_daily_snapshot(sig: Dict, ymd: str) -> Path:
    outdir = ensure_snapshot_dirs(ymd)
    outpath = outdir / f"{ymd}.json"
    outpath.write_text(json.dumps(sig, ensure_ascii=False, indent=2), encoding="utf-8")
    return outpath

# -------- logic --------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--teimosinha", type=int, default=37)
    ap.add_argument("--min_hits_stop", type=int, default=14)
    ap.add_argument("--gate_percentis", type=str, default="25,75")
    ap.add_argument("--script", type=str, default=SCRIPT)
    ap.add_argument("--xlsx", type=str, default="", help="Opcional: path fixo do XLSX (senão usa auto do script).")
    return ap.parse_args()

def run_daily_signal(script: str, teimosinha: int, min_hits_stop: int, gate_percentis: str) -> Dict:
    # Gera abcd_signal.json via seu script
    cmd = [
        "python", script,
        "--abcd_daily_signal",
        "--abcd_teimosinha_n", str(teimosinha),
        "--abcd_min_hits", str(min_hits_stop),
        "--abcd_gate_percentis", gate_percentis,
        "--abcd_daily_json", "abcd_signal.json",
    ]
    subprocess.check_call(cmd)
    return json.loads(Path("abcd_signal.json").read_text(encoding="utf-8"))

def find_latest_xlsx() -> Optional[str]:
    # pega o resultados_*.xlsx mais recente no diretório atual
    files = sorted(Path(".").glob("resultados_*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    return str(files[0]) if files else None

def parse_game_nums(s: str) -> Set[int]:
    # "02 05 06 ..." -> {2,5,6,...}
    nums = set()
    for tok in re.split(r"[,\s]+", (s or "").strip()):
        if not tok:
            continue
        try:
            nums.add(int(tok))
        except Exception:
            pass
    return nums

def compute_hits(game: Set[int], draw_nums: Set[int]) -> int:
    return len(game.intersection(draw_nums))

def check_campaign_against_draw(camp: Dict, draw: Draw) -> Dict:
    jogos = camp.get("jogos", {}) or {}
    # normalize: accept any keys/values in JSON
    parsed_games: List[Tuple[str, Set[int]]] = []
    for k, v in jogos.items():
        parsed_games.append((str(k), parse_game_nums(str(v))))

    best_hits = -1
    best_key = None
    per_game = []
    for k, gnums in parsed_games:
        h = compute_hits(gnums, draw.nums)
        per_game.append({"game": k, "hits": h})
        if h > best_hits:
            best_hits = h
            best_key = k

    return {
        "concurso": draw.concurso,
        "data": draw.data,
        "best_hits": best_hits,
        "best_game": best_key,
        "per_game": per_game,
    }

def already_checked(camp: Dict, concurso: int) -> bool:
    for chk in camp.get("checks", []) or []:
        if chk.get("concurso") == concurso:
            return True
    return False

def within_offset_window(camp: Dict, concurso: int) -> bool:
    start = int(camp["target_start_concurso"])
    n = int(camp["teimosinha_n"])
    end = start + n - 1
    return start <= concurso <= end

def checks_done_in_window(camp: Dict) -> int:
    return len(camp.get("checks", []) or [])

def build_email_digest(
    sig: Dict,
    today_ymd: str,
    latest_draw: Draw,
    opened: List[Dict],
    updates: List[Dict],
    won: List[Dict],
    expired: List[Dict],
    active: List[Dict],
) -> str:
    gate = sig.get("gate", {}) or {}

    lines = []
    lines.append("Lotofácil ABCD — Resumo Diário")
    lines.append("")
    lines.append(f"Rodado em (Dublin 04:30): {today_ymd}")
    lines.append(f"Último concurso disponível: {latest_draw.concurso} | Data: {latest_draw.data}")
    lines.append("")
    lines.append(f"gate_pass (hoje): {sig.get('gate_pass')}")
    lines.append(f"Percentis: {gate.get('percentis')} | Faixa: {gate.get('faixa')} | Gap atual: {gate.get('gap_atual')}")
    lines.append("")

    if opened:
        lines.append("=== NOVAS CAMPANHAS ABERTAS HOJE ===")
        for c in opened:
            lines.append(f"- {c['id']} | start={c['start_concurso']} -> alvo_início={c['target_start_concurso']} | teimosinha={c['teimosinha_n']}")
        lines.append("")

    if won:
        lines.append("=== CAMPANHAS ENCERRADAS (GANHOU >=14) ===")
        for c in won:
            w = c.get("won", {}) or {}
            lines.append(f"- {c['id']} | start={c['start_concurso']} | ganhou no concurso {w.get('when_concurso')} com {w.get('best_hits')} hits")
        lines.append("")

    if expired:
        lines.append("=== CAMPANHAS ENCERRADAS (EXPIRADAS) ===")
        for c in expired:
            lines.append(f"- {c['id']} | start={c['start_concurso']} | expirou após {c.get('teimosinha_n')} concursos")
        lines.append("")

    if updates:
        lines.append("=== CHECKS DE HOJE (por campanha) ===")
        for u in updates:
            cid = u["id"]
            chk = u["check"]
            lines.append(f"- {cid} | concurso {chk['concurso']} | best_hits={chk['best_hits']} | best_game={chk['best_game']}")
        lines.append("")

    if active:
        lines.append("=== CAMPANHAS ATIVAS (lembrete) ===")
        for c in active:
            done = checks_done_in_window(c)
            total = int(c["teimosinha_n"])
            rem = max(0, total - done)
            last_chk = (c.get("checks", []) or [])[-1] if (c.get("checks") or []) else None
            lines.append(f"- {c['id']} | start={c['start_concurso']} -> alvo={c['target_start_concurso']} | surpresinha {done}/{total} | remaining={rem}")
            if last_chk:
                lines.append(f"  último: concurso {last_chk.get('concurso')} | best_hits={last_chk.get('best_hits')} | best_game={last_chk.get('best_game')}")
            # jogos
            lines.append("  Jogos (do JSON da campanha):")
            for k, v in (c.get("jogos", {}) or {}).items():
                lines.append(f"    {k}: {v}")
        lines.append("")

    return "\n".join(lines)

def main() -> int:
    args = parse_args()

    sig = run_daily_signal(args.script, args.teimosinha, args.min_hits_stop, args.gate_percentis)

    # Determine "today" for snapshot naming: use last_data if present, else UTC date
    ymd = sig.get("last_data") or datetime.utcnow().date().isoformat()
    write_daily_snapshot(sig, ymd)

    # Find XLSX saved by your script auto-download flow
    xlsx = args.xlsx.strip() or find_latest_xlsx()
    if not xlsx:
        raise RuntimeError("Não encontrei resultados_*.xlsx no workspace após rodar o daily_signal.")

    draws = load_draws_from_xlsx(xlsx)
    if not draws:
        raise RuntimeError("Não consegui carregar concursos do XLSX.")
    latest = draws[-1]
    draw_by_concurso = {d.concurso: d for d in draws}

    state = load_state()
    campaigns: List[Dict] = state.get("campaigns", []) or []

    today_created_on = ymd
    opened: List[Dict] = []
    won: List[Dict] = []
    expired: List[Dict] = []
    updates: List[Dict] = []

    # A) Open new campaign if gate_pass=True today
    if bool(sig.get("gate_pass")):
        start_conc = int(sig.get("last_concurso"))
        target_start = start_conc + 1
        cid = campaign_key(start_conc, today_created_on)

        # dedupe by start_concurso OR target_start_concurso
        exists = False
        for c in campaigns:
            if int(c.get("start_concurso", -1)) == start_conc or int(c.get("target_start_concurso", -1)) == target_start:
                exists = True
                break

        if not exists:
            new_c = {
                "id": cid,
                "status": "active",
                "created_on": today_created_on,
                "start_concurso": start_conc,
                "target_start_concurso": target_start,
                "teimosinha_n": int(args.teimosinha),
                "min_hits_stop": int(args.min_hits_stop),
                "jogos": sig.get("jogos", {}) or {},
                "checks": [],
                "won": {"when_concurso": None, "best_hits": None, "best_game": None},
            }
            campaigns.append(new_c)
            opened.append(new_c)

    # B) Evaluate all active campaigns against latest concurso (offset window)
    for c in campaigns:
        if c.get("status") != "active":
            continue

        if not within_offset_window(c, latest.concurso):
            # not in window yet (campaign started but target not reached) OR already beyond window
            # if beyond window and no win -> expire
            start = int(c["target_start_concurso"])
            end = start + int(c["teimosinha_n"]) - 1
            if latest.concurso > end:
                c["status"] = "expired"
                expired.append(c)
            continue

        if already_checked(c, latest.concurso):
            continue

        chk = check_campaign_against_draw(c, latest)
        c.setdefault("checks", []).append(chk)
        updates.append({"id": c["id"], "check": chk})

        if int(chk["best_hits"]) >= int(c["min_hits_stop"]):
            c["status"] = "won"
            c["won"] = {
                "when_concurso": chk["concurso"],
                "best_hits": chk["best_hits"],
                "best_game": chk["best_game"],
            }
            won.append(c)
        else:
            # if used all offsets after appending this check, expire
            if checks_done_in_window(c) >= int(c["teimosinha_n"]):
                c["status"] = "expired"
                expired.append(c)

    # active campaigns after updates
    active = [c for c in campaigns if c.get("status") == "active"]

    # Save state
    state["campaigns"] = campaigns
    save_state(state)

    # Decide email policy:
    # Send if:
    # - opened today OR
    # - any active OR
    # - any won/expired today
    should_email = bool(opened or active or won or expired)

    if should_email:
        body = build_email_digest(
            sig=sig,
            today_ymd=ymd,
            latest_draw=latest,
            opened=opened,
            updates=updates,
            won=won,
            expired=expired,
            active=active,
        )
        EMAIL_BODY_PATH.write_text(body, encoding="utf-8")
        print("[EMAIL] will_send=true")
    else:
        print("[EMAIL] will_send=false")

    # expose a tiny output JSON for workflow consumption if you want later
    Path("runner_out.json").write_text(
        json.dumps(
            {
                "ymd": ymd,
                "latest_concurso": latest.concurso,
                "email": should_email,
                "opened": len(opened),
                "active": len(active),
                "won": len(won),
                "expired": len(expired),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
