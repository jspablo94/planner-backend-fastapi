from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from datetime import date, time, datetime
from typing import List, Optional, Set, Tuple

app = FastAPI(title="Planner Manutenção - Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Armazenamento em memória (MVP)
# -----------------------------
ordens = []
programacoes = []

TIPOS_SERVICO_SUGERIDOS = [
    "Altura",
    "Espaço Confinado",
    "Trabalho a quente",
    "Trabalho elétrico",
    "Terceirizado",
]

STATUS_VALIDOS = ["Planejado", "Em execução", "Concluído", "Reprogramado"]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def pick_col(cols: List[str], options: List[str]) -> Optional[str]:
    for opt in options:
        if opt in cols:
            return opt
    return None


def parse_executantes_free_text(s: Optional[str]) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in str(s).replace(";", ",").split(",")]
    return [p for p in parts if p]


def normalize_name(name: str) -> str:
    return " ".join((name or "").strip().lower().split())


def get_exec_set(executantes_texto: str) -> Set[str]:
    return set(normalize_name(x) for x in parse_executantes_free_text(executantes_texto) if normalize_name(x))


def time_to_minutes(t: time) -> int:
    return t.hour * 60 + t.minute


def hhmm_to_minutes(hhmm: str) -> int:
    # "08:30" -> 510
    try:
        h, m = hhmm.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 0


def interval_from_prog(horario_inicio_hhmm: str, duracao_min: Optional[int]) -> Tuple[int, int, bool]:
    """
    Retorna (ini, fim, has_duration).
    Se duracao_min não vier, has_duration=False e fim=ini.
    """
    ini = hhmm_to_minutes(horario_inicio_hhmm)
    if duracao_min is None:
        return ini, ini, False
    # duração 0 ou negativa vira "sem duração" na prática
    if duracao_min <= 0:
        return ini, ini, False
    return ini, ini + duracao_min, True


def interval_from_request(horario: time, duracao_min: Optional[int]) -> Tuple[int, int, bool]:
    ini = time_to_minutes(horario)
    if duracao_min is None or duracao_min <= 0:
        return ini, ini, False
    return ini, ini + duracao_min, True


def overlaps(a_ini: int, a_fim: int, a_has: bool, b_ini: int, b_fim: int, b_has: bool) -> bool:
    """
    Regra:
    - Se ambos têm duração -> conflito se intervalos se sobrepõem
    - Se um ou ambos não têm duração -> conflito só se horário inicial for igual
    """
    if a_has and b_has:
        # intervalo semi-aberto [ini, fim)
        return not (a_fim <= b_ini or b_fim <= a_ini)
    # sem duração: só conflito se mesma hora de início
    return a_ini == b_ini


def conflitos_execucao_regra_b(
    data_iso: str,
    executantes_texto: str,
    novo_horario: time,
    nova_duracao_min: Optional[int],
    ignorar_prog_id: Optional[int] = None,
):
    alvo_execs = get_exec_set(executantes_texto or "")
    if not alvo_execs:
        return []

    a_ini, a_fim, a_has = interval_from_request(novo_horario, nova_duracao_min)

    conflitos = []
    for p in programacoes:
        if ignorar_prog_id is not None and p.get("id") == ignorar_prog_id:
            continue
        if p.get("data") != data_iso:
            continue

        b_execs = get_exec_set(p.get("executantes_texto", "") or "")
        inter_execs = sorted(list(alvo_execs.intersection(b_execs)))
        if not inter_execs:
            continue

        b_ini, b_fim, b_has = interval_from_prog(p.get("horario_inicio", "00:00"), p.get("duracao_min"))
        if overlaps(a_ini, a_fim, a_has, b_ini, b_fim, b_has):
            conflitos.append({
                "programacao_id": p.get("id"),
                "numero_os": p.get("numero_os"),
                "horario_inicio": p.get("horario_inicio"),
                "duracao_min": p.get("duracao_min"),
                "executantes_em_conflito": inter_execs
            })

    return conflitos


# -----------------------------
# Importação (Excel/CSV)
# -----------------------------
@app.post("/importar-excel")
async def importar_excel(file: UploadFile = File(...)):
    try:
        filename = (file.filename or "").lower()
        raw = await file.read()

        if filename.endswith(".csv"):
            df = pd.read_csv(pd.io.common.BytesIO(raw))
        elif filename.endswith(".xls"):
            df = pd.read_excel(pd.io.common.BytesIO(raw), engine="xlrd")
        else:
            df = pd.read_excel(pd.io.common.BytesIO(raw), engine="openpyxl")

        df = normalize_columns(df)
        cols = df.columns.tolist()

        col_os = pick_col(cols, ["OS", "Ordem", "Ordem de Serviço", "Numero OS", "Número OS"])
        col_desc = pick_col(cols, ["Descricao", "Descrição", "Descrição Curta", "Descrição do Serviço", "Descrição da OS", "1 Descrição"])
        col_tipo = pick_col(cols, ["Tipo", "Tipo de Serviço", "Tipo de Servico"])
        col_setor = pick_col(cols, ["Setor", "Área", "Area", "Setor de Manutenção", "Local", "Centro de Trabalho", "1 Setor"])

        if not col_os or not col_desc:
            raise HTTPException(
                status_code=400,
                detail={
                    "erro": "Não encontrei colunas mínimas para importar.",
                    "colunas_encontradas": cols,
                    "minimo_esperado": ["OS (ou Ordem)", "Descrição/Descricao"],
                },
            )

        count_before = len(ordens)

        for _, row in df.iterrows():
            numero_os = str(row.get(col_os, "")).strip()
            descricao = str(row.get(col_desc, "")).strip()
            tipo = str(row.get(col_tipo, "")).strip() if col_tipo else ""
            setor = str(row.get(col_setor, "")).strip() if col_setor else ""

            if not (numero_os or descricao):
                continue

            ordens.append(
                {
                    "id": len(ordens) + 1,
                    "numero_os": numero_os,
                    "descricao": descricao,
                    "tipo_servico": tipo,
                    "setor": setor,
                }
            )

        return {
            "status": "Importação concluída",
            "adicionadas": len(ordens) - count_before,
            "total": len(ordens),
            "colunas_lidas": cols,
            "mapeamento": {"os": col_os, "descricao": col_desc, "tipo": col_tipo, "setor": col_setor},
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"erro": str(e)})


@app.get("/ordens")
def listar_ordens():
    return ordens


@app.get("/setores")
def listar_setores():
    s: Set[str] = set()
    for o in ordens:
        v = (o.get("setor") or "").strip()
        if v:
            s.add(v)
    return sorted(list(s))


@app.get("/tipos")
def listar_tipos():
    s: Set[str] = set(TIPOS_SERVICO_SUGERIDOS)
    for o in ordens:
        v = (o.get("tipo_servico") or "").strip()
        if v:
            s.add(v)
    return sorted(list(s))


@app.get("/status")
def listar_status():
    return STATUS_VALIDOS


# -----------------------------
# Programação (Planner)
# -----------------------------
class ProgramarRequest(BaseModel):
    ordem_id: int
    data: date
    periodo: str  # "Manhã" | "Tarde"
    horario: time
    duracao_min: Optional[int] = None  # NOVO (opcional)
    executantes_texto: Optional[str] = ""
    tipo_servico: Optional[str] = None
    status: Optional[str] = "Planejado"
    observacoes: Optional[str] = ""


class AtualizarProgramacaoRequest(BaseModel):
    data: date
    periodo: str
    horario: time
    duracao_min: Optional[int] = None  # NOVO
    executantes_texto: Optional[str] = ""
    tipo_servico: Optional[str] = None
    status: Optional[str] = "Planejado"
    observacoes: Optional[str] = ""


def find_programacao(prog_id: int):
    return next((p for p in programacoes if p.get("id") == prog_id), None)


@app.post("/programar")
def programar(req: ProgramarRequest):
    if req.periodo not in {"Manhã", "Tarde"}:
        raise HTTPException(status_code=400, detail={"erro": "periodo inválido. Use 'Manhã' ou 'Tarde'."})

    if req.status and req.status not in STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail={"erro": f"status inválido. Use um de {STATUS_VALIDOS}"})

    os_item = next((o for o in ordens if o.get("id") == req.ordem_id), None)
    if not os_item:
        raise HTTPException(status_code=404, detail={"erro": "Ordem de serviço não encontrada", "ordem_id": req.ordem_id})

    # Conflito (Regra B)
    conflitos = conflitos_execucao_regra_b(
        req.data.isoformat(),
        req.executantes_texto or "",
        req.horario,
        req.duracao_min,
        ignorar_prog_id=None
    )
    if conflitos:
        raise HTTPException(
            status_code=409,
            detail={
                "erro": "Conflito de executantes por horário (Regra B).",
                "regra": "No mesmo dia, um executante não pode estar em duas atividades com horário igual (sem duração) ou com sobreposição (com duração).",
                "conflitos": conflitos,
            },
        )

    tipo = (req.tipo_servico or os_item.get("tipo_servico") or "").strip()
    setor = (os_item.get("setor") or "").strip()
    executantes = parse_executantes_free_text(req.executantes_texto)

    prog = {
        "id": len(programacoes) + 1,
        "ordem_id": os_item["id"],
        "numero_os": os_item.get("numero_os"),
        "descricao": os_item.get("descricao"),
        "setor": setor,
        "data": req.data.isoformat(),
        "periodo": req.periodo,
        "horario_inicio": req.horario.strftime("%H:%M"),
        "duracao_min": req.duracao_min,
        "executantes": executantes,
        "executantes_texto": req.executantes_texto or "",
        "tipo_servico": tipo,
        "status": req.status or "Planejado",
        "observacoes": req.observacoes or "",
        "criado_em": datetime.utcnow().isoformat() + "Z",
        "atualizado_em": None,
    }
    programacoes.append(prog)
    return {"status": "OK", "programacao": prog}


@app.put("/programacoes/{prog_id}")
def atualizar_programacao(prog_id: int, req: AtualizarProgramacaoRequest):
    p = find_programacao(prog_id)
    if not p:
        raise HTTPException(status_code=404, detail={"erro": "Programação não encontrada", "id": prog_id})

    if req.periodo not in {"Manhã", "Tarde"}:
        raise HTTPException(status_code=400, detail={"erro": "periodo inválido. Use 'Manhã' ou 'Tarde'."})

    if req.status and req.status not in STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail={"erro": f"status inválido. Use um de {STATUS_VALIDOS}"})

    # Conflito (Regra B) — ignorando a própria programação
    conflitos = conflitos_execucao_regra_b(
        req.data.isoformat(),
        req.executantes_texto or "",
        req.horario,
        req.duracao_min,
        ignorar_prog_id=prog_id
    )
    if conflitos:
        raise HTTPException(
            status_code=409,
            detail={
                "erro": "Conflito de executantes por horário (Regra B).",
                "regra": "No mesmo dia, um executante não pode estar em duas atividades com horário igual (sem duração) ou com sobreposição (com duração).",
                "conflitos": conflitos,
            },
        )

    p["data"] = req.data.isoformat()
    p["periodo"] = req.periodo
    p["horario_inicio"] = req.horario.strftime("%H:%M")
    p["duracao_min"] = req.duracao_min
    p["executantes_texto"] = req.executantes_texto or ""
    p["executantes"] = parse_executantes_free_text(req.executantes_texto)
    if req.tipo_servico is not None:
        p["tipo_servico"] = (req.tipo_servico or "").strip()
    p["status"] = req.status or "Planejado"
    p["observacoes"] = req.observacoes or ""
    p["atualizado_em"] = datetime.utcnow().isoformat() + "Z"
    return {"status": "OK", "programacao": p}


@app.delete("/programacoes/{prog_id}")
def deletar_programacao(prog_id: int):
    p = find_programacao(prog_id)
    if not p:
        raise HTTPException(status_code=404, detail={"erro": "Programação não encontrada", "id": prog_id})
    programacoes.remove(p)
    return {"status": "OK", "removida_id": prog_id}


@app.get("/programacoes")
def listar_programacoes(
    data_ini: Optional[date] = None,
    data_fim: Optional[date] = None,
):
    if data_ini or data_fim:
        def in_range(p):
            d = date.fromisoformat(p["data"])
            if data_ini and d < data_ini:
                return False
            if data_fim and d > data_fim:
                return False
            return True
        return [p for p in programacoes if in_range(p)]
    return programacoes
