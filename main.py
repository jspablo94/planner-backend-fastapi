from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from datetime import date, time, datetime
from typing import List, Optional, Set, Tuple, Dict, Any

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
ordens: List[Dict[str, Any]] = []

# planners: {planner_id: {id, name, created_at, programacoes: []}}
planners: Dict[int, Dict[str, Any]] = {}
next_planner_id = 1

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
    try:
        h, m = hhmm.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 0


def interval_from_prog(horario_inicio_hhmm: str, duracao_min: Optional[int]) -> Tuple[int, int, bool]:
    ini = hhmm_to_minutes(horario_inicio_hhmm)
    if duracao_min is None or duracao_min <= 0:
        return ini, ini, False
    return ini, ini + duracao_min, True


def interval_from_request(horario: time, duracao_min: Optional[int]) -> Tuple[int, int, bool]:
    ini = time_to_minutes(horario)
    if duracao_min is None or duracao_min <= 0:
        return ini, ini, False
    return ini, ini + duracao_min, True


def overlaps(a_ini: int, a_fim: int, a_has: bool, b_ini: int, b_fim: int, b_has: bool) -> bool:
    # Regra B:
    # - ambos com duração -> sobreposição
    # - sem duração em um dos lados -> conflito só se horário inicial igual
    if a_has and b_has:
        return not (a_fim <= b_ini or b_fim <= a_ini)
    return a_ini == b_ini


def get_planner(planner_id: int) -> Dict[str, Any]:
    p = planners.get(planner_id)
    if not p:
        raise HTTPException(status_code=404, detail={"erro": "Planner não encontrado", "planner_id": planner_id})
    return p


def get_programacoes(planner_id: int) -> List[Dict[str, Any]]:
    planner = get_planner(planner_id)
    return planner["programacoes"]


def find_programacao(planner_id: int, prog_id: int) -> Optional[Dict[str, Any]]:
    progs = get_programacoes(planner_id)
    return next((p for p in progs if p.get("id") == prog_id), None)


def conflitos_execucao_regra_b(
    progs: List[Dict[str, Any]],
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
    for p in progs:
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
# Planners (Programações nomeadas)
# -----------------------------
class CreatePlannerRequest(BaseModel):
    name: str


@app.post("/planners")
def create_planner(req: CreatePlannerRequest):
    global next_planner_id
    name = (req.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail={"erro": "Nome do planner é obrigatório."})

    # evita nomes duplicados (opcional)
    for p in planners.values():
        if (p.get("name") or "").strip().lower() == name.lower():
            raise HTTPException(status_code=409, detail={"erro": "Já existe um planner com esse nome."})

    pid = next_planner_id
    next_planner_id += 1
    planners[pid] = {
        "id": pid,
        "name": name,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "programacoes": [],
        "next_prog_id": 1,
    }
    return {"status": "OK", "planner": {"id": pid, "name": name}}


@app.get("/planners")
def list_planners():
    return [{"id": p["id"], "name": p["name"], "created_at": p["created_at"]} for p in planners.values()]


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
def listar_ordens(planner_id: Optional[int] = Query(default=None)):
    """
    Se planner_id for informado:
      retorna apenas OS que NÃO estão usadas nesse planner (uma OS por ordem_id).
    """
    if planner_id is None:
        return ordens

    progs = get_programacoes(planner_id)
    usados = set(p.get("ordem_id") for p in progs)
    return [o for o in ordens if o.get("id") not in usados]


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
    planner_id: int
    ordem_id: int
    data: date
    periodo: str  # "Manhã" | "Tarde"
    horario: time
    duracao_min: Optional[int] = None
    executantes_texto: Optional[str] = ""
    tipo_servico: Optional[str] = None
    status: Optional[str] = "Planejado"
    observacoes: Optional[str] = ""


class AtualizarProgramacaoRequest(BaseModel):
    planner_id: int
    data: date
    periodo: str
    horario: time
    duracao_min: Optional[int] = None
    executantes_texto: Optional[str] = ""
    tipo_servico: Optional[str] = None
    status: Optional[str] = "Planejado"
    observacoes: Optional[str] = ""


@app.post("/programar")
def programar(req: ProgramarRequest):
    if req.periodo not in {"Manhã", "Tarde"}:
        raise HTTPException(status_code=400, detail={"erro": "periodo inválido. Use 'Manhã' ou 'Tarde'."})

    if req.status and req.status not in STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail={"erro": f"status inválido. Use um de {STATUS_VALIDOS}"})

    planner = get_planner(req.planner_id)
    progs = planner["programacoes"]

    os_item = next((o for o in ordens if o.get("id") == req.ordem_id), None)
    if not os_item:
        raise HTTPException(status_code=404, detail={"erro": "Ordem de serviço não encontrada", "ordem_id": req.ordem_id})

    # regra: OS não pode repetir no mesmo planner
    if any(p.get("ordem_id") == req.ordem_id for p in progs):
        raise HTTPException(status_code=409, detail={"erro": "Essa OS já está usada neste planner."})

    # Conflitos (Regra B)
    conflitos = conflitos_execucao_regra_b(
        progs,
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
                "conflitos": conflitos,
            },
        )

    tipo = (req.tipo_servico or os_item.get("tipo_servico") or "").strip()
    setor = (os_item.get("setor") or "").strip()
    executantes = parse_executantes_free_text(req.executantes_texto)

    prog_id = planner["next_prog_id"]
    planner["next_prog_id"] += 1

    prog = {
        "id": prog_id,
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
    progs.append(prog)
    return {"status": "OK", "programacao": prog}


@app.get("/programacoes")
def listar_programacoes(
    planner_id: int = Query(...),
    data_ini: Optional[date] = None,
    data_fim: Optional[date] = None,
):
    progs = get_programacoes(planner_id)

    if data_ini or data_fim:
        def in_range(p):
            d = date.fromisoformat(p["data"])
            if data_ini and d < data_ini:
                return False
            if data_fim and d > data_fim:
                return False
            return True
        return [p for p in progs if in_range(p)]

    return progs


@app.put("/programacoes/{prog_id}")
def atualizar_programacao(prog_id: int, req: AtualizarProgramacaoRequest):
    planner = get_planner(req.planner_id)
    progs = planner["programacoes"]
    p = find_programacao(req.planner_id, prog_id)
    if not p:
        raise HTTPException(status_code=404, detail={"erro": "Programação não encontrada", "id": prog_id})

    if req.periodo not in {"Manhã", "Tarde"}:
        raise HTTPException(status_code=400, detail={"erro": "periodo inválido. Use 'Manhã' ou 'Tarde'."})

    if req.status and req.status not in STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail={"erro": f"status inválido. Use um de {STATUS_VALIDOS}"})

    conflitos = conflitos_execucao_regra_b(
        progs,
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
def deletar_programacao(prog_id: int, planner_id: int = Query(...)):
    planner = get_planner(planner_id)
    progs = planner["programacoes"]
    p = find_programacao(planner_id, prog_id)
    if not p:
        raise HTTPException(status_code=404, detail={"erro": "Programação não encontrada", "id": prog_id})
    progs.remove(p)
    return {"status": "OK", "removida_id": prog_id}
