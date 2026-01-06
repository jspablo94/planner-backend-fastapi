from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from datetime import date, time, datetime
from typing import List, Optional, Set

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
    # separa por vírgula ou ponto e vírgula
    parts = [p.strip() for p in str(s).replace(";", ",").split(",")]
    return [p for p in parts if p]


# -----------------------------
# Importação (Excel/CSV)
# -----------------------------
@app.post("/importar-excel")
async def importar_excel(file: UploadFile = File(...)):
    """
    Recebe xlsx/xls/csv e adiciona as OS em memória.
    Colunas esperadas (com variações):
      - OS (ou Ordem)
      - Descricao/Descrição/Descrição Curta/Descrição do Serviço
      - Tipo (ou Tipo de Serviço)
      - Setor (ou Área, Setor de Manutenção, Local, Centro de Trabalho)
    """
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
                    "dica": "Me diga os nomes exatos das colunas caso estejam diferentes.",
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
            "mapeamento": {
                "os": col_os,
                "descricao": col_desc,
                "tipo": col_tipo,
                "setor": col_setor,
            },
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
    # mistura os sugeridos com os que vierem das OS
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
    executantes_texto: Optional[str] = ""  # texto livre (ex: "João, Maria")
    tipo_servico: Optional[str] = None
    status: Optional[str] = "Planejado"
    observacoes: Optional[str] = ""


@app.post("/programar")
def programar(req: ProgramarRequest):
    if req.periodo not in {"Manhã", "Tarde"}:
        raise HTTPException(status_code=400, detail={"erro": "periodo inválido. Use 'Manhã' ou 'Tarde'."})

    if req.status and req.status not in STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail={"erro": f"status inválido. Use um de {STATUS_VALIDOS}"})

    os_item = next((o for o in ordens if o.get("id") == req.ordem_id), None)
    if not os_item:
        raise HTTPException(status_code=404, detail={"erro": "Ordem de serviço não encontrada", "ordem_id": req.ordem_id})

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
        "executantes": executantes,
        "executantes_texto": req.executantes_texto or "",
        "tipo_servico": tipo,
        "status": req.status or "Planejado",
        "observacoes": req.observacoes or "",
        "criado_em": datetime.utcnow().isoformat() + "Z",
    }
    programacoes.append(prog)
    return {"status": "OK", "programacao": prog}


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
