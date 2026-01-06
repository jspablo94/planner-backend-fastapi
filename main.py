from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from datetime import date, time, datetime
from typing import List, Optional

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

TIPOS_SERVICO_VALIDOS = {
    "Altura",
    "Espaço Confinado",
    "Trabalho a quente",
    "Trabalho elétrico",
    "Terceirizado",
}

STATUS_VALIDOS = {"Planejado", "Em execução", "Concluído", "Reprogramado"}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


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

        col_os = "OS" if "OS" in cols else ("Ordem" if "Ordem" in cols else None)
        col_desc = "Descricao" if "Descricao" in cols else ("Descrição" if "Descrição" in cols else None)
        col_tipo = "Tipo" if "Tipo" in cols else ("Tipo de Serviço" if "Tipo de Serviço" in cols else None)

        if not col_os or not col_desc or not col_tipo:
            raise HTTPException(
                status_code=400,
                detail={
                    "erro": "Não encontrei as colunas esperadas.",
                    "colunas_encontradas": cols,
                    "esperado": ["OS (ou Ordem)", "Descricao/Descrição", "Tipo/Tipo de Serviço"],
                },
            )

        count_before = len(ordens)

        for _, row in df.iterrows():
            numero_os = str(row.get(col_os, "")).strip()
            descricao = str(row.get(col_desc, "")).strip()
            tipo = str(row.get(col_tipo, "")).strip()

            if not (numero_os or descricao or tipo):
                continue

            ordens.append(
                {
                    "id": len(ordens) + 1,
                    "numero_os": numero_os,
                    "descricao": descricao,
                    "tipo_servico": tipo,
                }
            )

        return {
            "status": "Importação concluída",
            "adicionadas": len(ordens) - count_before,
            "total": len(ordens),
            "colunas_lidas": cols,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"erro": str(e)})


@app.get("/ordens")
def listar_ordens():
    return ordens


# -----------------------------
# Programação (Planner)
# -----------------------------
class ProgramarRequest(BaseModel):
    ordem_id: int
    data: date
    periodo: str  # "Manhã" | "Tarde"
    horario: time
    executantes: Optional[List[str]] = []
    tipo_servico: Optional[str] = None
    status: Optional[str] = "Planejado"
    observacoes: Optional[str] = ""


@app.post("/programar")
def programar(req: ProgramarRequest):
    # valida período
    if req.periodo not in {"Manhã", "Tarde"}:
        raise HTTPException(status_code=400, detail={"erro": "periodo inválido. Use 'Manhã' ou 'Tarde'."})

    # valida status
    if req.status and req.status not in STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail={"erro": f"status inválido. Use um de {sorted(STATUS_VALIDOS)}"})

    # acha OS
    os_item = next((o for o in ordens if o.get("id") == req.ordem_id), None)
    if not os_item:
        raise HTTPException(status_code=404, detail={"erro": "Ordem de serviço não encontrada", "ordem_id": req.ordem_id})

    # define tipo_servico (se não vier, usa o da OS)
    tipo = req.tipo_servico or os_item.get("tipo_servico") or ""

    # (opcional) validar contra lista de tipos conhecidos
    # Se seu CMMS tiver variações, pode comentar esta validação.
    if tipo in TIPOS_SERVICO_VALIDOS or tipo == "":
        pass
    # else: não bloqueia, só aceita (mantém flexível)

    prog = {
        "id": len(programacoes) + 1,
        "ordem_id": os_item["id"],
        "numero_os": os_item.get("numero_os"),
        "descricao": os_item.get("descricao"),
        "data": req.data.isoformat(),
        "periodo": req.periodo,
        "horario_inicio": req.horario.strftime("%H:%M"),
        "executantes": req.executantes or [],
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
    # se vier filtro de data, aplica
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
