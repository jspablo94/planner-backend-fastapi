from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI(title="Planner Manutenção - Backend")

# CORS para permitir StackBlitz/qualquer origem
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Armazenamento em memória (MVP)
ordens = []


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espaços e padroniza nomes de colunas."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


@app.post("/importar-excel")
async def importar_excel(file: UploadFile = File(...)):
    """
    Recebe um arquivo (xlsx/xls/csv), lê e adiciona as ordens em memória.
    Espera colunas equivalentes a: OS, Descricao/Descrição, Tipo/Tipo de Serviço
    """
    try:
        filename = (file.filename or "").lower()

        # Lê o arquivo inteiro em memória
        raw = await file.read()

        # Decide como ler baseado na extensão
        if filename.endswith(".csv"):
            df = pd.read_csv(pd.io.common.BytesIO(raw))
        elif filename.endswith(".xls"):
            # .xls precisa do xlrd
            df = pd.read_excel(pd.io.common.BytesIO(raw), engine="xlrd")
        else:
            # default: .xlsx
            df = pd.read_excel(pd.io.common.BytesIO(raw), engine="openpyxl")

        df = normalize_columns(df)
        cols = df.columns.tolist()

        # Fallbacks de nomes comuns (você pode ampliar depois)
        col_os = "OS" if "OS" in cols else ("Ordem" if "Ordem" in cols else None)

        # Alguns CMMS exportam com acento
        col_desc = (
            "Descricao" if "Descricao" in cols else
            ("Descrição" if "Descrição" in cols else None)
        )

        col_tipo = (
            "Tipo" if "Tipo" in cols else
            ("Tipo de Serviço" if "Tipo de Serviço" in cols else None)
        )

        # Se não encontrou colunas
