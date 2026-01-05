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

        # Se não encontrou colunas mínimas, devolve 400 com diagnóstico
        if not col_os or not col_desc or not col_tipo:
            raise HTTPException(
                status_code=400,
                detail={
                    "erro": "Não encontrei as colunas esperadas.",
                    "colunas_encontradas": cols,
                    "esperado": ["OS (ou Ordem)", "Descricao/Descrição", "Tipo/Tipo de Serviço"],
                    "dica": "Me diga os nomes exatos das colunas do seu Excel para eu ajustar o mapeamento."
                }
            )

        count_before = len(ordens)

        for _, row in df.iterrows():
            numero_os = str(row.get(col_os, "")).strip()
            descricao = str(row.get(col_desc, "")).strip()
            tipo = str(row.get(col_tipo, "")).strip()

            # Ignora linhas totalmente vazias
            if not (numero_os or descricao or tipo):
                continue

            ordens.append({
                "id": len(ordens) + 1,
                "numero_os": numero_os,
                "descricao": descricao,
                "tipo_servico": tipo,
            })

        return {
            "status": "Importação concluída",
            "adicionadas": len(ordens) - count_before,
            "total": len(ordens),
            "colunas_lidas": cols
        }

    except HTTPException:
        raise
    except Exception as e:
        # Retorna o erro real para facilitar correção
        raise HTTPException(status_code=500, detail={"erro": str(e)})


@app.get("/ordens")
def listar_ordens():
    """Retorna as ordens importadas (em memória)."""
    return ordens
