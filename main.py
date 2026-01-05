from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ordens = []

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Remove espaços e padroniza nomes (ajuda MUITO com CMMS)
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

@app.post("/importar-excel")
async def importar_excel(file: UploadFile = File(...)):
    try:
        filename = (file.filename or "").lower()

        # Lê o arquivo inteiro em memória
        raw = await file.read()

        if filename.endswith(".csv"):
            df = pd.read_csv(pd.io.common.BytesIO(raw))
        elif filename.endswith(".xls"):
            # .xls precisa do xlrd
            df = pd.read_excel(pd.io.common.BytesIO(raw), engine="xlrd")
        else:
            # default: .xlsx
            df = pd.read_excel(pd.io.common.BytesIO(raw), engine="openpyxl")

        df = normalize_columns(df)

        # Mostra quais colunas vieram (ajuda a diagnosticar)
        cols = df.columns.tolist()

        # Tenta achar colunas comuns (fallback)
        # Se no seu arquivo os nomes forem diferentes, a gente ajusta aqui depois.
        col_os = "OS" if "OS" in cols else ("Ordem" if "Ordem" in cols else None)
        col_desc = "Descricao" if "Descricao" in cols else ("Descrição" if "Descrição" in cols else None)
        col_tipo = "Tipo" if "Tipo" in cols else ("Tipo de Serviço" if "Tipo de Serviço" in cols else None)

        if not col_os or not col_desc or not col_tipo:
            raise HTTPException(
                status_code=400,
                detail={
                    "erro": "Não encontrei as colunas esperadas.",
                    "colunas_encontradas": cols,
                    "esperado": ["OS", "Descricao/Descrição", "Tipo/Tipo de Serviço"]
                }
            )

        count_before = len(ordens)

        for _, row in df.iterrows():
            ordens.append({
                "id": len(ordens) + 1,
                "numero_os": str(row.get(col_os, "")).strip(),
                "descricao": str(row.get(col_desc, "")).strip(),
                "tipo_servico": str(row.get(col_tipo, "")).strip(),
            })

        return {"status": "Importação concluída", "adicionadas": len(ordens) - count_before, "total": len(ordens)}

    except HTTPException:
        raise
    except Exception as e:
        # Retorna o erro real (em vez de “Internal Server Error” genérico)
        raise HTTPException(status_code=500, detail={"erro": str(e)})
