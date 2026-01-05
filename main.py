from fastapi import FastAPI, UploadFile, File
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

@app.post("/importar-excel")
async def importar_excel(file: UploadFile = File(...)):
    df = pd.read_excel(file.file)
    for _, row in df.iterrows():
        ordens.append({
            "numero_os": str(row.get("OS")),
            "descricao": row.get("Descricao"),
            "tipo_servico": row.get("Tipo"),
        })
    return {"status": "Importação concluída", "total": len(ordens)}

@app.get("/ordens")
def listar_ordens():
    return ordens
