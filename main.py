import os
from datetime import date, time, datetime
from typing import List, Optional, Set, Tuple, Dict, Any

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import (
    create_engine, Column, Integer, String, Date, DateTime, Text,
    ForeignKey, select, func, and_, or_, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# -----------------------------
# Config / DB
# -----------------------------
def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return "sqlite:///./planner.db"

    # Render às vezes fornece postgres:// ... SQLAlchemy 2 prefere postgresql+psycopg2://
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return db_url

DATABASE_URL = get_database_url()

connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

Base = declarative_base()

# -----------------------------
# Models
# -----------------------------
class Ordem(Base):
    __tablename__ = "ordens"

    id = Column(Integer, primary_key=True)
    numero_os = Column(String(120), nullable=True, index=True)
    descricao = Column(Text, nullable=True)
    tipo_servico = Column(String(200), nullable=True, index=True)
    setor = Column(String(200), nullable=True, index=True)

    intervencao = Column(String(200), nullable=True, index=True)
    categoria_os = Column(String(50), nullable=False, default="Preventiva")

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Planner(Base):
    __tablename__ = "planners"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    programacoes = relationship("Programacao", back_populates="planner", cascade="all, delete-orphan")


class Programacao(Base):
    __tablename__ = "programacoes"

    id = Column(Integer, primary_key=True)

    planner_id = Column(Integer, ForeignKey("planners.id", ondelete="CASCADE"), nullable=False, index=True)
    ordem_id = Column(Integer, ForeignKey("ordens.id", ondelete="RESTRICT"), nullable=False, index=True)

    numero_os = Column(String(120), nullable=True, index=True)
    descricao = Column(Text, nullable=True)
    setor = Column(String(200), nullable=True, index=True)

    intervencao = Column(String(200), nullable=True)
    categoria_os = Column(String(50), nullable=False, default="Preventiva")

    area = Column(String(200), nullable=True, index=True)

    data = Column(Date, nullable=False, index=True)
    data_conclusao = Column(Date, nullable=False, index=True)

    periodo = Column(String(20), nullable=False)  # Manhã/Tarde
    horario_inicio = Column(String(5), nullable=False)  # "HH:MM"
    duracao_min = Column(Integer, nullable=True)

    executantes_texto = Column(Text, nullable=True)
    tipo_servico = Column(String(200), nullable=True, index=True)
    status = Column(String(50), nullable=True, index=True)
    observacoes = Column(Text, nullable=True)

    criado_em = Column(DateTime, nullable=False, default=datetime.utcnow)
    atualizado_em = Column(DateTime, nullable=True)

    planner = relationship("Planner", back_populates="programacoes")
    ordem = relationship("Ordem")

    __table_args__ = (
        # OS não pode repetir dentro do mesmo planner
        UniqueConstraint("planner_id", "ordem_id", name="uq_planner_ordem"),
    )


# cria tabelas
Base.metadata.create_all(bind=engine)

# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(title="Planner Manutenção - Backend (Postgres)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TIPOS_SERVICO_SUGERIDOS = [
    "Altura",
    "Espaço Confinado",
    "Trabalho a quente",
    "Trabalho elétrico",
    "Terceirizado",
]

STATUS_VALIDOS = ["Planejado", "Em execução", "Concluído", "Reprogramado"]

# -----------------------------
# Helpers
# -----------------------------
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

def time_to_minutes(t: time) -> int:
    return t.hour * 60 + t.minute

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

def categoria_por_intervencao(interv: str) -> str:
    s = (interv or "").strip()
    if s.upper().startswith("UPLN"):
        return "Corretiva"
    return "Preventiva"

def get_planner_or_404(db, planner_id: int) -> Planner:
    p = db.get(Planner, planner_id)
    if not p:
        raise HTTPException(status_code=404, detail={"erro": "Planner não encontrado", "planner_id": planner_id})
    return p

def conflitos_execucao_regra_b(db, planner_id: int, data_iso: str, executantes_texto: str, novo_horario: time, nova_duracao_min: Optional[int], ignorar_prog_id: Optional[int] = None):
    alvo_execs = get_exec_set(executantes_texto or "")
    if not alvo_execs:
        return []

    a_ini, a_fim, a_has = interval_from_request(novo_horario, nova_duracao_min)

    q = select(Programacao).where(
        Programacao.planner_id == planner_id,
        Programacao.data == date.fromisoformat(data_iso),
    )
    if ignorar_prog_id is not None:
        q = q.where(Programacao.id != ignorar_prog_id)

    rows = db.execute(q).scalars().all()

    conflitos = []
    for p in rows:
        b_execs = get_exec_set(p.executantes_texto or "")
        inter_execs = sorted(list(alvo_execs.intersection(b_execs)))
        if not inter_execs:
            continue

        b_ini, b_fim, b_has = interval_from_prog(p.horario_inicio or "00:00", p.duracao_min)
        if overlaps(a_ini, a_fim, a_has, b_ini, b_fim, b_has):
            conflitos.append({
                "programacao_id": p.id,
                "numero_os": p.numero_os,
                "horario_inicio": p.horario_inicio,
                "duracao_min": p.duracao_min,
                "executantes_em_conflito": inter_execs,
            })
    return conflitos

# -----------------------------
# Schemas
# -----------------------------
class CreatePlannerRequest(BaseModel):
    name: str

class ProgramarRequest(BaseModel):
    planner_id: int
    ordem_id: int
    data: date
    data_conclusao: Optional[date] = None
    periodo: str
    horario: time
    duracao_min: Optional[int] = None
    area: Optional[str] = ""
    executantes_texto: Optional[str] = ""
    tipo_servico: Optional[str] = None
    status: Optional[str] = "Planejado"
    observacoes: Optional[str] = ""

class AtualizarProgramacaoRequest(BaseModel):
    planner_id: int
    data: date
    data_conclusao: Optional[date] = None
    periodo: str
    horario: time
    duracao_min: Optional[int] = None
    area: Optional[str] = ""
    executantes_texto: Optional[str] = ""
    tipo_servico: Optional[str] = None
    status: Optional[str] = "Planejado"
    observacoes: Optional[str] = ""

# -----------------------------
# Endpoints
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok", "db": "sqlite" if DATABASE_URL.startswith("sqlite") else "postgres"}

@app.post("/planners")
def create_planner(req: CreatePlannerRequest):
    name = (req.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail={"erro": "Nome do planner é obrigatório."})

    db = SessionLocal()
    try:
        exists = db.execute(select(Planner).where(func.lower(Planner.name) == name.lower())).scalar_one_or_none()
        if exists:
            raise HTTPException(status_code=409, detail={"erro": "Já existe um planner com esse nome."})

        p = Planner(name=name)
        db.add(p)
        db.commit()
        db.refresh(p)
        return {"status": "OK", "planner": {"id": p.id, "name": p.name}}
    finally:
        db.close()

@app.get("/planners")
def list_planners():
    db = SessionLocal()
    try:
        rows = db.execute(select(Planner).order_by(Planner.created_at.desc())).scalars().all()
        return [{"id": p.id, "name": p.name, "created_at": p.created_at.isoformat() + "Z"} for p in rows]
    finally:
        db.close()

@app.post("/importar-excel")
async def importar_excel(file: UploadFile = File(...)):
    db = SessionLocal()
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
        col_interv = pick_col(cols, ["Intervenção", "Intervencao", "Intervenção (Código)", "Intervencao (Codigo)", "Intervencao Codigo"])

        if not col_os or not col_desc:
            raise HTTPException(
                status_code=400,
                detail={
                    "erro": "Não encontrei colunas mínimas para importar.",
                    "colunas_encontradas": cols,
                    "minimo_esperado": ["OS (ou Ordem)", "Descrição/Descricao"],
                },
            )

        before = db.execute(select(func.count(Ordem.id))).scalar_one()

        for _, row in df.iterrows():
            numero_os = str(row.get(col_os, "")).strip()
            descricao = str(row.get(col_desc, "")).strip()
            tipo = str(row.get(col_tipo, "")).strip() if col_tipo else ""
            setor = str(row.get(col_setor, "")).strip() if col_setor else ""
            interv = str(row.get(col_interv, "")).strip() if col_interv else ""
            cat = categoria_por_intervencao(interv)

            if not (numero_os or descricao):
                continue

            o = Ordem(
                numero_os=numero_os,
                descricao=descricao,
                tipo_servico=tipo,
                setor=setor,
                intervencao=interv,
                categoria_os=cat,
            )
            db.add(o)

        db.commit()
        after = db.execute(select(func.count(Ordem.id))).scalar_one()

        return {
            "status": "Importação concluída",
            "adicionadas": int(after - before),
            "total": int(after),
            "colunas_lidas": cols,
            "mapeamento": {"os": col_os, "descricao": col_desc, "tipo": col_tipo, "setor": col_setor, "intervencao": col_interv},
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail={"erro": str(e)})
    finally:
        db.close()

@app.get("/ordens")
def listar_ordens(planner_id: Optional[int] = Query(default=None)):
    db = SessionLocal()
    try:
        if planner_id is None:
            rows = db.execute(select(Ordem).order_by(Ordem.id.desc())).scalars().all()
            return [ordem_to_dict(o) for o in rows]

        # backlog do planner: ordens que NÃO aparecem em programacoes desse planner
        usados = db.execute(select(Programacao.ordem_id).where(Programacao.planner_id == planner_id)).scalars().all()
        usados_set = set(usados)

        q = select(Ordem)
        if usados_set:
            q = q.where(Ordem.id.not_in(usados_set))
        rows = db.execute(q.order_by(Ordem.id.desc())).scalars().all()
        return [ordem_to_dict(o) for o in rows]
    finally:
        db.close()

@app.get("/setores")
def listar_setores():
    db = SessionLocal()
    try:
        rows = db.execute(select(Ordem.setor).where(Ordem.setor.is_not(None))).scalars().all()
        s = sorted({(r or "").strip() for r in rows if (r or "").strip()})
        return s
    finally:
        db.close()

@app.get("/tipos")
def listar_tipos():
    db = SessionLocal()
    try:
        rows = db.execute(select(Ordem.tipo_servico).where(Ordem.tipo_servico.is_not(None))).scalars().all()
        s = set(TIPOS_SERVICO_SUGERIDOS)
        for r in rows:
            v = (r or "").strip()
            if v:
                s.add(v)
        return sorted(list(s))
    finally:
        db.close()

@app.get("/status")
def listar_status():
    return STATUS_VALIDOS

@app.post("/programar")
def programar(req: ProgramarRequest):
    if req.periodo not in {"Manhã", "Tarde"}:
        raise HTTPException(status_code=400, detail={"erro": "periodo inválido. Use 'Manhã' ou 'Tarde'."})
    if req.status and req.status not in STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail={"erro": f"status inválido. Use um de {STATUS_VALIDOS}"})

    db = SessionLocal()
    try:
        get_planner_or_404(db, req.planner_id)

        os_item = db.get(Ordem, req.ordem_id)
        if not os_item:
            raise HTTPException(status_code=404, detail={"erro": "Ordem de serviço não encontrada", "ordem_id": req.ordem_id})

        data_conc = req.data_conclusao or req.data
        if data_conc < req.data:
            raise HTTPException(status_code=400, detail={"erro": "data_conclusao não pode ser menor que data."})

        # OS não pode repetir no planner
        exists = db.execute(
            select(Programacao).where(and_(Programacao.planner_id == req.planner_id, Programacao.ordem_id == req.ordem_id))
        ).scalar_one_or_none()
        if exists:
            raise HTTPException(status_code=409, detail={"erro": "Essa OS já está usada neste planner."})

        # Conflitos (Regra B) no dia de início
        conflitos = conflitos_execucao_regra_b(
            db, req.planner_id, req.data.isoformat(), req.executantes_texto or "", req.horario, req.duracao_min, ignorar_prog_id=None
        )
        if conflitos:
            raise HTTPException(status_code=409, detail={"erro": "Conflito de executantes por horário (Regra B).", "conflitos": conflitos})

        tipo = (req.tipo_servico or os_item.tipo_servico or "").strip()
        setor = (os_item.setor or "").strip()

        p = Programacao(
            planner_id=req.planner_id,
            ordem_id=os_item.id,

            numero_os=os_item.numero_os,
            descricao=os_item.descricao,
            setor=setor,

            intervencao=os_item.intervencao,
            categoria_os=os_item.categoria_os,

            area=(req.area or "").strip(),

            data=req.data,
            data_conclusao=data_conc,

            periodo=req.periodo,
            horario_inicio=req.horario.strftime("%H:%M"),
            duracao_min=req.duracao_min,

            executantes_texto=req.executantes_texto or "",
            tipo_servico=tipo,
            status=req.status or "Planejado",
            observacoes=req.observacoes or "",
        )
        db.add(p)
        db.commit()
        db.refresh(p)
        return {"status": "OK", "programacao": programacao_to_dict(p)}
    finally:
        db.close()

@app.get("/programacoes")
def listar_programacoes(
    planner_id: int = Query(...),
    data_ini: Optional[date] = None,
    data_fim: Optional[date] = None,
):
    db = SessionLocal()
    try:
        get_planner_or_404(db, planner_id)

        q = select(Programacao).where(Programacao.planner_id == planner_id)

        if data_ini or data_fim:
            # interseção de intervalos [data, data_conclusao] com [data_ini, data_fim]
            if data_ini:
                q = q.where(Programacao.data_conclusao >= data_ini)
            if data_fim:
                q = q.where(Programacao.data <= data_fim)

        rows = db.execute(q.order_by(Programacao.data.asc(), Programacao.horario_inicio.asc())).scalars().all()
        return [programacao_to_dict(p) for p in rows]
    finally:
        db.close()

@app.put("/programacoes/{prog_id}")
def atualizar_programacao(prog_id: int, req: AtualizarProgramacaoRequest):
    if req.periodo not in {"Manhã", "Tarde"}:
        raise HTTPException(status_code=400, detail={"erro": "periodo inválido. Use 'Manhã' ou 'Tarde'."})
    if req.status and req.status not in STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail={"erro": f"status inválido. Use um de {STATUS_VALIDOS}"})

    db = SessionLocal()
    try:
        get_planner_or_404(db, req.planner_id)

        p = db.get(Programacao, prog_id)
        if not p or p.planner_id != req.planner_id:
            raise HTTPException(status_code=404, detail={"erro": "Programação não encontrada", "id": prog_id})

        data_conc = req.data_conclusao or req.data
        if data_conc < req.data:
            raise HTTPException(status_code=400, detail={"erro": "data_conclusao não pode ser menor que data."})

        conflitos = conflitos_execucao_regra_b(
            db, req.planner_id, req.data.isoformat(), req.executantes_texto or "", req.horario, req.duracao_min, ignorar_prog_id=prog_id
        )
        if conflitos:
            raise HTTPException(status_code=409, detail={"erro": "Conflito de executantes por horário (Regra B).", "conflitos": conflitos})

        p.data = req.data
        p.data_conclusao = data_conc
        p.periodo = req.periodo
        p.horario_inicio = req.horario.strftime("%H:%M")
        p.duracao_min = req.duracao_min
        p.area = (req.area or "").strip()
        p.executantes_texto = req.executantes_texto or ""
        if req.tipo_servico is not None:
            p.tipo_servico = (req.tipo_servico or "").strip()
        p.status = req.status or "Planejado"
        p.observacoes = req.observacoes or ""
        p.atualizado_em = datetime.utcnow()

        db.commit()
        db.refresh(p)
        return {"status": "OK", "programacao": programacao_to_dict(p)}
    finally:
        db.close()

@app.delete("/programacoes/{prog_id}")
def deletar_programacao(prog_id: int, planner_id: int = Query(...)):
    db = SessionLocal()
    try:
        get_planner_or_404(db, planner_id)

        p = db.get(Programacao, prog_id)
        if not p or p.planner_id != planner_id:
            raise HTTPException(status_code=404, detail={"erro": "Programação não encontrada", "id": prog_id})

        db.delete(p)
        db.commit()
        return {"status": "OK", "removida_id": prog_id}
    finally:
        db.close()

# -----------------------------
# Serializers
# -----------------------------
def ordem_to_dict(o: Ordem) -> Dict[str, Any]:
    return {
        "id": o.id,
        "numero_os": o.numero_os,
        "descricao": o.descricao,
        "tipo_servico": o.tipo_servico,
        "setor": o.setor,
        "intervencao": o.intervencao,
        "categoria_os": o.categoria_os,
    }

def programacao_to_dict(p: Programacao) -> Dict[str, Any]:
    # mantém o formato que seu front já usa
    return {
        "id": p.id,
        "ordem_id": p.ordem_id,
        "numero_os": p.numero_os,
        "descricao": p.descricao,
        "setor": p.setor,
        "intervencao": p.intervencao,
        "categoria_os": p.categoria_os,
        "area": p.area,
        "data": p.data.isoformat(),
        "data_conclusao": p.data_conclusao.isoformat(),
        "periodo": p.periodo,
        "horario_inicio": p.horario_inicio,
        "duracao_min": p.duracao_min,
        "executantes": parse_executantes_free_text(p.executantes_texto or ""),
        "executantes_texto": p.executantes_texto or "",
        "tipo_servico": p.tipo_servico,
        "status": p.status,
        "observacoes": p.observacoes,
        "criado_em": p.criado_em.isoformat() + "Z" if p.criado_em else None,
        "atualizado_em": p.atualizado_em.isoformat() + "Z" if p.atualizado_em else None,
    }
