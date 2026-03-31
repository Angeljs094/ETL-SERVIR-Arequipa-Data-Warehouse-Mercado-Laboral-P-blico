from sqlalchemy import (
    create_engine, Column, Integer, String, Text,
    Numeric, Date, ForeignKey, text, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, sessionmaker
import calendar
import os

Base = declarative_base()

class DimCargo(Base):
    __tablename__  = "dim_cargo"
    __table_args__ = {"schema": "dw"}

    id_cargo                   = Column(Integer, primary_key=True, autoincrement=True)
    oferta_laboral             = Column(Text,         nullable=True)
    nivel_educativo            = Column(String(100),  nullable=True)
    experiencia                = Column(Text,         nullable=True)
    formacion_academica_perfil = Column(Text,         nullable=True)
    especializacion            = Column(Text,         nullable=True)
    conocimiento               = Column(Text,         nullable=True)
    competencias               = Column(Text,         nullable=True)


class DimInstitucion(Base):
    __tablename__  = "dim_institucion"
    __table_args__ = (
        UniqueConstraint('institucion', 'region', 'distrito', name='uq_institucion'),
        {"schema": "dw"}
    )

    id_institucion = Column(Integer, primary_key=True, autoincrement=True)
    institucion    = Column(String(255), nullable=True)
    region         = Column(String(100), nullable=True)
    distrito       = Column(String(100), nullable=True)


class DimContrato(Base):
    __tablename__  = "dim_contrato"
    __table_args__ = {"schema": "dw"}

    id_contrato     = Column(Integer, primary_key=True, autoincrement=True)
    regimen_laboral = Column(String(100), nullable=True)
    modalidad       = Column(String(100), nullable=True)
    banda_salarial  = Column(String(100), nullable=True)

# ══════════════════════════════════════════════════════════════
# TABLA DE HECHOS
# ══════════════════════════════════════════════════════════════

class FactConvocatoria(Base):
    __tablename__  = "fact_convocatoria"
    __table_args__ = {"schema": "dw"}
   
    id_convocatoria   = Column(String,  primary_key=True,     nullable=True)
    id_cargo          = Column(Integer, ForeignKey("dw.dim_cargo.id_cargo"),            nullable=True)
    id_institucion    = Column(Integer, ForeignKey("dw.dim_institucion.id_institucion"), nullable=True)
    id_contrato       = Column(Integer, ForeignKey("dw.dim_contrato.id_contrato"),      nullable=True)
    id_tiempo_inicio  = Column(Date,           nullable=True)
    id_tiempo_fin     = Column(Date,          nullable=True)
    cantidad_vacantes = Column(Integer,      nullable=True)
    salario           = Column(Numeric(10,2),nullable=True)
    duracion_dias     = Column(Integer,      nullable=True)
    link_postulacion  = Column(Text,         nullable=True)
    fecha_carga       = Column(Date,         nullable=True)


# ── Conexión ─────────────────────────────────────────────────
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")
DB_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")   # nombre del servicio en docker-compose
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")

engine  = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
Session = sessionmaker(bind=engine)


# ══════════════════════════════════════════════════════════════
# CREACIÓN DE ESQUEMA Y TABLAS
# ══════════════════════════════════════════════════════════════

def crear_schema_dw():
    """Crea el esquema dw si no existe."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw"))        
    print("✅ Esquema dw listo")


def crear_tablas_dw():
    """Crea todas las tablas del DW si no existen."""
    Base.metadata.create_all(engine)
    print("✅ Tablas dw creadas: dim_cargo, dim_institucion, dim_contrato, dim_tiempo, fact_convocatoria")


def inicializar_dw():
    """Punto de entrada único: esquema + tablas."""
    crear_schema_dw()
    crear_tablas_dw()


# ══════════════════════════════════════════════════════════════
# POBLAR dim_tiempo (rango de fechas independiente del ODS)
# ══════════════════════════════════════════════════════════════

