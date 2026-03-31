# db_config_ods.py
from sqlalchemy import create_engine, Column, Integer, String, Text, Numeric, Date, TIMESTAMP,text
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import os
import logging
log = logging.getLogger(__name__)


Base = declarative_base()

class ConvocatoriaODS(Base):
    __tablename__ = "ofertas_servir"
    __table_args__ = {"schema": "ods"}   # capa ODS

    id = Column(Integer, primary_key=True,)
    id_convocatoria = Column(String, nullable=False)
    oferta_laboral = Column(Text, nullable=True)
    institucion = Column(String(255), nullable=True)
    region = Column(String(100), nullable=True)
    distrito = Column(String(100), nullable=True)
    regimen_laboral = Column(String(100), nullable=True)
    modalidad = Column(String(100), nullable=True)
    cantidad_vacantes = Column(Integer, nullable=True)
    salario = Column(Numeric(10,2), nullable=True)
    nivel_educativo = Column(String(100), nullable=True)
    experiencia = Column(Text, nullable=True)
    formacion_academica_perfil = Column(Text, nullable=True)
    especializacion = Column(Text, nullable=True)
    conocimiento = Column(Text, nullable=True)
    competencias = Column(Text, nullable=True)
    link_postulacion = Column(Text, nullable=True)
    fecha_inicio = Column(Date, nullable=True)
    fecha_fin = Column(Date, nullable=True)
    duracion_oferta_dias = Column(Integer, nullable=True)
    banda_salarial = Column(String(100), nullable=True)
    fecha_carga = Column(Date, nullable=True)

# Conexión a PostgreSQL (reutilizable)
# Conexión a PostgreSQL usando variables de entorno
# Variables de entorno con valores por defecto
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")
DB_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")   # nombre del servicio en docker-compose
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
Session = sessionmaker(bind=engine)
def get_session():
    return Session()


# Crear tabla si no existe
Base.metadata.create_all(engine)

def guardar_transformacion(df):
    session = get_session()
    session.execute(text("TRUNCATE TABLE ods.ofertas_servir RESTART IDENTITY"))
    session.commit()
    registros = []
    for _, row in df.iterrows():
        convocatoria = ConvocatoriaODS(
            id=row["id"],
            id_convocatoria=(row["id_convocatoria"]), #if pd.notnull(row["id_convocatoria"]) else None,
            oferta_laboral=row["oferta_laboral"],
            institucion=row["institucion"],
            region=row["region"],
            distrito=row["distrito"],
            regimen_laboral=row["regimen_laboral"],
            modalidad=row["modalidad"],
            cantidad_vacantes=int(row["cantidad_vacantes"]), #if pd.notnull(row["cantidad_vacantes"]) else None,
            salario=float(row["salario"]), #if pd.notnull(row["salario"]) else None,
            nivel_educativo=row["nivel_educativo"],
            experiencia=row["experiencia"],
            formacion_academica_perfil=row["formacion_academica_perfil"],
            especializacion=row["especializacion"],
            conocimiento=row["conocimiento"],
            competencias=row["competencias"],
            link_postulacion=row["link_postulacion"],
            fecha_inicio=row["fecha_inicio"], #if pd.notnull(row["fecha_inicio"]) else None,
            fecha_fin=row["fecha_fin"], #if pd.notnull(row["fecha_fin"]) else None,
            duracion_oferta_dias=int(row["duracion_oferta_dias"]), #if pd.notnull(row["duracion_oferta_dias"]) else None,
            banda_salarial=row["banda_salarial"],
            fecha_carga=row["fecha_carga"]
        )
        registros.append(convocatoria)
    session.add_all(registros)
    session.commit()



def leer_ods() -> pd.DataFrame:
    """
    Consulta staging.ofertas_servir usando ORM (SQLAlchemy session.query),
    convierte cada objeto ORM a dict y construye el DataFrame.
    """
    session = get_session()
    try:
        log.info("🔍 Consultando ods.ofertas_servir via ORM ...")
        registros = session.query(ConvocatoriaODS).all()

        if not registros:
            raise ValueError("La tabla staging.ofertas_servir está vacía")

        # Convertir objetos ORM → lista de dicts (excluye atributos internos de SQLAlchemy)
        datos = [
            {
                col.name: getattr(r, col.name)
                for col in ConvocatoriaODS.__table__.columns
            }
            for r in registros
        ]

        df = pd.DataFrame(datos)
        log.info("✅ %d filas cargadas via ORM → DataFrame", len(df))
        return df

    finally:
        session.close()
