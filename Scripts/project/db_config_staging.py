# db_config_staging.py
from sqlalchemy import create_engine, Column, Integer, Text, TIMESTAMP,text
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import os

Base = declarative_base()

class OfertaServir(Base):
    __tablename__ = "ofertas_servir"
    __table_args__ = {"schema": "staging"}  # esquema staging

    id = Column(Integer, primary_key=True, autoincrement=True)
    titulo = Column(Text, nullable=True)
    lugar = Column(Text, nullable=True)
    ubicacion = Column(Text, nullable=True)
    id_convocatoria = Column(Text, nullable=True)
    numero_convocatoria = Column(Text, nullable=True)
    cantidad_vacantes = Column(Text, nullable=True)
    remuneracion = Column(Text, nullable=True)
    fecha_inicio_publicacion = Column(Text, nullable=True)
    fecha_fin_publicacion = Column(Text, nullable=True)
    experiencia = Column(Text, nullable=True)
    formacion_academica_perfil = Column(Text, nullable=True)
    especializacion = Column(Text, nullable=True)
    conocimiento = Column(Text, nullable=True)
    competencias = Column(Text, nullable=True)
    detalle = Column(Text, nullable=True)
    fecha_carga = Column(TIMESTAMP)

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
session = Session()

# Crear tabla si no existe
Base.metadata.create_all(engine)

def guardar_dataframe(df: pd.DataFrame):


    session.execute(text("TRUNCATE TABLE staging.ofertas_servir RESTART IDENTITY"))
    session.commit()
    registros = []
    for _, row in df.iterrows():
        oferta = OfertaServir(
            titulo=row.get("titulo"),
            lugar=row.get("lugar"),
            ubicacion=row.get("ubicacion"),
            id_convocatoria = row.get("id_convocatoria"),
            numero_convocatoria=row.get("numero_convocatoria"),
            cantidad_vacantes=row.get("cantidad_vacantes"),
            remuneracion=row.get("remuneracion"),
            fecha_inicio_publicacion=row.get("fecha_inicio_publicacion"),
            fecha_fin_publicacion=row.get("fecha_fin_publicacion"),
            experiencia=row.get("experiencia"),
            formacion_academica_perfil=row.get("formacion_academica_perfil"),
            especializacion=row.get("especializacion"),
            conocimiento=row.get("conocimiento"),
            competencias=row.get("competencias"),
            detalle=row.get("detalle"),
            fecha_carga=pd.Timestamp.now()
        )
        registros.append(oferta)
    try:
        session.add_all(registros)
        session.commit()
    except Exception as e:
        session.rollback()
        print("Error al guardar:", e)
