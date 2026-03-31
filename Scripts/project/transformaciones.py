import pandas as pd
import numpy as np
import re
import logging
from datetime import date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os

from project.db_config_staging import OfertaServir, Base

log = logging.getLogger(__name__)

# ── Conexión ─────────────────────────────────────────────────
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")
DB_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")   # nombre del servicio en docker-compose
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

def get_session():
    engine  = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


# ══════════════════════════════════════════════════════════════
# MÓDULO 1 — CARGA via ORM → DataFrame
# ══════════════════════════════════════════════════════════════
def cargar_datos() -> pd.DataFrame:
    """
    Consulta staging.ofertas_servir usando ORM (SQLAlchemy session.query),
    convierte cada objeto ORM a dict y construye el DataFrame.
    """
    session = get_session()
    try:
        log.info("🔍 Consultando staging.ofertas_servir via ORM ...")
        registros = session.query(OfertaServir).all()

        if not registros:
            raise ValueError("La tabla staging.ofertas_servir está vacía")

        # Convertir objetos ORM → lista de dicts (excluye atributos internos de SQLAlchemy)
        datos = [
            {
                col.name: getattr(r, col.name)
                for col in OfertaServir.__table__.columns
            }
            for r in registros
        ]

        df = pd.DataFrame(datos)
        log.info("✅ %d filas cargadas via ORM → DataFrame", len(df))
        return df

    finally:
        session.close()


# ══════════════════════════════════════════════════════════════
# MÓDULO 2 — NORMALIZACIÓN DE NULOS
# ══════════════════════════════════════════════════════════════
NULL_TOKENS = {
    "NO APLICA", "NO", ".", "N/A", "NA", "NINGUNO",
    "XX", "NO REQUIERE", "SIN INFORMACIÓN", "-", "",
    "RELACIONADO AL CARGO", "RELACIONADA A LAS FUNCIONES DEL CARGO",
    "RELACIONADOS AL CARGO",
}

def normalizar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
        df[col] = df[col].apply(
            lambda v: np.nan if str(v).strip().upper() in NULL_TOKENS else v
        )
    log.info("T1 normalizar_nulos — %d celdas → NaN", df.isna().sum().sum())
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 3 — REMUNERACIÓN
# ══════════════════════════════════════════════════════════════
def _parse_sueldo(valor) -> float:
    if pd.isna(valor):
        return np.nan
    limpio = re.sub(r"[Ss]/\.?\s*", "", str(valor))
    limpio = limpio.replace(",", "").strip()
    try:
        return float(limpio)
    except ValueError:
        return np.nan

def transformar_remuneracion(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["salario"] = df["remuneracion"].apply(_parse_sueldo)
    log.info(
        "T2 transformar_remuneracion — rango: S/. %.2f – S/. %.2f",
        df["salario"].min(), df["salario"].max(),
    )
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 4 — FECHAS
# ══════════════════════════════════════════════════════════════
def transformar_fechas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Detecta el nombre real de la columna en el DataFrame
    col_ini = next((c for c in ["fecha_inicio", "fecha_inicio_publicacion"] if c in df.columns), None)
    col_fin = next((c for c in ["fecha_fin",    "fecha_fin_publicacion"]    if c in df.columns), None)

    if col_ini:
        df["fecha_inicio"] = pd.to_datetime(df[col_ini], format="%d/%m/%Y", errors="coerce")
    if col_fin:
        df["fecha_fin"] = pd.to_datetime(df[col_fin], format="%d/%m/%Y", errors="coerce")

    if "fecha_inicio" in df.columns and "fecha_fin" in df.columns:
        df["duracion_oferta_dias"] = (df["fecha_fin"] - df["fecha_inicio"]).dt.days
        log.info(
            "T3 transformar_fechas — duración media: %.0f días | inválidas: %d",
            df["duracion_oferta_dias"].mean(),
            df["fecha_inicio"].isna().sum(),
        )
    df["fecha_carga"] = date.today()
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 5 — UBICACIÓN
# ══════════════════════════════════════════════════════════════
def separar_ubicacion(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    partes     = df["ubicacion"].str.split(" - ", n=1, expand=True)
    df["region"]   = partes[0].str.strip().str.title()
    df["distrito"] = partes[1].str.strip().str.title() if partes.shape[1] > 1 else np.nan
    log.info("T4 separar_ubicacion — %d distritos únicos", df["distrito"].nunique())
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 6 — VACANTES
# ══════════════════════════════════════════════════════════════
def transformar_vacantes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cantidad_vacantes"] = pd.to_numeric(
        df["cantidad_vacantes"], errors="coerce"
    ).astype("Int64")
    log.info("T5 transformar_vacantes — total: %d", df["cantidad_vacantes"].sum())
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 7 — RÉGIMEN Y MODALIDAD
# ══════════════════════════════════════════════════════════════
def _extraer_regimen(valor) -> str:
    if pd.isna(valor): return np.nan
    v = str(valor).upper()
    if "D.LEG 1057" in v or "D.LEG1057" in v: return "CAS - D.LEG 1057"
    if re.search(r"\b276\b", v): return "NOMBRADO - D.LEG 276"
    if re.search(r"\b728\b", v): return "PRIVADO - D.LEG 728"
    return "OTRO"

def _extraer_modalidad(valor) -> str:
    if pd.isna(valor): return np.nan
    v = str(valor).upper()
    if "SUPLENCIA" in v: return "SUPLENCIA"
    if "NECESIDAD TRANSITORIA" in v: return "NECESIDAD TRANSITORIA"
    return "OTRA"

def extraer_regimen_modalidad(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["regimen_laboral"] = df["numero_convocatoria"].apply(_extraer_regimen)
    df["modalidad"]       = df["numero_convocatoria"].apply(_extraer_modalidad)
    log.info("T6 regimen — %s", df["regimen_laboral"].value_counts().to_dict())
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 8 — TÍTULO NORMALIZADO Y DETALLE
# ══════════════════════════════════════════════════════════════
def normalizar_titulo(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["oferta_laboral"] = (
        df["titulo"].str.strip().str.lower()
        .str.replace(r"\s+", " ", regex=True).str.title()
    )
    log.info("T7 normalizar_titulo — %d registros", df["oferta_laboral"].notna().sum())
    if "detalle" in df.columns:
        df = df.rename(columns={"detalle": "link_postulacion"})
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 9 — NIVEL EDUCATIVO
# ══════════════════════════════════════════════════════════════
_NIVEL_MAP = [
    ("5_Posgrado",               ["DOCTORADO", "MAESTRÍA", "MAESTRIA", "POSGRADO"]),
    ("4_Universitario_Completo", ["TÍTULO", "TITULADO", "LICENCIADO", "LICENCIATURA",
                                  "INGENIERO", "ABOGADO", "MÉDICO", "MEDICO",
                                  "UNIVERSITARIO COMPLETO", "CIRUJANO"]),
    ("3_Bachiller_Egresado",     ["BACHILLER", "EGRESADO UNIVERSITARIO"]),
    ("2_Tecnico_Superior",       ["TÉCNICO", "TECNICO", "SUPERIOR", "IST"]),
    ("1_Educacion_Basica",       ["SECUNDARIA", "PRIMARIA"]),
]

def _clasificar_nivel(valor) -> str:
    if pd.isna(valor): return np.nan
    v = str(valor).upper()
    for nivel, kws in _NIVEL_MAP:
        if any(kw in v for kw in kws): return nivel
    return "0_Sin_Requisito"

def clasificar_nivel_educativo(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["nivel_educativo"] = df["formacion_academica_perfil"].apply(_clasificar_nivel)
    log.info("T8 nivel_educativo — %s", df["nivel_educativo"].value_counts().to_dict())
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 10 — BANDA SALARIAL
# ══════════════════════════════════════════════════════════════
_BINS   = [0, 1500, 2000, 3000, 4000, 6000, float("inf")]
_LABELS = ["1_Muy_Bajo(<1500)", "2_Bajo(1500-2000)", "3_Medio(2000-3000)",
           "4_Medio_Alto(3000-4000)", "5_Alto(4000-6000)", "6_Muy_Alto(>6000)"]

def crear_banda_salarial(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["banda_salarial"] = pd.cut(
        df["salario"], bins=_BINS, labels=_LABELS, right=True
    )
    log.info("T9 banda_salarial — %s",
             df["banda_salarial"].value_counts().sort_index().to_dict())
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 11 — INSTITUCIÓN
# ══════════════════════════════════════════════════════════════
def normalizar_lugar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["institucion"] = df["lugar"].str.strip().str.title()
    log.info("T10 normalizar_lugar — %d instituciones únicas", df["institucion"].nunique())
    return df

# ══════════════════════════════════════════════════════════════
# MÓDULO 12 — ID_CONVOCATORIA
# ══════════════════════════════════════════════════════════════
def limpiar_id_convocatoria(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Extrae solo los dígitos después de "N°"
    df["id_convocatoria"] = df["id_convocatoria"].apply(
        lambda v: re.search(r"\d+", str(v)).group(0) if pd.notnull(v) else None
    )
    return df


# ══════════════════════════════════════════════════════════════
# MÓDULO 13 — REPORTE DE CALIDAD
# ══════════════════════════════════════════════════════════════
def reporte_calidad(df: pd.DataFrame) -> None:
    sep = "═" * 62
    print(f"\n{sep}")
    print("📋  REPORTE DE CALIDAD POST-TRANSFORMACIÓN")
    print(sep)
    print(f"  Filas              : {len(df)}")
    print(f"  Columnas totales   : {len(df.columns)}")
    print(f"  Celdas con NaN     : {df.isna().sum().sum()}")

    cols_clave = ["salario", "fecha_inicio", "fecha_fin",
                  "duracion_oferta_dias", "nivel_educativo",
                  "regimen_laboral", "modalidad"]
    print("\n── Nulos por columna clave ──────────────────────────────")
    for col in [c for c in cols_clave if c in df.columns]:
        n = df[col].isna().sum()
        print(f"  {col:<28}: {n:>3} nulos ({n/len(df)*100:.1f}%)")

    if "salario" in df.columns:
        print("\n── Estadísticas salariales (PEN) ────────────────────────")
        for k, v in df["salario"].describe().items():
            print(f"  {k:<8}: S/. {v:>10,.2f}")

    if "regimen_laboral" in df.columns:
        print("\n── Distribución régimen laboral ─────────────────────────")
        for k, v in df["regimen_laboral"].value_counts().items():
            print(f"  {k:<30}: {v}")

    if "banda_salarial" in df.columns:
        print("\n── Distribución banda salarial ──────────────────────────")
        for k, v in df["banda_salarial"].value_counts().sort_index().items():
            print(f"  {k:<35}: {v}")
    print(f"\n{sep}\n")



