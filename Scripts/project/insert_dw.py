
import calendar
import pandas as pd
from project.db_config_dw import (
    Session,
    DimCargo, DimInstitucion, DimContrato, FactConvocatoria,
)

from sqlalchemy.dialects.postgresql import insert
# ══════════════════════════════════════════════════════════════
# HELPERS — lookup o insertar si no existe (SCD tipo 1)
# ══════════════════════════════════════════════════════════════

def _nv(val):
    """Convierte NaN / None → None. Deja pasar el resto."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    return val


def _get_pk(session, Model, filtro: dict, strict=False) -> int | None:

    if strict:
        obj = session.query(Model).filter_by(**filtro).first()
    else:
        filtro_limpio = {k: v for k, v in filtro.items() if v is not None}
        obj = session.query(Model).filter_by(**filtro_limpio).first() if filtro_limpio else None

    if obj is None:
        obj = Model(**filtro)
        session.add(obj)
        session.flush()

    pk_col = Model.__table__.primary_key.columns.keys()[0]
    return getattr(obj, pk_col)





# ══════════════════════════════════════════════════════════════
# CARGA PRINCIPAL
# ══════════════════════════════════════════════════════════════

def cargar_dw(df: pd.DataFrame) -> None:
    """
    Recibe el DataFrame leído de ods.ofertas_servir y carga el DW:
      1. Resuelve FK de cada dimensión via _get_pk()
      2. Resuelve FK de dim_tiempo via _fecha_a_dim()
      3. Inserta todos los hechos en fact_convocatoria
    """
    session = Session()
    #hechos  = []

    print(f"🏗️  Cargando {len(df)} registros en dw.fact_convocatoria ...")

    try:
        for _, row in df.iterrows():

            # ── dim_cargo ─────────────────────────────────────
            id_cargo = _get_pk(session, DimCargo, {
                "oferta_laboral"            : _nv(row.get("oferta_laboral")),
                "nivel_educativo"           : _nv(row.get("nivel_educativo")),
                "experiencia"               : _nv(row.get("experiencia")),
                "formacion_academica_perfil": _nv(row.get("formacion_academica_perfil")),
                "especializacion"           : _nv(row.get("especializacion")),
                "conocimiento"              : _nv(row.get("conocimiento")),
                "competencias"              : _nv(row.get("competencias")),
            })

            # ── dim_institucion ───────────────────────────────
            id_institucion = _get_pk(session, DimInstitucion, {
                "institucion": _nv(row.get("institucion")),
                "region"     : _nv(row.get("region")),
                "distrito"   : _nv(row.get("distrito")),
            })

            # ── dim_contrato ──────────────────────────────────
            id_contrato = _get_pk(session, DimContrato, {
                "regimen_laboral": _nv(row.get("regimen_laboral")),
                "modalidad"      : _nv(row.get("modalidad")),
                "banda_salarial" : _nv(row.get("banda_salarial")),
            })

            # ── dim_tiempo ────────────────────────────────────
            id_tiempo_inicio = _nv(row.get("fecha_inicio"))
            id_tiempo_fin    = _nv(row.get("fecha_fin"))

            # ── fact_convocatoria ─────────────────────────────
            

            stmt = insert(FactConvocatoria).values(
                id_convocatoria  = _nv(row.get("id_convocatoria")),
                id_cargo         = id_cargo,
                id_institucion   = id_institucion,
                id_contrato      = id_contrato,
                id_tiempo_inicio = id_tiempo_inicio,
                id_tiempo_fin    = id_tiempo_fin,
                cantidad_vacantes= _nv(row.get("cantidad_vacantes")),
                salario          = _nv(row.get("salario")),
                duracion_dias    = _nv(row.get("duracion_oferta_dias")),
                link_postulacion = _nv(row.get("link_postulacion")),
                fecha_carga      = _nv(row.get("fecha_carga")),
            ).on_conflict_do_nothing(
                index_elements=['id_convocatoria']
            )

            session.execute(stmt)

        session.commit()
        print(f"✅ Carga completada sin duplicados")
        #session.add_all(hechos)
        #print(f"✅ {len(hechos)} hechos insertados en dw.fact_convocatoria")

    except Exception as e:
        session.rollback()
        raise RuntimeError(f"Error al cargar DW: {e}")
    finally:
        session.close()