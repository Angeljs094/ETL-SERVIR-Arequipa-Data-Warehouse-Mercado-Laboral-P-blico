from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

from project.db_config_ods import guardar_transformacion, engine
from project.transformaciones import (
    cargar_datos,
    normalizar_nulos,
    transformar_remuneracion,
    transformar_fechas,
    separar_ubicacion,
    transformar_vacantes,
    extraer_regimen_modalidad,
    normalizar_titulo,
    clasificar_nivel_educativo,
    crear_banda_salarial,
    normalizar_lugar,
    reporte_calidad,   
    limpiar_id_convocatoria,
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ── Tarea 1: Leer staging via ORM → DataFrame ────────────────
def tarea_cargar(**context):
    df = cargar_datos()   # ORM → DataFrame
    print(f"📥 {len(df)} filas cargadas desde staging.ofertas_servir")
    print(f"📋 Columnas: {list(df.columns)}")
    context["ti"].xcom_push(
        key="df_raw",
        value=df.to_json(orient="records", date_format="iso")
    )


# ── Tarea 2: Transformaciones ────────────────────────────────
def tarea_transformar(**context):
    raw = context["ti"].xcom_pull(key="df_raw", task_ids="cargar_datos")
    df  = pd.read_json(raw, orient="records")

    print(f"🔄 Transformando {len(df)} filas ...")

    pasos = [
        normalizar_nulos,
        transformar_remuneracion,
        transformar_fechas,
        separar_ubicacion,
        transformar_vacantes,
        extraer_regimen_modalidad,
        normalizar_titulo,
        clasificar_nivel_educativo,
        crear_banda_salarial,
        normalizar_lugar,
        limpiar_id_convocatoria,
    ]
    for fn in pasos:
        df = fn(df)
        print(f"   ✅ {fn.__name__}")

    # Columnas finales
    columnas_finales = [
        "id", "id_convocatoria", "oferta_laboral",
        "institucion", "region", "distrito",
        "regimen_laboral", "modalidad",
        "cantidad_vacantes", "salario", "banda_salarial",
        "nivel_educativo", "experiencia",
        "formacion_academica_perfil", "especializacion",
        "conocimiento", "competencias",
        "fecha_inicio", "fecha_fin", "duracion_oferta_dias",
        "link_postulacion", "fecha_carga",
    ]
    cols = [c for c in columnas_finales if c in df.columns]
    df   = df[cols]

    reporte_calidad(df)
    print(f"📊 Resultado: {df.shape[0]} filas × {df.shape[1]} columnas")
    context["ti"].xcom_push(
        key="df_clean",
        value=df.to_json(orient="records", date_format="iso")
    )


# ── Tarea 3: Guardar en analytics ────────────────────────────
def tarea_guardar(**context):
    raw = context["ti"].xcom_pull(key="df_clean", task_ids="transformar")
    df  = pd.read_json(raw, orient="records")
    #df.to_csv("/opt/airflow/dags/project/transformaciones.csv", index=False)
    #guardar_analytics(df)
    guardar_transformacion(df)
    print(f"✅ {len(df)} filas en analytics.ofertas_servir_clean")


# ── DAG ──────────────────────────────────────────────────────
with DAG(
    dag_id="transformacion_dag",
    description="ORM → staging.ofertas_servir → transformaciones → analytics",
    schedule="10 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["transformacion", "analytics", "servir"],
) as dag:

    t1 = PythonOperator(task_id="cargar_datos",      python_callable=tarea_cargar)
    t2 = PythonOperator(task_id="transformar",        python_callable=tarea_transformar)
    t3 = PythonOperator(task_id="guardar_analytics",  python_callable=tarea_guardar)

    t1 >> t2 >> t3