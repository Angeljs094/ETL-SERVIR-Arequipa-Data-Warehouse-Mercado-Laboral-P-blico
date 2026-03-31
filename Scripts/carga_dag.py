"""
dw_dag.py — Pipeline ODS → Data Warehouse
──────────────────────────────────────────
Lee ods.ofertas_servir via ORM y carga el star schema en dw.*
"""
 
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
 
from project.db_config_ods  import leer_ods
from project.db_config_dw   import inicializar_dw
from project.insert_dw   import cargar_dw
 
default_args = {
    "owner"       : "airflow",
    "retries"     : 1,
    "retry_delay" : timedelta(minutes=2),
}
 
 
# ── Tarea 1: Crear esquema y tablas del DW ───────────────────
def tarea_inicializar():
    inicializar_dw()          # esquema dw + todas las tablas
    
 
 
# ── Tarea 2: Leer ODS via ORM → XCom ────────────────────────
def tarea_leer_ods(**context):
    df = leer_ods()
    print(f"📥 {len(df)} filas leídas desde ods.ofertas_servir")
    print(f"📋 Columnas: {list(df.columns)}")
    context["ti"].xcom_push(
        key="df_ods",
        value=df.to_json(orient="records", date_format="iso")
    )
 
 
# ── Tarea 3: Cargar dimensiones + hechos ─────────────────────
def tarea_cargar_dw(**context):
    raw = context["ti"].xcom_pull(key="df_ods", task_ids="leer_ods")
    df  = pd.read_json(raw, orient="records")
 
    # JSON serializa fechas como ms epoch → restaurar a date
    for col in ["fecha_inicio", "fecha_fin", "fecha_carga"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
 
    print(f"🏗️  Iniciando carga DW con {len(df)} registros ...")
    cargar_dw(df)
 
 
# ── DAG ──────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dag",
    description="ODS → DW star schema (dims + fact_convocatoria)",
    schedule="20 6 * * *",      # 6:20 AM — después del transformacion_dag (6:10)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dw", "star-schema", "servir"],
) as dag:
 
    t1 = PythonOperator(task_id="inicializar_dw", python_callable=tarea_inicializar)
    t2 = PythonOperator(task_id="leer_ods",       python_callable=tarea_leer_ods)
    t3 = PythonOperator(task_id="cargar_dw",      python_callable=tarea_cargar_dw)
 
    t1 >> t2 >> t3



