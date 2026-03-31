from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
#import pandas as pd
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from project.extraccion import (
    init_driver,
    seleccionar_departamento,
    extraer_convocatorias,
    entrar_ver_mas,
    extraer_detalle,
    volver_a_lista,
    recorrer_paginas
)
from project.db_config_staging import guardar_dataframe, engine

# ── Configuración ────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

DEPARTAMENTO = "AREQUIPA"



# ── Tarea 1: Verificar conexiones ────────────────────────────
def tarea_verificar_conexiones():
    import os
    import urllib.request
    from sqlalchemy import text

    # 1. Selenium
    selenium_url = os.getenv("SELENIUM_REMOTE_URL", "http://selenium-chrome:4444/wd/hub")
    print(f"🔌 Verificando Selenium en {selenium_url} ...")
    try:
        with urllib.request.urlopen(f"{selenium_url}/status", timeout=10) as resp:
            print(f"✅ Selenium OK — HTTP {resp.status}")
    except Exception as e:
        raise RuntimeError(f"❌ Selenium no disponible: {e}")

    # 2. PostgreSQL + esquema staging
    print("🔌 Verificando PostgreSQL ...")
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT current_database(), current_user")).fetchone()
            print(f"✅ PostgreSQL OK — DB: {row[0]}, usuario: {row[1]}")

        # 👇 IMPORTANTE: usar begin para operaciones que modifican
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))

        print("✅ Esquema 'staging' listo")

    except Exception as e:
        raise RuntimeError(f"❌ PostgreSQL no disponible: {e}")

    print("✅ Todas las conexiones OK")


# ── Tarea 2: Scraping ────────────────────────────────────────
def tarea_scraping(**context):
    driver = init_driver()
    #resultados = []

    try:
        print(f"🌍 Navegando al portal SERVIR ...")
        driver.get(
            "https://app.servir.gob.pe/DifusionOfertasExterno/faces/"
            "consultas/ofertas_laborales.xhtml"
        )

        print(f"📍 Seleccionando departamento: {DEPARTAMENTO}")
        seleccionar_departamento(driver, DEPARTAMENTO)

        #wait    = WebDriverWait(driver, 15)
       
        resultados = recorrer_paginas(driver)

    finally:
        driver.quit()
        print("🔒 Sesión Chrome cerrada")

    print(f"\n✅ Scraping completado — {len(resultados)} convocatorias extraídas")
    context["ti"].xcom_push(key="convocatorias", value=resultados)


# ── Tarea 3: Ingesta a PostgreSQL ────────────────────────────
def tarea_ingesta(**context):
    import pandas as pd 
    datos = context["ti"].xcom_pull(
        key="convocatorias", task_ids="scraping_servir"
    )

    if not datos:
        print("⚠️  XCom vacío — el scraping no devolvió resultados, no hay nada que guardar")
        return

    print(f"💾 Preparando {len(datos)} convocatorias para insertar ...")

    df = pd.DataFrame(datos)
    df["fecha_carga"] = pd.Timestamp.now()

    print(f"📋 Columnas en el DataFrame: {list(df.columns)}")
    print(f"📊 Primeras 3 filas:\n{df.head(3).to_string()}")
    
    df = df.rename(columns={
        "fecha inicio de publicación": "fecha_inicio_publicacion",
        "fecha fin de publicación": "fecha_fin_publicacion",
        "número de convocatoria": "numero_convocatoria",
        "formación académica - perfil": "formacion_academica_perfil"
    })

        # Eliminar duplicados y columnas conflictivas
    #if "número de convocatoria" in df.columns:
    #    df = df.drop(columns=["número de convocatoria"])
    #df = df.loc[:, ~df.columns.duplicated()]

    # Forzar tipos a string para evitar Series
    for col in ["numero_convocatoria", "fecha_inicio_publicacion", "fecha_fin_publicacion"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    #df.to_csv("/opt/airflow/dags/project/scrapeo.csv", index=False)
    #print("📂 CSV guardado en /opt/airflow/dags/project/scrapeo.csv")

   #df.to_csv("scrapeo.csv")
    guardar_dataframe(df)
    print(f"\n✅ {len(df)} registros guardados en staging.ofertas_servir")


# ── Definición del DAG ───────────────────────────────────────
with DAG(
    dag_id="extraccion_dag",
    description="ETL: scraping convocatorias SERVIR → PostgreSQL (staging)",
    schedule="0 6 * * *",            # todos los días a las 6 AM (hora Lima)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "servir", "selenium", "scraping"],
) as dag:

    t1 = PythonOperator(
        task_id="verificar_conexiones",
        python_callable=tarea_verificar_conexiones,
    )

    t2 = PythonOperator(
        task_id="scraping_servir",
        python_callable=tarea_scraping,
        execution_timeout=timedelta(minutes=45),
    )

    t3 = PythonOperator(
        task_id="ingesta_postgres",
        python_callable=tarea_ingesta,
    )

    t1 >> t2 >> t3
