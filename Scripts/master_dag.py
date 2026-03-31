from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="pipeline_servir_master",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # solo este se agenda
    catchup=False,
    tags=["master", "etl"],
) as dag:

    # 1. Ejecutar extracción
    t1 = TriggerDagRunOperator(
        task_id="run_extraccion",
        trigger_dag_id="extraccion_dag",
        wait_for_completion=True,
        
    )

    # 2. Ejecutar transformación
    t2 = TriggerDagRunOperator(
        task_id="run_transformacion",
        trigger_dag_id="transformacion_dag",
        wait_for_completion=True,
        
    )

    # 3. Ejecutar DW
    t3 = TriggerDagRunOperator(
        task_id="run_dw",
        trigger_dag_id="dw_dag",
        wait_for_completion=True,
    )

    t1 >> t2 >>  t3