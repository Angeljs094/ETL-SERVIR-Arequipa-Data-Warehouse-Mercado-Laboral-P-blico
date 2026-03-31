# 🏛️ ETL SERVIR-Arequipa: Data Warehouse de Mercado Laboral Público

## 📖 Contexto y Problemática
El portal de SERVIR es la fuente primaria de empleo en el sector público peruano. Sin embargo, analizar la demanda laboral en regiones específicas como Arequipa presenta serios desafíos:
* **Volatilidad de Datos:** Las ofertas desaparecen del portal una vez finalizado el proceso, perdiendo trazabilidad histórica.
* **Datos No Estructurados:** Los salarios y requisitos vienen en formatos de texto sucios y heterogéneos (ej. "S/ 3,500.00", "3500", "Tres mil quinientos").
* **Esfuerzo Manual:** La recolección manual es ineficiente y propensa a errores humanos.

## 🎯 Solución Implementada y Objetivos
Se diseñó y desplegó un ecosistema de datos (ETL) automatizado, **completamente contenido en Docker**, para la extracción desatendida, limpieza y modelado analítico de las ofertas laborales. 

La solución centraliza la información bajo una **Arquitectura Medallion** (Staging, ODS, DW), habilitando un Data Warehouse optimizado (Esquema Estrella) que sirve de base para tableros de control en Power BI, permitiendo tomar decisiones basadas en datos reales.

---

## 🛠️ Stack Tecnológico
* **Orquestación:** Apache Airflow (Celery Executor)
* **Contenedores & Infraestructura:** Docker & Docker Compose
* **Ingesta / Web Scraping:** Selenium Grid (Chrome Headless)
* **Procesamiento de Datos:** Python (Pandas, Numpy, Regex)
* **Base de Datos & ORM:** PostgreSQL 16, SQLAlchemy

---

## 🏗️ Arquitectura y Flujo de Datos (Medallion)

El sistema se divide en tres fases críticas, orquestadas por un DAG maestro (`master_dag.py`) que dispara los procesos secuencialmente:

### 1. Fase I: Extracción (Capa Staging)
* **Proceso:** Scraping desatendido mediante un contenedor `selenium/standalone-chrome`. Se implementó un mapeo dinámico (`FIELD_MAP`) para abstraer los selectores CSS y hacer el scraper resiliente a cambios menores en la web.
* **Resultado:** Persistencia de los registros crudos en el esquema `staging.ofertas_servir` de PostgreSQL.

### 2. Fase II: Transformación (Capa ODS - Operational Data Store)
Es el "corazón" lógico del proyecto. Un pipeline en Pandas que procesa los datos crudos:
* **Limpieza y Parseo:** Limpieza de IDs, conversión de cadenas salariales complejas a valores numéricos (`Float64`), y normalización de fechas.
* **Categorización Inteligente:** Funciones de clasificación para el Nivel Educativo (Bachiller, Titulado, etc.), Régimen Laboral (CAS, 728, etc.) y creación de Bandas Salariales.
* **Resultado:** Datos normalizados con reporte de calidad integrado, cargados en `ods.ofertas_servir`.

### 3. Fase III: Modelado Analítico (Capa DW - Data Warehouse)
* **Modelo Dimensional:** Poblado de un Esquema Estrella (*Star Schema*) usando SQLAlchemy.
* **Dimensiones:** `dim_cargo`, `dim_institucion`, `dim_contrato` y dimensión de tiempo. Se implementó lógica de validación para asegurar que las dimensiones no se dupliquen y devuelvan siempre la clave subrogada correcta.
* **Tabla de Hechos:** `fact_convocatoria` consolida las métricas garantizando la idempotencia del pipeline mediante la cláusula `ON CONFLICT DO NOTHING`.

---

## 📁 Estructura del Repositorio

La arquitectura del código separa claramente la lógica de negocio (extracciones y transformaciones) de la lógica de orquestación (DAGs de Airflow) y la infraestructura.

```Text
├── dags/
│   ├── master_dag.py               # Orquestador principal (TriggerDagRunOperator)
│   ├── extraccion_dag.py           # DAG de scraping y carga a Staging
│   ├── transformacion_dag.py       # DAG de limpieza con Pandas y carga a ODS
│   └── carga_dag.py                # DAG de carga al Data Warehouse (Star Schema)
├── project/
│   ├── db_config_staging.py        # ORM (SQLAlchemy) y conexión a capa Staging
│   ├── db_config_ods.py            # ORM (SQLAlchemy) y conexión a capa ODS
│   ├── db_config_dw.py             # ORM (SQLAlchemy) y conexión a capa DW
│   ├── extraccion.py               # Lógica pura de Selenium Web Scraping
│   ├── transformaciones.py         # Módulos de limpieza, parseo y categorización
│   └── insert_dw.py                # Lógica de inserción idempotente y cruce de IDs
├── docker-compose.yml              # Infraestructura (Airflow, Postgres, Redis, Selenium)
├── requirements.txt                # Dependencias de Python (pandas, selenium, sqlalchemy, etc.)
└── README.md                       # Documentación del proyecto.
```

## 🐳 Contenerización y Reproducibilidad (Docker)

Para garantizar que este ecosistema de datos pueda ser desplegado y auditado en cualquier entorno (local o servidor) sin conflictos de dependencias, toda la infraestructura ha sido contenerizada utilizando **Docker** y **Docker Compose**.

El archivo `docker-compose.yml` aprovisiona y orquesta los siguientes servicios interconectados:

### 📦 Servicios Desplegados
* **Apache Airflow (Core):** * `airflow-webserver`: Interfaz gráfica (UI) expuesta en el puerto `8082`.
  * `airflow-scheduler`: Motor principal que monitorea y dispara los DAGs.
  * `airflow-worker`: Nodos de trabajo (Celery) para ejecución distribuida y en paralelo.
  * `airflow-triggerer`: Manejo de tareas asíncronas (Deferrable Operators).
* **Bases de Datos y Caché:**
  * `postgres:16`: Base de datos de metadatos para Airflow y almacenamiento relacional para las capas Staging, ODS y DW.
  * `redis:7.2`: Broker de mensajería para gestionar las colas de tareas de Celery.
* **Nodo de Scraping:**
  * `selenium-chrome`: Contenedor *standalone* (`selenium/standalone-chrome:latest`) aislado con 2GB de memoria compartida, encargado de ejecutar la navegación *headless* en el portal de SERVIR de manera invisible y escalable.

---

## Stack Tecnologicos
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)]()
[![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white)]()
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)]()
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)]()

**Autor:** Angel Teodoro Jaramillo Sulca  
**Rol:** Data Engineer  
**Contacto:** [LinkedIn](https://www.linkedin.com/in/angeljarads/) | [GitHub](https://github.com/Angeljs094)

---