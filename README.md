# Proyecto-BD-GRAN-ESCALA

# Global Mart Consolidation Pipeline

Proyecto desarrollado para el Workshop de Data Engineering 2026.

La solución implementa un pipeline ETL utilizando Apache Beam,
Parquet, PostgreSQL, dbt y Apache Airflow para construir una
arquitectura analítica basada en capas Silver y Gold.

---

# Estructura del proyecto

```text
PROYECTO/

├── airflow/
│   └── global_mart_consolidation_pipeline.py
│
├── airflow_home/
│
├── beam/
│   └── pipeline.py
│
├── data/
│   ├── sales_data.csv
│   ├── status_logs.csv
│   ├── silver/
│   │   └── sales_silver.parquet
│   │
│   └── rejected/
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   │
│   ├── tests/
│   ├── seeds/
│   ├── target/
│   ├── logs/
│   ├── dbt_packages/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── .user.yml
│
├── postgres/
│
├── load_silver_to_postgres.py
│
└── check_parquet.py
```

---

# Configuración del entorno

Crear entorno virtual

```bash
python -m venv venv
```

Activar entorno

Linux / WSL

```bash
source venv/bin/activate
```

Windows

```bash
venv\Scripts\activate
```

Instalar dependencias

```bash
pip install -r requirements.txt
```

---

# Variables de entorno

Configurar PostgreSQL:

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=global_mart
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
```

Configurar Airflow:

```bash
AIRFLOW_HOME=~/airflow
```

---

# Ejecución del pipeline

## 1. Ejecutar Apache Beam

```bash
python beam/pipeline.py
```

Salida generada

```text
data/silver/sales_silver.parquet
```

---

## 2. Verificar Parquet

```bash
python check_parquet.py
```

---

## 3. Cargar Silver en PostgreSQL

```bash
python load_silver_to_postgres.py
```

Tabla generada

```text
raw_sales_silver
```

---

## 4. Ejecutar dbt

Instalar dependencias

```bash
dbt deps
```

Ejecutar modelos

```bash
dbt run
```

Validar modelos

```bash
dbt test
```

---

## 5. Ejecutar Airflow

Inicializar base de datos

```bash
airflow db init
```

Ejecutar scheduler

```bash
airflow scheduler
```

Ejecutar servidor web

```bash
airflow webserver
```

---

# DAG implementado

```text
extract_and_transform_silver

↓

sensor_silver_data

↓

load_silver_to_postgres

↓

load_and_model_gold
```

Archivo:

```text
airflow/global_mart_consolidation_pipeline.py
```

---

# Modelos dbt

La capa analítica está organizada en tres niveles:

```text
staging/
intermediate/
marts/
```

permitiendo separar:

- limpieza de datos;
- transformaciones intermedias;
- indicadores finales;
- tablas de negocio.

---

# Dependencias principales

- Apache Beam
- PyArrow
- pandas
- PostgreSQL
- SQLAlchemy
- psycopg2
- dbt
- Apache Airflow

---

# Limitaciones conocidas

La ejecución simultánea de PostgreSQL sobre Windows y Airflow en WSL
puede requerir configuraciones adicionales de conectividad.

En futuros desarrollos se propone incorporar Docker para simplificar
el despliegue y la integración entre servicios.

---

# Integrantes

- Javiera Bobadilla
