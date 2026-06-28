# Proyecto Global Mart Consolidation Pipeline

Arquitectura ETL desarrollada con **Apache Beam, dbt, PostgreSQL y Airflow**, orientada a la construcción de una arquitectura tipo **Bronze–Silver–Gold** para el procesamiento analítico de datos de ventas.

---

# Estructura del proyecto

```text
proyecto/
│
├── airflow/
│   └── global_mart_consolidation_pipeline.py
│
├── beam/
│   └── pipeline.py
│
├── data/
│   ├── sales_data.csv
│   ├── status_logs.csv
│   ├── silver/
│   │   └── sales_silver.parquet
│   └── rejected/
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   ├── seeds/
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── postgres/
│
├── check_parquet.py
│
└── README.md
```

---

# Configuración del entorno

El proyecto fue desarrollado utilizando **WSL2 (Ubuntu)** debido a las dependencias requeridas por Apache Airflow, las cuales presentan incompatibilidades con Windows.

## Instalación de dependencias

```bash
pip install -r requirements.txt
```

o alternativamente:

```bash
pip install apache-beam
pip install pyarrow
pip install pandas
pip install psycopg2
pip install sqlalchemy
pip install dbt-postgres
pip install apache-airflow
```

---

# Variables de entorno

Para Airflow:

```bash
export AIRFLOW_HOME=~/airflow
```

Para PostgreSQL:

```text
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=global_mart
POSTGRES_USER=postgres
POSTGRES_PASSWORD=*****
```

---

# Ejecución del pipeline

## Apache Beam

```bash
python beam/pipeline.py
```

Genera:

```text
data/silver/sales_silver.parquet
```

y

```text
data/rejected/rejected_sales.csv
```

---

## Verificación del archivo Parquet

```bash
python check_parquet.py
```

---

## Carga de Silver hacia PostgreSQL

```bash
python dbt_project/load_silver_to_postgres.py
```

---

## Ejecución de dbt

```bash
cd dbt_project

dbt seed
dbt run
dbt test
```

---

## Ejecución de Airflow

Iniciar scheduler:

```bash
airflow scheduler
```

En otra terminal:

```bash
airflow webserver
```

La interfaz estará disponible en:

```text
http://localhost:8080
```

---

# Tecnologías utilizadas

- Apache Beam
- PyArrow
- dbt
- PostgreSQL
- Apache Airflow
- Pandas
- SQLAlchemy
- WSL2

---

# Observaciones

Durante el desarrollo se identificaron limitaciones asociadas a la interoperabilidad entre WSL2 y PostgreSQL ejecutado en Windows, lo que afectó la persistencia automática de la capa Gold y la generación de KPIs mediante dbt.

No obstante, se implementó satisfactoriamente:

- Procesamiento ETL con Apache Beam
- Manejo de errores mediante Side Outputs
- Generación de archivos Parquet
- Modelado analítico con dbt
- Definición del DAG de Airflow
- Arquitectura Silver–Gold propuesta en el diseño

---

# Integrantes

- Javiera Bobadilla
