# Healthcare Data Pipeline

Pequeño pipeline ETL en Python orientado a datos tabulares de salud. El flujo descarga el conjunto **Thyroid Disease** del repositorio UCI, lo normaliza por capas y deja un CSV listo para análisis o modelado en `data/final/`.

El diseño es deliberadamente lineal: cada script tiene una responsabilidad clara, y `main.py` los encadena para poder ejecutar todo de una vez o depurar paso a paso.

## Requisitos

- Python 3.10 o superior (el proyecto incluye `.python-version` como referencia para pyenv u herramientas similares).
- Conexión a Internet si ejecutas la ingesta desde la URL de UCI.

## Instalación local

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

En Linux o macOS, activa el entorno con `source venv/bin/activate`.

## Cómo ejecutarlo

**Pipeline completo** (ingestion → staging → processed):

```powershell
python main.py
```

Si ya tienes un CSV en `data/raw/` y no quieres volver a descargar:

```powershell
python main.py --skip-ingest
```

También puedes lanzar cada etapa por separado: `python ingestion.py`, `python staging.py`, `python processed.py`.

## Arquitectura del código

| Módulo          | Rol |
|-----------------|-----|
| `config.py`     | Rutas bajo el proyecto, URL del dataset, columnas de cleaning y lectura de `.env`. |
| `log_config.py` | Configuración única de `logging` (formato y nivel). |
| `ingestion.py`| Descarga desde UCI y escribe el raw en `data/raw/`. |
| `staging.py`  | Toma el `.csv` más reciente en `raw/`, aplica reglas de limpieza y guarda `data/processed/thyroid_clean.csv`. |
| `processed.py`| Lee el resultado de staging, codifica lo necesario y escribe `data/final/thyroid_model_ready.csv`. |
| `main.py`     | Orquesta el orden anterior. |

Los CSV generados no suelen versionarse: están listados en `.gitignore`; las carpetas bajo `data/` se mantienen con `.gitkeep`.

## Configuración

Centraliza cambios en `config.py`: rutas, URL del fichero UCI y listas como `NUMERIC_CLEAN_COLUMNS` o `CATEGORICAL_FOR_MODEL`. Si tus datos reales usan otros nombres de columnas, ajusta ahí antes de tocar la lógica dispersa en los módulos.

### Variables de entorno (`.env`)

Copia `.env.example` a `.env` y modifica lo que necesites. Ahí puedes fijar el nivel de log (`LOG_LEVEL`), el mínimo de filas que debe tener el CSV final (`MIN_FINAL_ROWS`), si permites nulos en la salida (`ALLOW_FINAL_NULLS`) y una lista de columnas obligatorias separadas por comas (`FINAL_REQUIRED_COLUMNS`). Si no creas `.env`, se usan los valores por defecto del ejemplo.

El pipeline usa **`logging`** en lugar de `print`; el formato incluye timestamp, nivel y nombre del módulo.

Tras **`finalize_data`**, **`impute_all_nulls`** reparte todos los NaN que queden (columnas numéricas como objeto incluidas: primero intenta `to_numeric`, luego mediana o 0; texto con moda o `__missing__`). Si algo siguiera nulo, el proceso lanza error.

Después **`validate_final_schema`** comprueba filas mínimas, columnas requeridas y, si `ALLOW_FINAL_NULLS=false` (valor por defecto en código si no defines `.env`), que no quede ningún NaN como red de seguridad.

Para trabajar en Jupyter u otro notebook, la carpeta **`notebooks/`** incluye una guía breve; los datos curados salen en `data/final/`.

## Pruebas automáticas

```powershell
pytest
```

Las dependencias de test van en el mismo `requirements.txt` (incluye `pytest`). En CI (GitHub Actions) se instala ese archivo y se ejecuta la suite en cada push o pull request a la rama principal.

## Docker

Útil cuando quieres el mismo entorno en otra máquina o en un runner sin instalar Python a mano.

Construir la imagen desde la raíz del repositorio:

```powershell
docker build -t healthcare-etl .
```

Ejecutar el pipeline con los datos persistidos en tu carpeta local:

```powershell
docker run --rm -v "${PWD}/data:/app/data" healthcare-etl
```

Para repetir solo staging y processed sobre un raw ya descargado:

```powershell
docker run --rm -v "${PWD}/data:/app/data" healthcare-etl python main.py --skip-ingest
```

Ejecutar tests dentro del contenedor:

```powershell
docker run --rm healthcare-etl pytest
```

Los archivos `Dockerfile` y `.dockerignore` evitan copiar el `venv` y reducen el contexto de build.

## Dataset

**Thyroid Disease** — UCI Machine Learning Repository, dataset ID 102. Ficha del conjunto: [archive.ics.uci.edu](https://archive.ics.uci.edu/dataset/102/thyroid+disease).

## Roadmap

Ideas naturales a partir de aquí: métricas de calidad de datos (Great Expectations u otro), empaquetado como librería instalable, orquestación externa (Airflow, Prefect) o despliegue del artefacto final en un lakehouse o base analítica.
