"""
Rutas y parametros del pipeline (un solo lugar para integrar el proyecto).

Las rutas se resuelven respecto a este archivo. Opcionalmente se cargan overrides desde `.env`
(misma carpeta que este modulo); copia `.env.example` como `.env`.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FINAL_DIR = DATA_DIR / "final"

DEFAULT_RAW_CSV = RAW_DIR / "thyroid_raw.csv"
PROCESSED_CSV = PROCESSED_DIR / "thyroid_clean.csv"
FINAL_CSV = FINAL_DIR / "thyroid_model_ready.csv"

# Logging (ver tambien LOG_LEVEL en .env)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Validacion salida final (processed)
MIN_FINAL_ROWS = int(os.getenv("MIN_FINAL_ROWS", "1"))
ALLOW_FINAL_NULLS = os.getenv("ALLOW_FINAL_NULLS", "false").lower() in ("1", "true", "yes")
_cols = os.getenv("FINAL_REQUIRED_COLUMNS", "").strip()
FINAL_REQUIRED_COLUMNS = tuple(c.strip() for c in _cols.split(",") if c.strip())

# UCI Thyroid Disease (id 102)
UCI_THYROID_DISEASE_PAGE = "https://archive.ics.uci.edu/dataset/102/thyroid+disease"
UCI_THYROID0387_DATA_URL = (
    "https://archive.ics.uci.edu/ml/machine-learning-databases/thyroid-disease/thyroid0387.data"
)

# Staging: columnas numericas y categoricas conocidas (ajusta si tus CSV traen otros nombres)
NUMERIC_CLEAN_COLUMNS = ("age", "TSH", "T3", "TT4")
CATEGORICAL_FOR_MODEL = ("sex",)
