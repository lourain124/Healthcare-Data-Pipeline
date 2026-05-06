"""
Staging simple: toma el CSV mas reciente en ``data/raw/``, limpia y guarda en ``data/processed/``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

import config

logger = logging.getLogger(__name__)


def get_latest_file() -> str:
    paths = list(config.RAW_DIR.glob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"No hay archivos CSV en {config.RAW_DIR!s}.")
    latest = max(paths, key=lambda p: p.stat().st_ctime)
    return str(latest)


def load_data(path: str) -> pd.DataFrame:
    logger.info("Cargando datos RAW desde %s", path)
    return pd.read_csv(path)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Limpiando datos (staging)...")

    df = df.copy()
    df.replace("?", pd.NA, inplace=True)
    df.drop_duplicates(inplace=True)

    for col in config.NUMERIC_CLEAN_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "age" in df.columns:
        df["age"] = df["age"].fillna(df["age"].median())

    if "sex" in df.columns:
        df["sex"] = df["sex"].map({"F": "Female", "M": "Male"})

    return df


def save_data(df: pd.DataFrame) -> None:
    logger.info("Guardando datos procesados...")
    out = Path(config.PROCESSED_CSV)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    logger.info("Guardado en %s", config.PROCESSED_CSV)


def run() -> None:
    logger.info("Iniciando staging")

    latest_file = get_latest_file()
    df = load_data(latest_file)
    df = clean_data(df)
    save_data(df)

    logger.info("Staging finalizado")


if __name__ == "__main__":
    from log_config import configure_logging

    configure_logging(config.LOG_LEVEL)
    run()
