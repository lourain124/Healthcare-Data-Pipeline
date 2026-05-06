"""
Capa *processed* (curado para consumo): lee la salida de ``staging``, aplica pasos finales del ETL
y deja un dataset listo para modelado o reporting.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

import config

logger = logging.getLogger(__name__)


def load_staged(path: Path | str | None = None) -> pd.DataFrame:
    logger.info("Cargando salida de staging...")
    p = Path(path) if path is not None else config.PROCESSED_CSV
    if not p.is_file():
        raise FileNotFoundError(f"No existe {p.resolve()}. Ejecuta antes ``python staging.py``.")
    return pd.read_csv(p)


def finalize_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Finalizando dataset (codificacion, nulos restantes)...")
    out = df.copy()

    cat_present = [c for c in config.CATEGORICAL_FOR_MODEL if c in out.columns]
    rest = [c for c in out.columns if c not in cat_present]

    parts: list[pd.DataFrame] = [out[rest]] if rest else []
    for col in cat_present:
        parts.append(pd.get_dummies(out[col], prefix=col, dtype=int))

    merged = pd.concat(parts, axis=1) if parts else pd.DataFrame()

    num_cols = merged.select_dtypes(include="number").columns
    for col in num_cols:
        med = merged[col].median()
        merged[col] = merged[col].fillna(med if pd.notna(med) else 0)

    return merged


def impute_all_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina todos los NaN restantes: numericos con mediana (o 0), texto/categoria con moda o ``__missing__``.
    Convierte columnas objeto mayormente numericas con ``to_numeric`` para poder imputar.
    """
    out = df.copy()
    before = int(out.isna().sum().sum())
    if before == 0:
        return out

    for col in out.columns:
        if not out[col].isna().any():
            continue
        series = out[col]

        if pd.api.types.is_numeric_dtype(series):
            med = pd.to_numeric(series, errors="coerce").median()
            filled = pd.to_numeric(series, errors="coerce")
            out[col] = filled.fillna(med if pd.notna(med) else 0)
            continue

        conv = pd.to_numeric(series, errors="coerce")
        non_null_orig = series.notna().sum()
        conv_ok = conv.notna().sum()
        # Si casi todos los valores no nulos son numericos, tratamos la columna como numerica
        if non_null_orig > 0 and conv_ok >= non_null_orig:
            med = conv.median()
            out[col] = conv.fillna(med if pd.notna(med) else 0)
            continue

        str_series = series.astype("string")
        mode = str_series.dropna().mode()
        fill = mode.iloc[0] if len(mode) > 0 else "__missing__"
        out[col] = str_series.fillna(fill)

    # Red de seguridad por tipos mixtos o columnas todo-NaN
    for col in out.columns[out.isna().any()]:
        if pd.api.types.is_numeric_dtype(out[col]) or pd.api.types.is_bool_dtype(out[col]):
            med = pd.to_numeric(out[col], errors="coerce").median()
            tmp = pd.to_numeric(out[col], errors="coerce")
            out[col] = tmp.fillna(med if pd.notna(med) else 0)
        else:
            out[col] = out[col].astype("string").fillna("__missing__")

    after = int(out.isna().sum().sum())
    logger.info(
        "Imputacion residual: %s celdas nulas tratadas; quedan %s.",
        before,
        after,
    )
    if after > 0:
        raise RuntimeError(f"Aun quedan {after} valores nulos tras imputacion; revisar tipos en columna(s).")
    return out


def validate_final_schema(df: pd.DataFrame) -> None:
    """Comprueba reglas minimas antes de escribir el artefacto final. Lanza ValueError si falla."""
    n = len(df)
    if n < config.MIN_FINAL_ROWS:
        raise ValueError(
            f"Salida final con menos filas que MIN_FINAL_ROWS ({config.MIN_FINAL_ROWS}): {n}"
        )
    for col in config.FINAL_REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Falta columna requerida en salida final: {col!r}")
    if not config.ALLOW_FINAL_NULLS and df.isna().any().any():
        bad = df.isna().sum()
        summary = bad[bad > 0].to_dict()
        raise ValueError(
            "Hay valores nulos y ALLOW_FINAL_NULLS=false; columnas afectadas: "
            f"{summary}"
        )


def report_quality(df: pd.DataFrame) -> None:
    n_null = int(df.isna().sum().sum())
    if n_null > 0:
        detail = df.isna().sum()
        logger.warning(
            "Quedan %s valores nulos: %s",
            n_null,
            detail[detail > 0].to_dict(),
        )
    else:
        logger.info("Sin valores nulos en la salida final.")
    logger.info("Forma final: %s filas x %s columnas.", df.shape[0], df.shape[1])


def save_final(df: pd.DataFrame, path: Path | str | None = None) -> None:
    logger.info("Guardando capa final...")
    p = Path(path) if path is not None else config.FINAL_CSV
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    logger.info("Guardado en %s", p.resolve())


def run() -> None:
    logger.info("Iniciando capa processed")

    df = load_staged()
    df = finalize_data(df)
    df = impute_all_nulls(df)
    validate_final_schema(df)
    report_quality(df)
    save_final(df)

    logger.info("Processed finalizado")


if __name__ == "__main__":
    from log_config import configure_logging

    configure_logging(config.LOG_LEVEL)
    run()
