"""
Carga datos tabulares desde una URL y los devuelve únicamente como ``pandas.DataFrame``.

Dataset Thyroid Disease (UCI, id 102): la ficha es HTML; el fichero ``thyroid0387.data`` tiene URL directa.

  - Ficha UCI: https://archive.ics.uci.edu/dataset/102/thyroid+disease
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

import pandas as pd

import config

logger = logging.getLogger(__name__)


def load_from_url(
    url: str,
    fmt: Literal["auto", "csv", "json"] = "auto",
    *,
    csv_kwargs: dict[str, Any] | None = None,
    json_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Descarga y parsea ``url``; la salida siempre es un ``DataFrame``."""
    ck = dict(csv_kwargs or {})
    jk = dict(json_kwargs or {})

    if fmt == "csv":
        return pd.read_csv(url, **ck)
    if fmt == "json":
        return pd.read_json(url, **jk)

    base = url.split("?", 1)[0].lower()
    if base.endswith(".json"):
        return pd.read_json(url, **jk)
    if base.endswith(".csv"):
        return pd.read_csv(url, **ck)

    try:
        return pd.read_csv(url, **ck)
    except Exception:
        return pd.read_json(url, **jk)


def load_uci_thyroid0387() -> pd.DataFrame:
    """``thyroid0387.data`` desde UCI como ``DataFrame`` (sin cabecera en el fichero)."""
    return load_from_url(
        config.UCI_THYROID0387_DATA_URL,
        fmt="csv",
        csv_kwargs={"header": None},
    )


def run_ingest(output_path: Path | str | None = None) -> Path:
    """
    Descarga el dataset UCI y lo guarda en ``data/raw/`` como CSV para que ``staging`` lo lea.
    """
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = Path(output_path) if output_path else config.DEFAULT_RAW_CSV
    logger.info("Descargando thyroid0387 (UCI)...")
    df = load_uci_thyroid0387()
    df.to_csv(path, index=False)
    logger.info("RAW guardado en %s", path.resolve())
    return path


if __name__ == "__main__":
    from log_config import configure_logging

    configure_logging(config.LOG_LEVEL)
    run_ingest()
