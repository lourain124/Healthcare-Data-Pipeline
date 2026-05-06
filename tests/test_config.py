"""Comprueba que la configuracion apunta a rutas bajo el proyecto."""

from __future__ import annotations

import config


def test_paths_under_project() -> None:
    assert config.RAW_DIR.is_relative_to(config.PROJECT_ROOT)
    assert config.PROCESSED_CSV.name == "thyroid_clean.csv"
    assert config.FINAL_CSV.name == "thyroid_model_ready.csv"
