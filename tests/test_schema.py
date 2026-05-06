"""Validacion de esquema en capa processed."""

from __future__ import annotations

import pandas as pd
import pytest

from processed import impute_all_nulls, validate_final_schema


def test_validate_final_schema_rejects_too_few_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    import config

    monkeypatch.setattr(config, "MIN_FINAL_ROWS", 100)
    df = pd.DataFrame({"a": [1, 2]})
    with pytest.raises(ValueError, match="MIN_FINAL_ROWS"):
        validate_final_schema(df)


def test_validate_final_schema_requires_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    import config

    monkeypatch.setattr(config, "FINAL_REQUIRED_COLUMNS", ("missing_col",))
    df = pd.DataFrame({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="Falta columna"):
        validate_final_schema(df)


def test_impute_all_nulls_removes_everything() -> None:
    df = pd.DataFrame(
        {
            "num": [1.0, float("nan"), 3.0],
            "txt": ["a", pd.NA, "c"],
        }
    )
    out = impute_all_nulls(df)
    assert out.isna().sum().sum() == 0
    assert len(out) == 3


def test_validate_final_schema_rejects_nulls_when_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    import config

    monkeypatch.setattr(config, "ALLOW_FINAL_NULLS", False)
    monkeypatch.setattr(config, "MIN_FINAL_ROWS", 1)
    monkeypatch.setattr(config, "FINAL_REQUIRED_COLUMNS", ())
    df = pd.DataFrame({"a": [1.0, float("nan")]})
    with pytest.raises(ValueError, match="ALLOW_FINAL_NULLS"):
        validate_final_schema(df)
