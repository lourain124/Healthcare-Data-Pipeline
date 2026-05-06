"""Pruebas rapidas sobre la capa de staging (sin red ni ficheros grandes)."""

from __future__ import annotations

import pandas as pd

from staging import clean_data


def test_clean_data_dupplicates_and_age_median() -> None:
    df = pd.DataFrame(
        {
            "age": [30, 30, None],
            "sex": ["F", "F", "M"],
        }
    )
    out = clean_data(df)
    assert len(out) == 2
    assert out["age"].isna().sum() == 0
    assert float(out["age"].iloc[0]) == 30.0
