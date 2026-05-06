"""
Orquesta el pipeline ETL en orden: ingesta → staging → processed.

Uso::

    python main.py              # los tres pasos
    python main.py --skip-ingest   # solo staging + processed (CSV ya en data/raw/)
"""

from __future__ import annotations

import argparse
import logging

import config
from log_config import configure_logging

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    configure_logging(config.LOG_LEVEL)

    parser = argparse.ArgumentParser(description="Pipeline ETL (ingesta → staging → processed).")
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="No descargar de UCI; usa el CSV más reciente que ya esté en data/raw/",
    )
    args = parser.parse_args(argv)

    logger.info("=" * 50)
    logger.info("Pipeline ETL")
    logger.info("=" * 50)

    if not args.skip_ingest:
        from ingestion import run_ingest

        run_ingest()

    from staging import run as run_staging

    run_staging()

    from processed import run as run_processed

    run_processed()

    logger.info("=" * 50)
    logger.info("Pipeline completo.")
    logger.info("=" * 50)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
