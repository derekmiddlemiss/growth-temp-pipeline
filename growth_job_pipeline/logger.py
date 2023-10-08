from __future__ import annotations
import logging
import os
import sys
from typing import TYPE_CHECKING

from growth_job_pipeline.config import config

if TYPE_CHECKING:
    from uuid import UUID


def setup_logger(run_output_dir_path: str, run_id: UUID):
    log_level = logging.DEBUG if config("DEBUG", cast=bool) else logging.INFO
    log_file = os.path.join(run_output_dir_path, f"{run_id}.log")
    logging.getLogger("backoff").addHandler(logging.StreamHandler())
    logging.basicConfig(
        level=log_level,
        format=(
            "[%(asctime)s] %(levelname)s [module=%(name)s, line=%(lineno)s]"
            " %(message)s"
        ),
        handlers=[
            logging.FileHandler(filename=log_file, mode="a"),
            logging.StreamHandler(sys.stdout),
        ],
    )
