import logging
from collections.abc import Generator
from typing import TextIO

from pydantic import ValidationError

from growth_job_pipeline.config import config
from growth_job_pipeline.yield_tsv_reader.models import YieldResult

logger = logging.getLogger(__name__)


def split_next_line_on_whitespace(file: TextIO) -> list[str]:
    # removes only carriage return chars \r
    # TODO @dsm fuller treatment might be needed if other MS-DOS control chars found in future
    return file.readline().replace("\r", "").split()


def yield_results_batcher(
    batch_size: int = 1000,
) -> Generator[list[YieldResult], None, None]:
    with open(config("YIELD_RESULTS_FILE"), "r") as f:
        column_names = split_next_line_on_whitespace(f)
        num_batches_read = 0
        fetch_next_batch = True
        while fetch_next_batch:
            results = []
            while len(results) < batch_size:
                try:
                    next_result = split_next_line_on_whitespace(f)
                    if not next_result:
                        fetch_next_batch = False
                        break
                    results.append(
                        YieldResult(**dict(zip(column_names, next_result)))
                    )
                except ValidationError as e:
                    logger.error(f"Error {e} validating yield results.")
                    raise e
            if results:
                num_batches_read += 1
                logger.debug(
                    f"Batch number={num_batches_read}, rows_in_batch={len(results)}"
                )
                yield results
