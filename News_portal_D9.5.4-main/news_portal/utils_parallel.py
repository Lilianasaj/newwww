from multiprocessing import Pool, cpu_count
import logging

logger = logging.getLogger(__name__)

def process_in_parallel(items, worker_fn, processes=None, chunk_size=1):
    if processes is None:
        processes = max(1, cpu_count() - 1)
    logger.info(f"Starting multiprocessing with {processes} processes")

    results = []
    with Pool(processes=processes) as pool:
        results = pool.map(worker_fn, items)
    return results
