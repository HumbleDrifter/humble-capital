import logging
import queue
import threading
import traceback

_trade_queue = queue.Queue()
_worker_started = False
_worker_lock = threading.Lock()
logger = logging.getLogger(__name__)


def submit_job(job):
    start_execution_worker()
    _trade_queue.put(job)
    logger.info(
        "[execution_queue] submitted proposal_id=%s broker=%s asset_class=%s queue_size=%s",
        str((job or {}).get("proposal_id") or "").strip(),
        str((job or {}).get("broker") or "").strip(),
        str((job or {}).get("asset_class") or "").strip(),
        _trade_queue.qsize(),
    )


def queue_size():
    return _trade_queue.qsize()


def _worker_loop():
    logger.info("[execution_queue] worker thread started")
    try:
        from services.execution_service import process_trade_job
    except Exception:
        logger.exception("[execution_queue] failed importing process_trade_job")
        raise

    while True:
        job = None

        try:
            job = _trade_queue.get()
            logger.info(
                "[execution_queue] dequeued proposal_id=%s broker=%s asset_class=%s queue_size=%s",
                str((job or {}).get("proposal_id") or "").strip(),
                str((job or {}).get("broker") or "").strip(),
                str((job or {}).get("asset_class") or "").strip(),
                _trade_queue.qsize(),
            )
            process_trade_job(job)
        except Exception:
            logger.exception(
                "[execution_queue] job failed proposal_id=%s",
                str((job or {}).get("proposal_id") or "").strip(),
            )
            traceback.print_exc()

        finally:
            if job is not None:
                _trade_queue.task_done()
                logger.info(
                    "[execution_queue] task_done proposal_id=%s queue_size=%s",
                    str((job or {}).get("proposal_id") or "").strip(),
                    _trade_queue.qsize(),
                )


def start_execution_worker():
    global _worker_started

    with _worker_lock:
        if _worker_started:
            return

        t = threading.Thread(
            target=_worker_loop,
            name="execution-queue-worker",
            daemon=True,
        )
        t.start()

        _worker_started = True
        logger.info("[execution_queue] worker startup complete")
