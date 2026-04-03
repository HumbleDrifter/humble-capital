import queue
import threading
import traceback

_trade_queue = queue.Queue()
_worker_started = False


def submit_job(job):
    _trade_queue.put(job)


def queue_size():
    return _trade_queue.qsize()


def _worker_loop():

    from services.execution_service import process_trade_job

    while True:

        job = _trade_queue.get()

        try:
            process_trade_job(job)

        except Exception:
            traceback.print_exc()

        finally:
            _trade_queue.task_done()


def start_execution_worker():

    global _worker_started

    if _worker_started:
        return

    t = threading.Thread(target=_worker_loop, daemon=True)
    t.start()

    _worker_started = True
