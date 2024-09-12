import structlog
import time
import threading

from typing import Optional

from director.background_tasks import BackgroundTasks
from director.webhook import requests_session

log = structlog.get_logger(__name__)

NEXT_QUEUE_INTERVAL: float = 30.0


class Worker:
    def __init__(
        self,
        queue: str,
        background_tasks: BackgroundTasks,
        id: Optional[str] = None,
        report_url: Optional[str] = None,
        report_key: Optional[str] = None,
    ):
        self.background_tasks = background_tasks

        self.id = id
        self.report_url = f"{report_url}/worker"
        self.session = requests_session(report_key)

        self.queue = queue
        self.switched = False
        self.expired = False

        self._thread: Optional[threading.Thread] = None
        self._should_exit = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self) -> None:
        log.info("Stopping worker report.")

        self._should_exit.set()

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join()

        log.info("Worker is down")

    def _run(self):
        while not self._should_exit.is_set():
            self.next_queue()
            time.sleep(NEXT_QUEUE_INTERVAL)

    def prepare(self):
        self.report("prepare")

    def idle(self):
        self.report("idle")

    def busy(self):
        self.report("busy")

    def shutdown(self):
        self.report("shutdown")

    def report(self, status: str):
        if not self._can_report():
            return

        self.background_tasks.add_task(self._report, status)

    def _report(self, status: str):
        try:
            resp = self.session.put(
                f"{self.report_url}/status/{self.id}?status={status}"
            )
            resp.raise_for_status()

        except:
            log.error("failed to report worker status", exc_info=True)

    def next_queue(self):
        if not self._can_report():
            return

        self.background_tasks.add_task(self._next_queue)

    def _next_queue(self):
        try:
            resp = self.session.get(f"{self.report_url}/next_queue/{self.id}")
            resp.raise_for_status()

            queue = resp.json().get("queue")

            self.expired = queue is None
            if queue != self.queue:
                # Update worker queue.
                self.switched = queue != self.queue
                self.queue = queue

        except:
            log.error("failed to get next queue", exc_info=True)

    def _can_report(self):
        can_report = self.id and self.report_url

        if not can_report:
            log.info(f"Worker cannot report. ID: {self.id} URL: {self.report_url}")

        return can_report
