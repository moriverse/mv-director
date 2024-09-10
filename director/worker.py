import structlog
import time
import threading

from pydantic import BaseModel
from queue import Empty, Full, Queue
from typing import Optional

from .webhook import requests_session

log = structlog.get_logger(__name__)

NEXT_QUEUE_INTERVAL: float = 30.0
EVENT_INTERNAL: float = 0.01


class ReportEvent(BaseModel):
    status: str


class NextQueueEvent(BaseModel):
    pass


class Worker:

    def __init__(
        self,
        queue: str,
        id: Optional[str] = None,
        report_url: Optional[str] = None,
        report_key: Optional[str] = None,
    ):
        self.queue = queue
        self.id = id
        self.report_url = f"{report_url}/worker"

        self.switched = False
        self.expired = False

        self._event_thread: Optional[threading.Thread] = None
        self._next_queue_thread: Optional[threading.Thread] = None

        self._should_exit = threading.Event()
        self._events = Queue()

        self._session = requests_session(report_key)

    def start(self) -> None:
        self._event_thread = threading.Thread(target=self._run)
        self._event_thread.start()

        self._next_queue_thread = threading.Thread(target=self._run_next_queue)
        self._next_queue_thread.start()

    def stop(self) -> None:
        log.info("Stopping worker report.")

        self._should_exit.set()

    def join(self) -> None:
        if self._next_queue_thread is not None:
            self._next_queue_thread.join()

        if self._event_thread is not None:
            self._event_thread.join()

        log.info("Worker is down")

    def _run(self):
        while not self._should_exit.is_set() or not self._events.empty():
            try:
                event = self._events.get_nowait()

                if isinstance(event, NextQueueEvent):
                    self.next_queue()

                elif isinstance(event, ReportEvent):
                    self.report(event.status)

            except Empty:
                pass

            except:
                log.error("Worker report queue error", exc_info=True)

            time.sleep(EVENT_INTERNAL)

    def _run_next_queue(self):
        while not self._should_exit.is_set():
            try:
                self._events.put_nowait(NextQueueEvent())

            except Full:
                log.info("Event queue is full, dropping next queue event")

            time.sleep(NEXT_QUEUE_INTERVAL)

    def prepare(self):
        self._events.put_nowait(ReportEvent(status="prepare"))

    def idle(self):
        self._events.put_nowait(ReportEvent(status="idle"))

    def busy(self):
        self._events.put_nowait(ReportEvent(status="busy"))

    def shutdown(self):
        self._events.put_nowait(ReportEvent(status="shutdown"))

    def report(self, status: str):
        if not self._can_report():
            return

        try:
            resp = self._session.put(
                f"{self.report_url}/status/{self.id}?status={status}"
            )
            resp.raise_for_status()

        except:
            log.error("failed to report worker status", exc_info=True)

    def next_queue(self):
        if not self._can_report():
            return

        try:
            resp = self._session.get(f"{self.report_url}/next_queue/{self.id}")
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
        return self.id and self.report_url
