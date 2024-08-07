import structlog
import time
import threading

from typing import Optional

from .webhook import requests_session

log = structlog.get_logger(__name__)

CHECK_EXPIRE_INTERVAL: float = 3.0
HEARTBEAT_INTERVAL: float = 10.0


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
        self.expired = False

        self._should_shutdown = False
        self._check_expired_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None

        self._session = requests_session(report_key)

    def start(self):
        """
        Start the background worker thread.
        """
        self._check_expired_thread = threading.Thread(target=self._run_check_expired)
        self._check_expired_thread.start()

        self._heartbeat_thread = threading.Thread(target=self._run_heartbeat)
        self._heartbeat_thread.start()

    def stop(self):
        """
        Trigger the termination of the worker thread.
        """
        self._should_shutdown = True

    def _run_check_expired(self):
        while not self._should_shutdown:
            self._check_expired()

            time.sleep(CHECK_EXPIRE_INTERVAL)

    def _run_heartbeat(self):
        while not self._should_shutdown:
            self._heartbeat()

            time.sleep(HEARTBEAT_INTERVAL)

    def join(self) -> None:
        if self._check_expired_thread is not None:
            self._check_expired_thread.join()

        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join()

        log.info("Worker is down")

    def prepare(self):
        self._report("prepare")

    def idle(self):
        self._report("idle")

    def busy(self):
        self._report("busy")

    def shutdown(self):
        self._report("shutdown")

    def _report(self, status: str):
        if not self._can_report():
            return

        try:
            resp = self._session.put(
                f"{self.report_url}/status/{self.id}?status={status}"
            )
            resp.raise_for_status()
        except Exception:
            log.warn("failed to report worker status")

    def _check_expired(self) -> bool:
        if not self._can_report():
            return

        try:
            resp = self._session.get(f"{self.report_url}/expired/{self.id}")
            resp.raise_for_status()

            self.expired = resp.json()

        except Exception:
            log.warn("failed to check worker expired")

    def _heartbeat(self) -> bool:
        if not self._can_report():
            return False

        try:
            resp = self._session.put(f"{self.report_url}/heartbeat/{self.id}")
            resp.raise_for_status()
        except Exception:
            log.warn("failed to heartbeat")

    def next_queue(self) -> str:
        if not self._can_report():
            return self.queue

        try:
            resp = self._session.get(f"{self.report_url}/next_queue/{self.id}")
            resp.raise_for_status()

            return resp.json().get("queue")

        except Exception as e:
            log.warn("failed to get next queue", error=e)

            return self.queue

    def _can_report(self):
        return self.id and self.report_url
