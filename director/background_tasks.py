import queue
import time
import threading
import typing as t
import structlog

from attrs import define

log = structlog.get_logger(__name__)

P = t.ParamSpec("P")


class BackgroundTasks:
    def __init__(self):
        self._thread: t.Optional[threading.Thread] = None
        self._control: queue.Queue = queue.Queue()

    def start(self) -> None:
        """
        Start the background tasks thread.
        """
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self) -> None:
        """
        Trigger the termination of the background tasks thread.
        """
        self._control.put_nowait(_Stop())

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join()

    def _run(self) -> None:
        while True:
            try:
                msg = self._control.get_nowait()
            except queue.Empty:
                continue

            if isinstance(msg, _Stop):
                break

            elif isinstance(msg, _BackgroundTask):
                try:
                    msg()

                except:
                    log.error(f"{msg} failed", exc_info=True)

            else:
                log.warn("unknown message on control queue", msg=msg)

    def add_task(
        self,
        func: t.Callable[P, t.Any],
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        self._control.put_nowait(_BackgroundTask(func, *args, **kwargs))


@define
class _Stop:
    pass


class _BackgroundTask:
    def __init__(
        self,
        func: t.Callable[P, t.Any],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self) -> None:

        start_time = time.perf_counter()

        self.func(*self.args, **self.kwargs)

        elapsed_time = time.perf_counter() - start_time
        log.info(
            f"[Background task]{self.func.__name__} executed in {elapsed_time:.2f}s"
        )
