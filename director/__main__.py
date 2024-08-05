import logging
import os
import queue
import signal
import sys
import structlog
import uvicorn

from argparse import ArgumentParser
from cog.logging import setup_logging
from typing import Any
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from .director import Director
from .health_checker import Healthchecker, http_fetcher
from .http import Server, create_app
from .monitor import Monitor
from .worker import Worker

setup_logging(log_level=logging.INFO)
log = structlog.get_logger("cog.director")

# Enable OpenTelemetry if the env vars are present. If this block isn't
# run, all the opentelemetry calls are no-ops.
if "OTEL_SERVICE_NAME" in os.environ:
    trace.set_tracer_provider(TracerProvider())
    span_processor = BatchSpanProcessor(OTLPSpanExporter())
    trace.get_tracer_provider().add_span_processor(span_processor)  # type: ignore


def _die(signum: Any, frame: Any) -> None:
    log.warn("caught early SIGTERM: exiting immediately!")
    sys.exit(1)


# We are probably running as PID 1 so need to explicitly register a handler
# to die on SIGTERM. This will be overwritten once we start uvicorn.
signal.signal(signal.SIGINT, _die)
signal.signal(signal.SIGTERM, _die)

parser = ArgumentParser()

parser.add_argument("--worker-id", type=str)
parser.add_argument("--queue", type=str, required=True)
parser.add_argument("--consume-timeout", type=int, default=30)
parser.add_argument("--predict-timeout", type=int, default=1800)
parser.add_argument(
    "--max-failure-count",
    type=int,
    default=5,
    help="Maximum number of consecutive failures before the worker should exit",
)
parser.add_argument("--redis-url", type=str, required=True)
parser.add_argument("--report-url", type=str, required=False)

args = parser.parse_args()

events: queue.Queue = queue.Queue(maxsize=128)

config = uvicorn.Config(create_app(events=events), port=4900, log_config=None)
server = Server(config)
server.start()

healthchecker = Healthchecker(
    events=events, 
    fetcher=http_fetcher("http://localhost:5000/health-check")
)
healthchecker.start()

monitor = Monitor()
monitor.start()

worker = Worker(
    id=args.worker_id, 
    queue=args.queue, 
    report_url=args.report_url
)
worker.start()

director = Director(
    events=events,
    healthchecker=healthchecker,
    monitor=monitor,
    worker=worker,
    redis_url=args.redis_url,
    consume_timeout=args.consume_timeout,
    predict_timeout=args.predict_timeout,
    max_failure_count=args.max_failure_count,
)

director.register_shutdown_hook(healthchecker.stop)
director.register_shutdown_hook(server.stop)
director.register_shutdown_hook(monitor.stop)
director.register_shutdown_hook(worker.stop)
director.start()

monitor.join()
healthchecker.join()
server.join()
worker.join()