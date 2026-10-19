"""JSON logs, one event per line, so job_id / task_id / worker_id can be grepped across services."""

import logging

import structlog


def configure_logging(level: str, service: str) -> None:
    logging.basicConfig(format="%(message)s", level=level.upper())
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[level.upper()]
        ),
    )
    structlog.contextvars.bind_contextvars(service=service)
