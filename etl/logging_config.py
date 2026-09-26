import logging
import os
import uuid
from datetime import datetime, timezone
from logging.handlers import SMTPHandler

import clickhouse_connect


class RunIdFilter(logging.Filter):
    """Attaches a run_id to every log record."""

    def __init__(self, run_id):
        super().__init__()
        self.run_id = run_id

    def filter(self, record):
        record.run_id = self.run_id
        return True


class ClickHouseLogHandler(logging.Handler):
    """Writes log records to a ClickHouse log table."""

    def __init__(self, clickhouse_params, table):
        super().__init__()
        self.clickhouse_params = clickhouse_params
        self.table = table
        self.client = None

    def _connect(self):
        self.client = clickhouse_connect.get_client(**self.clickhouse_params)

    def emit(self, record):
        try:
            if self.client is None:
                self._connect()

            self.client.insert(
                self.table,
                [(
                    datetime.fromtimestamp(record.created, tz=timezone.utc),
                    record.levelname,
                    record.getMessage(),
                    record.name,
                    record.funcName,
                    record.lineno,
                    record.pathname,
                    getattr(record, "run_id", None),
                )],
                column_names=[
                    "logged_at", "level", "message",
                    "logger", "func_name", "line_no", "pathname", "run_id"
                ]
            )
        except Exception:
            self.client = None
            self.handleError(record)

    def close(self):
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
        super().close()


class DynamicSubjectSMTPHandler(SMTPHandler):
    """Same as SMTPHandler, but puts the level and message in the subject."""

    def getSubject(self, record):
        return f"[{record.levelname}] Flight pipeline: {record.getMessage()[:80]}"


def build_email_handler():
    """Returns a handler that emails on ERROR and CRITICAL."""
    email = os.getenv("ALERT_EMAIL")

    handler = DynamicSubjectSMTPHandler(
        mailhost=("smtp.gmail.com", 587),
        fromaddr=email,
        toaddrs=[email],
        subject="Flight pipeline failed",
        credentials=(email, os.getenv("GOOGLE_EMAIL_APP_PASSWORD")),
        secure=(),
        timeout=10,
    )
    handler.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(filename)s | %(funcName)s | line %(lineno)d | run %(run_id)s\n\n%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    return handler


def new_run_id():
    return str(uuid.uuid4())