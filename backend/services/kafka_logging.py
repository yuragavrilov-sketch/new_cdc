from __future__ import annotations

import logging


class KafkaSocks5DeprecationFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            record.name == "kafka.net.manager"
            and record.getMessage() == "socks5_proxy is deprecated, use proxy_url instead"
        )


def install_kafka_log_filters() -> None:
    logger = logging.getLogger("kafka.net.manager")
    if any(isinstance(f, KafkaSocks5DeprecationFilter) for f in logger.filters):
        return
    logger.addFilter(KafkaSocks5DeprecationFilter())
