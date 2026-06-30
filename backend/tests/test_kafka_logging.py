from __future__ import annotations

import logging

from services.kafka_logging import KafkaSocks5DeprecationFilter


def test_kafka_socks5_deprecation_filter_only_suppresses_known_noise():
    flt = KafkaSocks5DeprecationFilter()

    noisy = logging.LogRecord(
        name="kafka.net.manager",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="socks5_proxy is deprecated, use proxy_url instead",
        args=(),
        exc_info=None,
    )
    real_error = logging.LogRecord(
        name="kafka.net.manager",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Connection failed: boom",
        args=(),
        exc_info=None,
    )

    assert flt.filter(noisy) is False
    assert flt.filter(real_error) is True
