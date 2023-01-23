#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import datetime
import logging
import socket
import string
import time
from collections import OrderedDict
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, InvalidStatsNameException
from airflow.typing_compat import Protocol

log = logging.getLogger(__name__)


class TimerProtocol(Protocol):
    """Type protocol for StatsLogger.timer."""

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_value, traceback):
        ...

    def start(self):
        """Start the timer."""
        ...

    def stop(self, send=True):
        """Stop, and (by default) submit the timer to StatsD."""
        ...


class StatsLogger(Protocol):
    """This class is only used for TypeChecking (for IDEs, mypy, etc)."""

    @classmethod
    def incr(
        cls,
        stat: str,
        count: int = 1,
        rate: int = 1,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str | None = None,
    ) -> None:
        """Increment stat."""

    @classmethod
    def decr(
        cls,
        stat: str,
        count: int = 1,
        rate: int = 1,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str | None = None,
    ) -> None:
        """Decrement stat."""

    @classmethod
    def gauge(
        cls,
        stat: str,
        value: float,
        rate: int = 1,
        delta: bool = False,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str | None = None,
    ) -> None:
        """Gauge stat."""

    @classmethod
    def timing(
        cls,
        stat: str,
        dt: float | datetime.timedelta,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str | None = None,
    ) -> None:
        """Stats timing."""

    @classmethod
    def timer(cls, *args, **kwargs) -> TimerProtocol:
        """Timer metric that can be cancelled."""


class Timer:
    """
    Timer that records duration, and optional sends to StatsD backend.

    This class lets us have an accurate timer with the logic in one place (so
    that we don't use datetime math for duration -- it is error prone).

    Example usage:

    .. code-block:: python

        with Stats.timer() as t:
            # Something to time
            frob_the_foos()

        log.info("Frobbing the foos took %.2f", t.duration)

    Or without a context manager:

    .. code-block:: python

        timer = Stats.timer().start()

        # Something to time
        frob_the_foos()

        timer.end()

        log.info("Frobbing the foos took %.2f", timer.duration)

    To send a metric:

    .. code-block:: python

        with Stats.timer("foos.frob"):
            # Something to time
            frob_the_foos()

    Or both:

    .. code-block:: python

        with Stats.timer("foos.frob") as t:
            # Something to time
            frob_the_foos()

        log.info("Frobbing the foos took %.2f", t.duration)
    """

    # pystatsd and dogstatsd both have a timer class, but present different API
    # so we can't use this as a mixin on those, instead this class is contains the "real" timer

    _start_time: int | None
    duration: int | None

    def __init__(self, real_timer=None):
        self.real_timer = real_timer

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        """Start the timer."""
        if self.real_timer:
            self.real_timer.start()
        self._start_time = time.perf_counter()
        return self

    def stop(self, send=True):
        """Stop the timer, and optionally send it to stats backend."""
        self.duration = time.perf_counter() - self._start_time
        if send and self.real_timer:
            self.real_timer.stop()


class DummyStatsLogger:
    """If no StatsLogger is configured, DummyStatsLogger is used as a fallback."""

    @classmethod
    def incr(cls, stat, count=1, rate=1, *, tags=None, name_tags=None, stat_suffix=None):
        """Increment stat."""

    @classmethod
    def decr(cls, stat, count=1, rate=1, *, tags=None, name_tags=None, stat_suffix=None):
        """Decrement stat."""

    @classmethod
    def gauge(cls, stat, value, rate=1, delta=False, *, tags=None, name_tags=None, stat_suffix=None):
        """Gauge stat."""

    @classmethod
    def timing(cls, stat, dt, *, tags=None, name_tags=None, stat_suffix=None):
        """Stats timing."""

    @classmethod
    def timer(cls, *args, **kwargs):
        """Timer metric that can be cancelled."""
        return Timer()


# Only characters in the character set are considered valid
# for the stat_name if stat_name_default_handler is used.
ALLOWED_CHARACTERS = set(string.ascii_letters + string.digits + "_.-")


def stat_name_default_handler(stat_name, max_length=250) -> str:
    """
    Validate the StatsD stat name.

    Apply changes when necessary and return the transformed stat name.
    """
    if not isinstance(stat_name, str):
        raise InvalidStatsNameException("The stat_name has to be a string")
    if len(stat_name) > max_length:
        raise InvalidStatsNameException(
            f"The stat_name ({stat_name}) has to be less than {max_length} characters."
        )
    if not all((c in ALLOWED_CHARACTERS) for c in stat_name):
        raise InvalidStatsNameException(
            f"The stat name ({stat_name}) has to be composed of ASCII "
            f"alphabets, numbers, or the underscore, dot, or dash characters."
        )
    return stat_name


def stat_name_influxdb_handler(stat_name, max_length=250) -> str:
    """InfluxDB-Statsd default name validator."""
    if not isinstance(stat_name, str):
        raise InvalidStatsNameException("The stat_name has to be a string")
    if len(stat_name) > max_length:
        raise InvalidStatsNameException(
            f"The stat_name ({stat_name}) has to be less than {max_length} characters."
        )
    if not all((c in ALLOWED_CHARACTERS | set([",", "="])) for c in stat_name):
        raise InvalidStatsNameException(
            f"The stat name ({stat_name}) has to be composed of ASCII "
            f"alphabets, numbers, or the underscore, dot, or dash characters."
        )
    return stat_name


def get_current_handler_stat_name_func() -> Callable[[str], str]:
    """Get Stat Name Handler from airflow.cfg."""
    handler = conf.getimport("metrics", "stat_name_handler")
    if handler is None:
        if conf.get("metrics", "statsd_influxdb_enabled", fallback=False):
            handler = stat_name_influxdb_handler
        else:
            handler = stat_name_default_handler
    return handler


T = TypeVar("T", bound=Callable)


def validate_stat(fn: T) -> T:
    """
    Check if stat name contains invalid characters.
    Log and not emit stats if name is invalid.
    """

    @wraps(fn)
    def wrapper(_self, stat=None, *args, **kwargs):
        try:
            if stat is not None:
                handler_stat_name_func = get_current_handler_stat_name_func()
                stat = handler_stat_name_func(stat)
            return fn(_self, stat, *args, **kwargs)
        except InvalidStatsNameException:
            log.exception("Invalid stat name: %s.", stat)
            return None

    return cast(T, wrapper)


class AllowListValidator:
    """Class to filter unwanted stats."""

    def __init__(self, allow_list=None):
        if allow_list:

            self.allow_list = tuple(item.strip().lower() for item in allow_list.split(","))
        else:
            self.allow_list = None

    def test(self, stat):
        """Test if stat is in the Allow List."""
        if self.allow_list is not None:
            return stat.strip().lower().startswith(self.allow_list)
        else:
            return True  # default is all metrics allowed


def concatenate_name_tags_to_stat_name(fn: T) -> T:
    """
    Concatenate name_tags to statname if aggregation_optimizer_enabled is False.
    This keeps backward compatibility with metric names like:
    'dagrun.<dag_id>.first_task_scheduling_delay' where <dag_id> is a name_tag.
    If aggregation_optimizer_enabled is True, name_tags will be published as
    metric tags instead.
    """

    @wraps(fn)
    def wrapper(self, stat=None, *args, tags=None, stat_suffix=None, name_tags=None, **kwargs):
        if self.aggregation_optimizer_enabled:
            if stat is not None:
                if not isinstance(stat, str):
                    log.exception("Invalid stat name: %s.", stat)
                    return None
                if stat_suffix:
                    stat += f".{stat_suffix}"
            if name_tags is not None:
                tags = tags or {}
                tags.update(name_tags)
            return fn(self, stat, *args, tags=tags, stat_suffix=stat_suffix, name_tags=name_tags, **kwargs)
        else:
            if stat is not None:
                if not isinstance(stat, str):
                    log.exception("Invalid stat name: %s.", stat)
                    return None
                if name_tags is not None:
                    for k, v in name_tags.items():
                        stat += f".{v}"
                if stat_suffix:
                    stat += f".{stat_suffix}"
            return fn(self, stat, *args, tags=tags, stat_suffix=stat_suffix, name_tags=None, **kwargs)

    return cast(T, wrapper)


def prepare_stat_with_tags(fn: T) -> T:
    """Add tags to stat with influxdb standard format if influxdb_tags_enabled is True."""

    @wraps(fn)
    def wrapper(self, stat=None, *args, tags=None, **kwargs):
        if self.influxdb_tags_enabled:
            if stat is not None and tags is not None:
                for k, v in tags.items():
                    stat += f",{k}={v}"
        return fn(self, stat, *args, tags=tags, **kwargs)

    return cast(T, wrapper)


class SafeStatsdLogger:
    """StatsD Logger."""

    def __init__(
        self,
        statsd_client,
        allow_list_validator=AllowListValidator(),
        aggregation_optimizer_enabled=False,
        influxdb_tags_enabled=False,
    ):
        self.statsd = statsd_client
        self.allow_list_validator = allow_list_validator
        self.aggregation_optimizer_enabled = aggregation_optimizer_enabled
        self.influxdb_tags_enabled = influxdb_tags_enabled

    @concatenate_name_tags_to_stat_name
    @prepare_stat_with_tags
    @validate_stat
    def incr(
        self,
        stat,
        count=1,
        rate=1,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
    ):
        """Increment stat."""
        if self.allow_list_validator.test(stat):
            return self.statsd.incr(stat, count, rate)
        return None

    @concatenate_name_tags_to_stat_name
    @prepare_stat_with_tags
    @validate_stat
    def decr(
        self,
        stat,
        count=1,
        rate=1,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
    ):
        """Decrement stat."""
        if self.allow_list_validator.test(stat):
            return self.statsd.decr(stat, count, rate)
        return None

    @concatenate_name_tags_to_stat_name
    @prepare_stat_with_tags
    @validate_stat
    def gauge(
        self,
        stat,
        value,
        rate=1,
        delta=False,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
    ):
        """Gauge stat."""
        if self.allow_list_validator.test(stat):
            return self.statsd.gauge(stat, value, rate, delta)
        return None

    @concatenate_name_tags_to_stat_name
    @prepare_stat_with_tags
    @validate_stat
    def timing(
        self,
        stat,
        dt,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
    ):
        """Stats timing."""
        if self.allow_list_validator.test(stat):
            return self.statsd.timing(stat, dt)
        return None

    @concatenate_name_tags_to_stat_name
    @prepare_stat_with_tags
    @validate_stat
    def timer(
        self,
        stat=None,
        *args,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
        **kwargs,
    ):
        """Timer metric that can be cancelled."""
        if stat and self.allow_list_validator.test(stat):
            return Timer(self.statsd.timer(stat, *args, **kwargs))
        return Timer()


class SafeDogStatsdLogger:
    """DogStatsd Logger."""

    def __init__(
        self,
        dogstatsd_client,
        allow_list_validator=AllowListValidator(),
        metrics_tags=False,
        aggregation_optimizer_enabled=False,
    ):
        self.dogstatsd = dogstatsd_client
        self.allow_list_validator = allow_list_validator
        self.metrics_tags = metrics_tags
        self.aggregation_optimizer_enabled = aggregation_optimizer_enabled

    @concatenate_name_tags_to_stat_name
    @validate_stat
    def incr(
        self,
        stat,
        count=1,
        rate=1,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
    ):
        """Increment stat."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [f"{key}:{value}" for key, value in tags.items()]
        else:
            tags_list = []
        if self.allow_list_validator.test(stat):
            return self.dogstatsd.increment(metric=stat, value=count, tags=tags_list, sample_rate=rate)
        return None

    @concatenate_name_tags_to_stat_name
    @validate_stat
    def decr(
        self,
        stat,
        count=1,
        rate=1,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
    ):
        """Decrement stat."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [f"{key}:{value}" for key, value in tags.items()]
        else:
            tags_list = []
        if self.allow_list_validator.test(stat):
            return self.dogstatsd.decrement(metric=stat, value=count, tags=tags_list, sample_rate=rate)
        return None

    @concatenate_name_tags_to_stat_name
    @validate_stat
    def gauge(
        self,
        stat,
        value,
        rate=1,
        delta=False,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
    ):
        """Gauge stat."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [f"{key}:{value}" for key, value in tags.items()]
        else:
            tags_list = []
        if self.allow_list_validator.test(stat):
            return self.dogstatsd.gauge(metric=stat, value=value, tags=tags_list, sample_rate=rate)
        return None

    @concatenate_name_tags_to_stat_name
    @validate_stat
    def timing(
        self,
        stat,
        dt: float | datetime.timedelta,
        *,
        tags: dict[str, str] | None = None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
    ):
        """Stats timing."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [f"{key}:{value}" for key, value in tags.items()]
        else:
            tags_list = []
        if self.allow_list_validator.test(stat):
            if isinstance(dt, datetime.timedelta):
                dt = dt.total_seconds()
            return self.dogstatsd.timing(metric=stat, value=dt, tags=tags_list)
        return None

    @concatenate_name_tags_to_stat_name
    @validate_stat
    def timer(
        self,
        stat=None,
        *args,
        tags=None,
        name_tags: OrderedDict[str, str] | None = None,
        stat_suffix: str = None,
        **kwargs,
    ):
        """Timer metric that can be cancelled."""
        if self.metrics_tags and isinstance(tags, dict):
            tags_list = [f"{key}:{value}" for key, value in tags.items()]
        else:
            tags_list = []
        if stat and self.allow_list_validator.test(stat):
            return Timer(self.dogstatsd.timed(stat, *args, tags=tags_list, **kwargs))
        return Timer()


class _Stats(type):
    factory = None
    instance: StatsLogger | None = None

    def __getattr__(cls, name):
        if not cls.instance:
            try:
                cls.instance = cls.factory()
            except (socket.gaierror, ImportError) as e:
                log.error("Could not configure StatsClient: %s, using DummyStatsLogger instead.", e)
                cls.instance = DummyStatsLogger()
        return getattr(cls.instance, name)

    def __init__(cls, *args, **kwargs):
        super().__init__(cls)
        if cls.__class__.factory is None:
            is_datadog_enabled_defined = conf.has_option("metrics", "statsd_datadog_enabled")
            if is_datadog_enabled_defined and conf.getboolean("metrics", "statsd_datadog_enabled"):
                cls.__class__.factory = cls.get_dogstatsd_logger
            elif conf.getboolean("metrics", "statsd_on"):
                cls.__class__.factory = cls.get_statsd_logger
            else:
                cls.__class__.factory = DummyStatsLogger

    @classmethod
    def get_statsd_logger(cls):
        """Returns logger for StatsD."""
        # no need to check for the scheduler/statsd_on -> this method is only called when it is set
        # and previously it would crash with None is callable if it was called without it.
        from statsd import StatsClient

        stats_class = conf.getimport("metrics", "statsd_custom_client_path", fallback=None)

        if stats_class:
            if not issubclass(stats_class, StatsClient):
                raise AirflowConfigException(
                    "Your custom StatsD client must extend the statsd.StatsClient in order to ensure "
                    "backwards compatibility."
                )
            else:
                log.info("Successfully loaded custom StatsD client")

        else:
            stats_class = StatsClient

        statsd = stats_class(
            host=conf.get("metrics", "statsd_host"),
            port=conf.getint("metrics", "statsd_port"),
            prefix=conf.get("metrics", "statsd_prefix"),
        )
        allow_list_validator = AllowListValidator(conf.get("metrics", "statsd_allow_list", fallback=None))
        aggregation_optimizer_enabled = conf.get(
            "metrics", "statsd_aggregation_optimized_naming_enabled", fallback=False
        )
        influxdb_tags_enabled = conf.get("metrics", "statsd_influxdb_enabled", fallback=False)
        return SafeStatsdLogger(
            statsd, allow_list_validator, aggregation_optimizer_enabled, influxdb_tags_enabled
        )

    @classmethod
    def get_dogstatsd_logger(cls):
        """Get DataDog StatsD logger."""
        from datadog import DogStatsd

        dogstatsd = DogStatsd(
            host=conf.get("metrics", "statsd_host"),
            port=conf.getint("metrics", "statsd_port"),
            namespace=conf.get("metrics", "statsd_prefix"),
            constant_tags=cls.get_constant_tags(),
        )
        dogstatsd_allow_list = conf.get("metrics", "statsd_allow_list", fallback=None)
        allow_list_validator = AllowListValidator(dogstatsd_allow_list)
        datadog_metrics_tags = conf.get("metrics", "statsd_datadog_metrics_tags", fallback=True)
        aggregation_optimizer_enabled = conf.get(
            "metrics", "statsd_aggregation_optimized_naming_enabled", fallback=False
        )
        return SafeDogStatsdLogger(
            dogstatsd, allow_list_validator, datadog_metrics_tags, aggregation_optimizer_enabled
        )

    @classmethod
    def get_constant_tags(cls):
        """Get constant DataDog tags to add to all stats."""
        tags = []
        tags_in_string = conf.get("metrics", "statsd_datadog_tags", fallback=None)
        if tags_in_string is None or tags_in_string == "":
            return tags
        else:
            for key_value in tags_in_string.split(","):
                tags.append(key_value)
            return tags


if TYPE_CHECKING:
    Stats: StatsLogger
else:

    class Stats(metaclass=_Stats):
        """Empty class for Stats - we use metaclass to inject the right one."""
