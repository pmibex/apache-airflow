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

import json
import os
from unittest import mock

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow.models import Connection
from airflow.providers.opentelemetry.hooks.otel import (
    OtelHook,
    is_listener_enabled,
    is_otel_metrics_enabled,
    is_otel_traces_enabled,
)


class TestOpenTelemetryHook:
    def test_hook_search(self):
        """test whether the hook operates with fallback if no conn id is found."""
        hook = OtelHook("test_conn_id")
        hook.gauge(stat="stat", value=1)
        hook.incr(stat="counter1")
        hook.decr(stat="counter1")
        with hook.start_as_current_span("my_span1") as s1:
            s1.set_attribute("attr1", "val1")
            with hook.start_as_current_span("my_span2") as s2:
                s2.set_attribute("attr2", "val2")
                pass
        span = hook.start_span("test_span")
        span.set_attribute("attr1", "val1")
        span.add_event("event1")
        assert True

    @mock.patch("airflow.providers.opentelemetry.hooks.otel.OtelHook.get_connection")
    def test_url_required(self, mock_get_connection):
        mock_get_connection.return_value = Connection()
        otel_hook = OtelHook()
        assert otel_hook.ready is False

    @mock.patch.dict(
        os.environ,
        {"OTEL_LISTENER_DISABLED": "false"},
    )
    @mock.patch("airflow.providers.opentelemetry.hooks.otel.conf")
    def test_traces_enabled(self, conf_a):
        conf_a.has_option.return_value = True
        conf_a.getboolean.return_value = True
        assert is_otel_traces_enabled() is True
        assert is_otel_metrics_enabled() is True
        assert is_listener_enabled() is False

    @mock.patch("airflow.providers.opentelemetry.hooks.otel.OTLPSpanExporter")
    @mock.patch("airflow.providers.opentelemetry.hooks.otel.conf")
    @mock.patch("airflow.providers.opentelemetry.hooks.otel.OtelHook.get_connection")
    def test_traces_start_span(self, conn_a, conf_a, span_exporter):
        conf_a.has_option.return_value = False
        conf_a.getboolean.return_value = False
        conn_a.password = "xyz"
        conn_a.host = "https://host:port"
        conn_a.login = "header"
        conn_a.port = 1000
        in_mem_exporter = InMemorySpanExporter()
        span_exporter.return_value = in_mem_exporter
        otel_hook = OtelHook()
        assert otel_hook.ready is True
        with otel_hook.start_as_current_span(name="span1") as s1:
            s1.set_attribute("attr1", "val1")
            with otel_hook.start_as_current_span(name="span2") as s2:
                s2.set_attribute("attr2", "val2")
                span2 = json.loads(s2.to_json())
            span1 = json.loads(s1.to_json())
        assert span1["name"] == "span1"
        assert span2["name"] == "span2"
        trace_id = span1["context"]["trace_id"]
        s1_span_id = span1["context"]["span_id"]
        assert span2["context"]["trace_id"] == trace_id
        assert span2["parent_id"] == s1_span_id
        assert span2["attributes"]["attr2"] == "val2"
        assert span1["attributes"]["attr1"] == "val1"
        assert len(in_mem_exporter.get_finished_spans()) == 2
