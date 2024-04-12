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

import pytest
from unittest import mock

from airflow import DAG
from airflow.decorators import task_group
from airflow.models.taskinstance import TaskInstance as TI
from airflow.operators.empty import EmptyOperator
from airflow.providers.openlineage.conf import custom_facet_functions
from airflow.providers.openlineage.plugins.facets import AirflowMappedTaskRunFacet
from airflow.providers.openlineage.utils.utils import get_custom_facets
from airflow.utils import timezone
from airflow.utils.state import State
from tests.test_utils.config import conf_vars

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
SAMPLE_TI = TI(
    task=EmptyOperator(task_id="test-task", dag=DAG("test-dag"), start_date=DEFAULT_DATE),
    state=State.RUNNING.value,
)


@pytest.fixture(autouse=True)
def clear_cache():
    custom_facet_functions.cache_clear()
    try:
        yield
    finally:
        custom_facet_functions.cache_clear()


@pytest.mark.db_test
def test_get_custom_facets(dag_maker):
    with dag_maker(dag_id="dag_test_get_custom_facets") as dag:

        @task_group
        def task_group_op(k):
            EmptyOperator(task_id="empty_operator")

        task_group_op.expand(k=[0])

        dag_maker.create_dagrun()
        ti_0 = TI(dag.get_task("task_group_op.empty_operator"), execution_date=DEFAULT_DATE, map_index=0)

        assert ti_0.map_index == 0

        assert get_custom_facets(ti_0)["airflow_mappedTask"] == AirflowMappedTaskRunFacet(
            mapIndex=0,
            operatorClass=f"{ti_0.task.operator_class.__module__}.{ti_0.task.operator_class.__name__}",
        )


@mock.patch.dict("os.environ", {})
def test_get_custom_facets_with_no_function_definition():
    result = get_custom_facets(SAMPLE_TI)
    assert result == {}


@conf_vars(
    {
        (
            "openlineage",
            "custom_facet_functions",
        ): "tests.providers.openlineage.utils.custom_facet_fixture.get_additional_test_facet"
    }
)
def test_get_custom_facets_with_function_definition():
    result = get_custom_facets(SAMPLE_TI)
    assert result == {
        "additional_run_facet": {
            "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.7.0",
            "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
            "name": "test-lineage-namespace",
            "jobState": "running",
            "uniqueName": "TEST.test-dag.test-task",
            "displayName": "test-dag.test-task",
            "dagId": "test-dag",
            "taskId": "test-task",
            "cluster": "TEST",
        }
    }


@conf_vars(
    {
        (
            "openlineage",
            "custom_facet_functions",
        ): "tests.providers.openlineage.utils.custom_facet_fixture.get_additional_test_facet; ;"
        "invalid_function; tests.providers.openlineage.utils.custom_facet_fixture.return_type_is_not_dict;"
        " tests.providers.openlineage.utils.custom_facet_fixture.get_another_test_facet "
    },
)
def test_get_custom_facets_with_multiple_function_definition():
    result = get_custom_facets(SAMPLE_TI)
    assert result == {
        "additional_run_facet": {
            "_producer": "https://github.com/apache/airflow/tree/providers-openlineage/1.7.0",
            "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
            "name": "test-lineage-namespace",
            "jobState": "running",
            "uniqueName": "TEST.test-dag.test-task",
            "displayName": "test-dag.test-task",
            "dagId": "test-dag",
            "taskId": "test-task",
            "cluster": "TEST",
        },
        "another_run_facet": {"name": "another-lineage-namespace"},
    }


@conf_vars(
    {("openlineage", "custom_facet_functions"): "invalid_function"},
)
def test_get_custom_facets_with_invalid_function_definition():
    result = get_custom_facets(SAMPLE_TI)
    assert result == {}


@conf_vars(
    {
        (
            "openlineage",
            "custom_facet_functions",
        ): "tests.providers.openlineage.utils.custom_facet_fixture.return_type_is_not_dict"
    },
)
def test_get_custom_facets_with_wrong_return_type_function():
    result = get_custom_facets(SAMPLE_TI)
    assert result == {}
