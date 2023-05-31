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

from unittest.mock import MagicMock, patch

import pytest
from databricks.sql.types import Row
from openlineage.client.facet import SchemaDatasetFacet, SchemaField, SqlJobFacet
from openlineage.client.run import Dataset

from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DATE = "2017-04-20"
TASK_ID = "databricks-sql-operator"
DEFAULT_CONN_ID = "snowflake_default"


@pytest.mark.parametrize(
    "sql, return_last, split_statement, hook_results, hook_descriptions, expected_results",
    [
        pytest.param(
            "select * from dummy",
            True,
            True,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            ([{"id": 1, "value": "value1"}, {"id": 2, "value": "value2"}]),
            id="Scalar: Single SQL statement, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy;select * from dummy2",
            True,
            True,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            ([{"id": 1, "value": "value1"}, {"id": 2, "value": "value2"}]),
            id="Scalar: Multiple SQL statements, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy",
            False,
            False,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            ([{"id": 1, "value": "value1"}, {"id": 2, "value": "value2"}]),
            id="Scalar: Single SQL statements, no return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            "select * from dummy",
            True,
            False,
            [Row(id=1, value="value1"), Row(id=2, value="value2")],
            [[("id",), ("value",)]],
            ([{"id": 1, "value": "value1"}, {"id": 2, "value": "value2"}]),
            id="Scalar: Single SQL statements, return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy"],
            False,
            False,
            [[Row(id=1, value="value1"), Row(id=2, value="value2")]],
            [[("id",), ("value",)]],
            [([{"id": 1, "value": "value1"}, {"id": 2, "value": "value2"}])],
            id="Non-Scalar: Single SQL statements in list, no return_last, no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            False,
            False,
            [
                [Row(id=1, value="value1"), Row(id=2, value="value2")],
                [Row(id2=1, value2="value1"), Row(id2=2, value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                ([{"id": 1, "value": "value1"}, {"id": 2, "value": "value2"}]),
                ([{"id2": 1, "value2": "value1"}, {"id2": 2, "value2": "value2"}]),
            ],
            id="Non-Scalar: Multiple SQL statements in list, no return_last (no matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            True,
            False,
            [
                [Row(id=1, value="value1"), Row(id=2, value="value2")],
                [Row(id2=1, value2="value1"), Row(id2=2, value2="value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                ([{"id": 1, "value": "value1"}, {"id": 2, "value": "value2"}]),
                ([{"id2": 1, "value2": "value1"}, {"id2": 2, "value2": "value2"}]),
            ],
            id="Non-Scalar: Multiple SQL statements in list, return_last (no matter), no split statement",
        ),
    ],
)
def test_exec_success(sql, return_last, split_statement, hook_results, hook_descriptions, expected_results):
    """
    Test the execute function in case where SQL query was successful.
    """
    with patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook") as get_db_hook_mock:
        op = SnowflakeOperator(
            task_id=TASK_ID,
            sql=sql,
            do_xcom_push=True,
            return_last=return_last,
            split_statements=split_statement,
        )
        dbapi_hook = MagicMock()
        get_db_hook_mock.return_value = dbapi_hook
        dbapi_hook.run.return_value = hook_results
        dbapi_hook.descriptions = hook_descriptions

        execute_results = op.execute(None)

        assert execute_results == expected_results
        dbapi_hook.run.assert_called_once_with(
            sql=sql,
            parameters=None,
            handler=fetch_all_handler,
            autocommit=False,
            return_last=return_last,
            split_statements=split_statement,
        )


def test_execute_openlineage_events():
    DB_SCHEMA_NAME = "PUBLIC"

    class SnowflakeHookForTests(SnowflakeHook):
        get_conn = MagicMock(name="conn")
        get_connection = MagicMock()

        def get_first(self, *_):
            return [DB_SCHEMA_NAME]

    dbapi_hook = SnowflakeHookForTests()

    class SnowflakeOperatorForTest(SnowflakeOperator):
        def get_db_hook(self):
            return dbapi_hook

    sql = """CREATE TABLE IF NOT EXISTS popular_orders_day_of_week (
        order_day_of_week VARCHAR(64) NOT NULL,
        order_placed_on   TIMESTAMP NOT NULL,
        orders_placed     INTEGER NOT NULL
    );
FORGOT TO COMMENT"""
    op = SnowflakeOperatorForTest(task_id="snowflake-operator", sql=sql)
    rows = [
        (DB_SCHEMA_NAME, "POPULAR_ORDERS_DAY_OF_WEEK", "ORDER_DAY_OF_WEEK", 1, "TEXT"),
        (DB_SCHEMA_NAME, "POPULAR_ORDERS_DAY_OF_WEEK", "ORDER_PLACED_ON", 2, "TIMESTAMP_NTZ"),
        (DB_SCHEMA_NAME, "POPULAR_ORDERS_DAY_OF_WEEK", "ORDERS_PLACED", 3, "NUMBER"),
    ]
    dbapi_hook.get_connection.return_value = Connection(
        conn_id="snowflake_default",
        conn_type="snowflake",
        extra={"account": "test_account", "region": "us-east", "warehouse": "snow-warehouse"},
    )
    dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    lineage = op.get_openlineage_facets_on_start()
    assert len(lineage.inputs) == 0
    assert lineage.outputs == [
        Dataset(
            namespace="snowflake://test_account.us-east.aws",
            name="PUBLIC.POPULAR_ORDERS_DAY_OF_WEEK",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="ORDER_DAY_OF_WEEK", type="TEXT"),
                        SchemaField(name="ORDER_PLACED_ON", type="TIMESTAMP_NTZ"),
                        SchemaField(name="ORDERS_PLACED", type="NUMBER"),
                    ]
                )
            },
        )
    ]

    assert lineage.job_facets == {"sql": SqlJobFacet(query=sql)}

    assert lineage.run_facets["extractionError"].failedTasks == 1
