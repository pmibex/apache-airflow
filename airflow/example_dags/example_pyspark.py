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

import typing

import pendulum

if typing.TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_pyspark():
    """
    ### Example Pyspark DAG
    This is an example DAG which uses the Pyspark operator to submit a job.
    """

    # [START task_pyspark]
    @task.pyspark(conn_id="spark-local")
    def spark_task(spark: SparkSession | None) -> pd.DataFrame:
        if spark is None:
            raise ValueError("Spark session is None")

        df = spark.createDataFrame(
            [
                (1, "John Doe", 21),
                (2, "Jane Doe", 22),
                (3, "Joe Bloggs", 23),
            ],
            ["id", "name", "age"],
        )
        df.show()

        return df.toPandas()

    # [END task_pyspark]

    @task
    def print_df(df: pd.DataFrame):
        print(df)

    df = spark_task(spark=None)
    print_df(df)


example_pyspark()
