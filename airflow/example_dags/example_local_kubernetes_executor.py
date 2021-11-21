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
"""
This is an example dag for using a Kubernetes Executor Configuration.
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.configuration import conf
from airflow.example_dags.libs.helper import print_stuff
from airflow.operators.python_operator import PythonOperator

log = logging.getLogger(__name__)

worker_container_repository = conf.get('kubernetes', 'worker_container_repository')
worker_container_tag = conf.get('kubernetes', 'worker_container_tag')

try:
    from kubernetes.client import models as k8s
except ImportError:
    log.warning("Could not import DAGs in example_kubernetes_executor.py", exc_info=True)
    log.warning("Install Kubernetes dependencies with: pip install apache-airflow[cncf.kubernetes]")

with DAG(
    dag_id='example_local_kubernetes_executor',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example3'],
) as dag:
    # You can use annotations on your kubernetes pods!
    start_task_executor_config = {
        "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))
    }

    task_with_template = PythonOperator(
        task_id="task_with_kubernetes_executor",
        python_callable=print_stuff,
        pool='kubernetes',
        queue='kubernetes',
        executor_config=start_task_executor_config,
    )

    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    task_with_local = PythonOperator(
        task_id="task_with_local_executor",
        python_callable=print_context,
    )

    task_with_local >> task_with_template
