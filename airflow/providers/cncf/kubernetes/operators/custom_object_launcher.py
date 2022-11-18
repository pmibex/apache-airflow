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
"""Launches Custom object"""
import time
from copy import deepcopy
from datetime import datetime as dt
from typing import Optional

import tenacity
from kubernetes import client
from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow.providers.cncf.kubernetes.resource_convert.secret import convert_secret, convert_image_pull_secrets
from airflow.providers.cncf.kubernetes.resource_convert.configmap import convert_configmap, convert_configmap_to_volume
from airflow.providers.cncf.kubernetes.resource_convert.env_variable import convert_env_vars

from airflow.compat.functools import cached_property


def should_retry_start_spark_job(exception: BaseException) -> bool:
    """Check if an Exception indicates a transient error and warrants retrying"""
    if isinstance(exception, ApiException):
        return exception.status == 409
    return False


class SparkJobSpec:
    def __init__(self, **entries):
        self.__dict__.update(entries)
        self.validate()
        self.update_resources()

    def validate(self):
        if self.spec['dynamicAllocation']['enabled']:
            if not all(
                [self.spec['dynamicAllocation']['initialExecutors'], self.spec['dynamicAllocation']['minExecutors'], self.spec['dynamicAllocation']['maxExecutors']]
            ):
                raise AirflowException("Make sure initial/min/max value for dynamic allocation is passed")

    def update_resources(self):
        spark_resources = SparkResources(self.spec['driver'].pop('container_resources'), self.spec['executor'].pop('container_resources'))
        self.spec['driver'].update(spark_resources.resources['driver'])
        self.spec['executor'].update(spark_resources.resources['executor'])


class KubernetesSpec:
    def __init__(self, **entries):
        self.__dict__.update(entries)
        self.set_attribute()

    def set_attribute(self):
        self.env_vars = convert_env_vars(self.env_vars) if self.env_vars else []
        self.image_pull_secrets = convert_image_pull_secrets(image_pull_secrets) if self.image_pull_secrets else []
        if self.config_map_mounts:
            vols, vols_mounts = convert_configmap_to_volume(self.config_map_mounts)
            self.volumes.extend(vols)
            self.volume_mounts.extend(vols_mounts)
        if self.from_env_config_map:
            self.env_from.extend([convert_configmap(c_name) for c_name in self.from_env_config_map])
        if self.from_env_secret:
            self.env_from.extend([convert_secret(c) for c in self.from_env_secret])


class SparkResources:
    """spark resources"""

    def __init__(
        self,
        driver: dict | None = None,
        executor: dict | None = None,
    ):
        self.default = {
            'gpu': {'name': None, 'quantity': 0},
            'cpu': {'request': None, 'limit': None},
            'memory': {'request': None, 'limit': None}
        }
        self.driver = deepcopy(self.default)
        self.executor = deepcopy(self.default)
        if driver:
            self.driver.update(driver)
        if executor:
            self.executor.update(executor)
        self.convert_resources()

    @property
    def resources(self):
        """Return job resources"""
        return {'driver': self.driver_resources, 'executor': self.executor_resources}

    @property
    def driver_resources(self):
        """Return resources to use"""
        driver = {}
        if self.driver['cpu'].get('request'):
            driver['cores'] = self.driver['cpu']['request']
        if self.driver['cpu'].get('limit'):
            driver['coreLimit'] = self.driver['cpu']['limit']
        if self.driver['memory'].get('limit'):
            driver['memory'] = self.driver['memory']['limit']
        if self.driver['gpu'].get('name') and self.driver['gpu'].get('quantity'):
            driver['gpu'] = {'name': self.driver['gpu']['name'], 'quantity': self.driver['gpu']['quantity']}
        return driver

    @property
    def executor_resources(self):
        """Return resources to use"""
        executor = {}
        if self.executor['cpu'].get('request'):
            executor['cores'] = self.executor['cpu']['request']
        if self.executor['cpu'].get('limit'):
            executor['coreLimit'] = self.executor['cpu']['limit']
        if self.executor['memory'].get('limit'):
            executor['memory'] = self.executor['memory']['limit']
        if self.executor['gpu'].get('name') and self.executor['gpu'].get('quantity'):
            executor['gpu'] = {'name': self.executor['gpu']['name'], 'quantity': self.executor['gpu']['quantity']}
        return executor

    def convert_resources(self):
        if isinstance(self.driver['memory'].get('limit'), str):
            if 'G' in self.driver['memory']['limit'] or 'Gi' in self.driver['memory']['limit']:
                self.driver['memory']['limit'] = float(self.driver['memory']['limit'].rstrip('Gi G')) * 1024
            elif 'm' in self.driver['memory']['limit']:
                self.driver['memory']['limit'] = float(self.driver['memory']['limit'].rstrip('m'))
            # Adjusting the memory value as operator adds 40% to the given value
            self.driver['memory']['limit'] = str(int(self.driver['memory']['limit'] / 1.4)) + 'm'

        if isinstance(self.executor['memory'].get('limit'), str):
            if 'G' in self.executor['memory']['limit'] or 'Gi' in self.executor['memory']['limit']:
                self.executor['memory']['limit'] = float(self.executor['memory']['limit'].rstrip('Gi G')) * 1024
            elif 'm' in self.executor['memory']['limit']:
                self.executor['memory']['limit'] = float(self.executor['memory']['limit'].rstrip('m'))
            # Adjusting the memory value as operator adds 40% to the given value
            self.executor['memory']['limit'] = str(int(self.executor['memory']['limit'] / 1.4)) + 'm'

        if self.driver['cpu'].get('request'):
            self.driver['cpu']['request'] = int(float(self.driver['cpu']['request']))
        if self.driver['cpu'].get('limit'):
            self.driver['cpu']['limit'] = str(self.driver['cpu']['limit'])
        if self.executor['cpu'].get('request'):
            self.executor['cpu']['request'] = int(float(self.executor['cpu']['request']))
        if self.executor['cpu'].get('limit'):
            self.executor['cpu']['limit'] = str(self.executor['cpu']['limit'])

        if self.driver['gpu'].get('quantity'):
            self.driver['gpu']['quantity'] = int(float(driver['gpu']['quantity']))
        if self.executor['gpu'].get('quantity'):
            self.executor['gpu']['quantity'] = int(float(self.executor['gpu']['quantity']))


class CustomObjectStatus:
    """Status of the PODs"""

    SUBMITTED = 'SUBMITTED'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    SUCCEEDED = 'SUCCEEDED'


class CustomObjectLauncher(LoggingMixin):
    """Launches PODS"""

    def __init__(
        self,
        name: str,
        namespace: str,
        kube_client: client.CoreV1Api,
        custom_obj_api: client.CustomObjectsApi,
        template_body: Optional[str] = None,
    ):
        """
        Creates custom object launcher(sparkapplications crd).
        :param kube_client: kubernetes client
        """
        super().__init__()
        self.name = name
        self.namespace = namespace
        self.template_body = template_body
        self.body: dict = self.get_body()
        self.api_group = self.body['apiGroup']
        self.api_version = self.body['version']
        self.plural = self.body['apiPlural']
        self.kind = self.body['kind']
        self._client = kube_client
        self.custom_obj_api = custom_obj_api
        self.spark_obj_spec: dict = {}
        self.pod_spec: Optional[k8s.V1Pod] = None

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(kube_client=self._client)

    def get_body(self):
        self.body: dict = SparkJobSpec(**self.template_body['spark'])
        self.body.metadata = {'name': self.name}
        k8s_spec: dict = KubernetesSpec(**self.template_body['kubernetes'])
        self.body.spec['volumes'] = k8s_spec.volumes
        for item in ['driver', 'executor']:
            # Env List
            self.body.spec[item]['env'] = k8s_spec.env_vars
            self.body.spec[item]['envFrom'] = k8s_spec.env_from
            # Volumes
            self.body.spec[item]['volumeMounts'] = k8s_spec.volume_mounts
            # Add affinity
            self.body.spec[item]['affinity'] = k8s_spec.affinity
            self.body.spec[item]['tolerations'] = k8s_spec.tolerations
            # Labels
            self.body.spec[item]['labels'] = self.body.spec['labels']
        return self.body.__dict__

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
        retry=tenacity.retry_if_exception(should_retry_start_spark_job),
    )
    def start_spark_job(self, image, code_path, startup_timeout: int = 600):
        """
        Launches the pod synchronously and waits for completion.

        :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
        :return:
        """
        try:
            self.body['spec']['image'] = image
            self.body['spec']['mainApplicationFile'] = code_path
            self.log.debug('Spark Job Creation Request Submitted')
            self.spark_obj_spec = self.custom_obj_api.create_namespaced_custom_object(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                plural=self.plural,
                body=self.body,
            )
            self.log.debug('Spark Job Creation Response: %s', self.spark_obj_spec)

            # Wait for the driver pod to come alive
            self.pod_spec = k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    labels=self.spark_obj_spec['spec']['driver']['labels'],
                    name=self.spark_obj_spec['metadata']['name'] + '-driver',
                    namespace=self.namespace
                )
            )
            curr_time = dt.now()
            while self.spark_job_not_running(self.spark_obj_spec):
                self.log.warning(
                    "Spark job submitted but not yet started. job_id: %s", self.spark_obj_spec['metadata']['name']
                )
                self.check_pod_start_failure()
                delta = dt.now() - curr_time
                if delta.total_seconds() >= startup_timeout:
                    pod_status = self.pod_manager.read_pod(self.pod_spec).status.container_statuses
                    raise AirflowException(f"Job took too long to start. pod status: {pod_status}")
                time.sleep(10)
        except Exception as e:
            self.log.exception('Exception when attempting to create spark job')
            raise e

        return self.pod_spec, self.spark_obj_spec

    def spark_job_not_running(self, spark_obj_spec):
        """Tests if spark_obj_spec has not started"""
        spark_job_info = self.custom_obj_api.get_namespaced_custom_object_status(
            group=self.api_group,
            version=self.api_version,
            namespace=self.namespace,
            name=spark_obj_spec['metadata']['name'],
            plural=self.plural,
        )
        driver_state = spark_job_info.get('status', {}).get('applicationState', {}).get('state', 'SUBMITTED')
        if driver_state == CustomObjectStatus.FAILED:
            err = spark_job_info.get('status', {}).get('applicationState', {}).get('errorMessage', 'N/A')
            try:
                self.pod_manager.fetch_container_logs(pod=self.pod_spec, container_name='spark-kubernetes-driver')
            except:
                pass
            raise AirflowException(f"Spark Job Failed.\nSparkJob Error stack:\n{err}")
        return driver_state == CustomObjectStatus.SUBMITTED

    def check_pod_start_failure(self):
        try:
            waiting_status = self.pod_manager.read_pod(self.pod_spec).status.container_statuses[0].state.waiting
            waiting_reason = waiting_status.reason
            waiting_message = waiting_status.message
        except Exception:
            return
        if waiting_reason != 'ContainerCreating':
            raise AirflowException(f"Spark Job Failed.\nStatus: {waiting_reason}\nError: {waiting_message}")

    def delete_spark_job(self, spark_job_name=None):
        """Deletes spark job"""
        spark_job_name = spark_job_name or self.spark_obj_spec.get('metadata', {}).get('name')
        if not spark_job_name:
            self.log.warning("Spark job not found: %s", spark_job_name)
            return
        try:
            v1 = client.CustomObjectsApi()
            v1.delete_namespaced_custom_object(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                plural=self.plural,
                name=spark_job_name,
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise
