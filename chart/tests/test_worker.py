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

import unittest

import jmespath
from parameterized import parameterized

from tests.helm_template_generator import render_chart


class WorkerTest(unittest.TestCase):
    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "extraVolumes": [{"name": "test-volume", "emptyDir": {}}],
                    "extraVolumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "test-volume" == jmespath.search("spec.template.spec.volumes[0].name", docs[0])
        assert "test-volume" == jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[0].name", docs[0]
        )

    @parameterized.expand(
        [
            ('CeleryExecutor',),
            ('CeleryKubernetesExecutor',),
        ]
    )
    def test_worker_must_use_celery_executor(self, executor):
        """
        When cluster is using CeleryKubernetesExecutor, the workers must still use CeleryExecutor.
        To accomplish this we inject environment variable.
        """
        docs = render_chart(
            values={
                "executor": executor,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        query = "spec.template.spec.containers[0].env[?name=='AIRFLOW__CORE__EXECUTOR'].value"
        assert jmespath.search(query, docs[0]) == ['CeleryExecutor']
