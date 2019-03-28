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

from airflow.contrib.kubernetes.kubernetes_request_factory.\
    kubernetes_request_factory import KubernetesRequestFactory
from airflow.contrib.kubernetes.pod import Pod, Resources
from airflow.contrib.kubernetes.secret import Secret
from parameterized import parameterized
import unittest


class TestKubernetesRequestFactory(unittest.TestCase):

    def setUp(self):
        self.kubernetes_request_factory = KubernetesRequestFactory()

        self.req = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': 'name'
            },
            'spec': {
                'restartPolicy': 'Never',
                'containers': [{
                    'name': 'base',
                    'image': 'airflow-worker:latest',
                    'command': [
                        "/usr/local/airflow/entrypoint.sh",
                        "/bin/bash sleep 25"
                    ],
                }]
            }
        }

    def test_extract_image(self):
        input_req = self.req.copy()
        expected = self.req
        image = 'v3.14'
        pod = Pod(image, {}, [])
        self.kubernetes_request_factory.extract_image(pod, input_req)
        expected['spec']['containers'][0]['image'] = image
        self.assertDictEqual(input_req, expected)

    def test_extract_image_pull_policy(self):
        # Test when pull policy is not none
        pull_policy = 'IfNotPresent'
        pod = Pod('v3.14', {}, [], image_pull_policy=pull_policy)

        input_req = self.req.copy()
        expected = self.req
        self.kubernetes_request_factory.extract_image_pull_policy(pod, input_req)
        expected['spec']['containers'][0]['imagePullPolicy'] = pull_policy
        self.assertDictEqual(input_req, expected)

    def test_add_secret_to_env(self):
        secret = Secret('env', 'target', 'my-secret', 'KEY')
        secret_list = []
        expected = [{
            'name': 'TARGET',
            'valueFrom': {
                'secretKeyRef': {
                    'name': 'my-secret',
                    'key': 'KEY'
                }
            }
        }]
        self.kubernetes_request_factory.add_secret_to_env(secret_list, secret)
        self.assertListEqual(secret_list, expected)

    def test_extract_labels(self):
        # Test when labels are not empty
        labels = {'label_a': 'val_a', 'label_b': 'val_b'}
        pod = Pod('v3.14', {}, [], labels=labels)
        input_req = self.req.copy()
        expected = self.req
        expected['metadata']['labels'] = labels
        self.kubernetes_request_factory.extract_labels(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_annotations(self):
        # Test when annotations are not empty
        annotations = {'annot_a': 'val_a', 'annot_b': 'val_b'}
        pod = Pod('v3.14', {}, [], annotations=annotations)
        input_req = self.req.copy()
        expected = self.req
        expected['metadata']['labels'] = annotations
        self.kubernetes_request_factory.extract_annotations(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_affinity(self):
        # Test when affinity is not empty
        affinity = {'podAffinity': 'requiredDuringSchedulingIgnoredDuringExecution'}
        pod = Pod('v3.14', {}, [], affinity=affinity)
        input_req = self.req.copy()
        expected = self.req
        expected['spec']['affinity'] = affinity
        self.kubernetes_request_factory.extract_affinity(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_node_selector(self):
        # Test when affinity is not empty
        node_selectors = {'disktype': 'ssd', 'accelerator': 'nvidia-tesla-p100'}
        pod = Pod('v3.14', {}, [], node_selectors=node_selectors)
        input_req = self.req.copy()
        expected = self.req
        expected['spec']['nodeSelector'] = node_selectors
        self.kubernetes_request_factory.extract_node_selector(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_cmds(self):
        input_req = self.req.copy()
        expected = self.req.copy()
        cmds = ['test-cmd.sh']
        pod = Pod('v3.14', {}, cmds)
        self.kubernetes_request_factory.extract_cmds(pod, input_req)
        expected['spec']['containers'][0]['command'] = cmds
        self.assertDictEqual(input_req, expected)

    def test_extract_args(self):
        input_req = self.req.copy()
        expected = self.req
        args = ['test_arg.sh']
        pod = Pod('v3.14', {}, [], args=args)
        self.kubernetes_request_factory.extract_args(pod, input_req)
        expected['spec']['containers'][0]['args'] = args
        self.assertDictEqual(input_req, expected)

    def test_attach_volumes(self):
        # Test when volumes is not empty list
        volumes = ['vol_a', 'vol_b']
        pod = Pod('v3.14', {}, [], volumes=volumes)
        input_req = self.req.copy()
        expected = self.req
        expected['spec']['volumes'] = volumes
        self.kubernetes_request_factory.attach_volumes(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_attach_volume_mounts(self):
        # Test when volumes is not empty list
        volume_mounts = ['vol_a', 'vol_b']
        pod = Pod('v3.14', {}, [], volume_mounts=volume_mounts)
        input_req = self.req.copy()
        expected = self.req
        expected['spec']['containers'][0]['volumeMounts'] = volume_mounts
        self.kubernetes_request_factory.attach_volume_mounts(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_name(self):
        input_req = self.req.copy()
        expected = self.req
        name = 'pod-name'
        pod = Pod('v3.14', {}, [], name=name)
        expected['metadata']['name'] = name
        self.kubernetes_request_factory.extract_name(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_volume_secrets(self):
        # Test when secrets is not empty
        secrets = [
            Secret('volume', 'KEY1', 's1', 'key-1'),
            Secret('env', 'KEY2', 's2'),
            Secret('volume', 'KEY3', 's3', 'key-2')
        ]
        pod = Pod('v3.14', {}, [], secrets=secrets)
        input_req = self.req.copy()
        expected = self.req
        expected['spec']['containers'][0]['volumeMounts'] = [{
            'mountPath': 'KEY1',
            'name': 'secretvol1',
            'readOnly': True
        }, {
            'mountPath': 'KEY3',
            'name': 'secretvol2',
            'readOnly': True
        }]
        expected['spec']['volumes'] = [{
            'name': 'secretvol1',
            'secret': {
                'secretName': 's1'
            }
        }, {
            'name': 'secretvol2',
            'secret': {
                'secretName': 's3'
            }
        }]
        self.kubernetes_request_factory.extract_volume_secrets(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_env_and_secrets(self):
        # Test when secrets and envs are not empty
        secrets = [
            Secret('env', None, 's1'),
            Secret('volume', 'KEY2', 's2', 'key-2'),
            Secret('env', None, 's3')
        ]
        envs = {
            'ENV1': 'val1',
            'ENV2': 'val2'
        }
        configmaps = ['configmap_a', 'configmap_b']
        pod = Pod('v3.14', envs, [], secrets=secrets, configmaps=configmaps)
        input_req = self.req.copy()
        expected = self.req
        expected['spec']['containers'][0]['env'] = envs
        expected['spec']['containers'][0]['envFrom'] = [{
            'secretRef': {
                'secretName': 's1'
            }
        }, {
            'secretRef': {
                'secretName': 's3'
            }
        }, {
            'configMapRef': {
                'name': 'configmap_a'
            }
        }, {
            'configMapRef': {
                'name': 'configmap_a'
            }
        }]
        self.kubernetes_request_factory.extract_volume_secrets(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_resources(self):
        # Test when resources is not empty
        resources = Resources('1Gi', 1, '2Gi', 2)
        pod = Pod('v3.14', {}, [], resources=resources)
        input_req = self.req.copy()
        expected = self.req
        expected['spec']['containers'][0]['resources'] = resources
        self.kubernetes_request_factory.extract_volume_secrets(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_init_containers(self):
        input_req = self.req.copy()
        expected = self.req
        init_container = 'init_container'
        pod = Pod('v3.14', {}, [], init_containers=init_container)
        expected['spec']['initContainers'] = init_container
        self.kubernetes_request_factory.extract_resources(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_service_account_name(self):
        input_req = self.req.copy()
        expected = self.req
        service_account_name = 'service_account_name'
        pod = Pod('v3.14', {}, [], service_account_name=service_account_name)
        expected['spec']['serviceAccountName'] = service_account_name
        self.kubernetes_request_factory.extract_resources(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_hostnetwork(self):
        input_req = self.req.copy()
        expected = self.req
        hostnetwork = True
        pod = Pod('v3.14', {}, [], hostnetwork=hostnetwork)
        expected['spec']['serviceAccountName'] = hostnetwork
        self.kubernetes_request_factory.extract_resources(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_image_pull_secrets(self):
        input_req = self.req.copy()
        expected = self.req
        image_pull_secrets = 'secret_a,secret_b,secret_c'
        pod = Pod('v3.14', {}, [], image_pull_secrets=image_pull_secrets)
        expected['spec']['imagePullSecrets'] = [
            {'name': 'secret_a'},
            {'name': 'secret_b'},
            {'name': 'secret_c'},
        ]
        self.kubernetes_request_factory.extract_resources(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_extract_tolerations(self):
        input_req = self.req.copy()
        expected = self.req
        tolerations = [{
            'key': 'key',
            'operator': 'Equal',
            'value': 'value',
            'effect': 'NoSchedule'
        }]
        pod = Pod('v3.14', {}, [], tolerations=tolerations)
        expected['spec']['tolerations'] = tolerations
        self.kubernetes_request_factory.extract_resources(pod, input_req)
        self.assertDictEqual(input_req, expected)

    def test_security_context(self):
        input_req = self.req.copy()
        expected = self.req
        security_context = {
            'runAsUser': 1000,
            'fsGroup': 2000
        }
        pod = Pod('v3.14', {}, [], security_context=security_context)
        expected['spec']['securityContext'] = security_context
        self.kubernetes_request_factory.extract_resources(pod, input_req)
        self.assertDictEqual(input_req, expected)

    @parameterized.expand([
        'extract_image_pull_policy',
        'extract_labels',
        'extract_annotations',
        'extract_affinity',
        'extract_node_selector',
        'attach_volumes',
        'attach_volume_mounts',
        'extract_volume_secrets',
        'extract_env_and_secrets',
        'extract_resources'
    ])
    def test_identity(self, name):
        input_req = self.req.copy()
        expected = self.req
        pod = Pod('v3.14', {}, [])
        kubernetes_request_factory_method = getattr(
            self.kubernetes_request_factory, name
        )
        kubernetes_request_factory_method(pod, input_req)
        self.assertDictEqual(input_req, expected)
