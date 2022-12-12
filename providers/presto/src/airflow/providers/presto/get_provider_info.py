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

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE
# OVERWRITTEN WHEN PREPARING PACKAGES.
#
# IF YOU WANT TO MODIFY IT, YOU SHOULD MODIFY THE TEMPLATE
# `get_provider_info_TEMPLATE.py.jinja2` IN the `provider_packages` DIRECTORY

def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-presto",
        "name": "Presto",
        "description": "`Presto <https://prestodb.github.io/>`__\n",
        "versions": [
            "4.2.0",
            "4.1.0",
            "4.0.1",
            "4.0.0",
            "3.1.0",
            "3.0.0",
            "2.2.1",
            "2.2.0",
            "2.1.2",
            "2.1.1",
            "2.1.0",
            "2.0.1",
            "2.0.0",
            "1.0.2",
            "1.0.1",
            "1.0.0",
        ],
        "dependencies": [
            "apache-airflow>=2.3.0",
            "apache-airflow-providers-common-sql>=1.3.1",
            "presto-python-client>=0.8.2",
            "pandas>=0.17.1",
        ],
        "integrations": [
            {
                "integration-name": "Presto",
                "external-doc-url": "http://prestodb.github.io/",
                "logo": "/integration-logos/presto/PrestoDB.png",
                "tags": ["software"],
            }
        ],
        "hooks": [
            {"integration-name": "Presto", "python-modules": ["airflow.providers.presto.hooks.presto"]}
        ],
        "transfers": [
            {
                "source-integration-name": "Google Cloud Storage (GCS)",
                "target-integration-name": "Presto",
                "how-to-guide": "/docs/apache-airflow-providers-presto/operators/transfer/gcs_to_presto.rst",
                "python-module": "airflow.providers.presto.transfers.gcs_to_presto",
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.presto.hooks.presto.PrestoHook",
                "connection-type": "presto",
            }
        ],
    }
