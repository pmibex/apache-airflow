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
#
"""This module contains a Google Cloud Looker hook."""

import json
import time
from enum import Enum
from typing import Dict, Optional

# TODO: Temporary import for Looker API response stub, remove once looker sdk 22.2 released
import attr
from looker_sdk.rtl import api_settings, auth_session, requests_transport, serialize
# TODO: Temporary import for Looker API response stub, remove once looker sdk 22.2 released
from looker_sdk.rtl.model import Model
from looker_sdk.sdk.api40 import methods as methods40
from packaging.version import parse as parse_version

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.version import version

# TODO: uncomment once looker sdk 22.2 released
# from looker_sdk.sdk.api40.models import MaterializePDT


@attr.s(auto_attribs=True, init=False)
class MaterializePDT(Model):
    """
    TODO: Temporary API response stub. Use Looker API class once looker sdk 22.2 released.
    Attributes:
    materialization_id: The ID of the enqueued materialization job, if any
    resp_text: The string response
    """

    materialization_id: str
    resp_text: Optional[str] = None

    def __init__(self, *, materialization_id: str, resp_text: Optional[str] = None):
        self.materialization_id = materialization_id
        self.resp_text = resp_text


class LookerHook(BaseHook):
    """Hook for Looker APIs."""

    def __init__(
        self,
        looker_conn_id: str,
    ) -> None:
        super().__init__()
        self.looker_conn_id = looker_conn_id
        self.source = self.get_api_source()

    def start_pdt_build(
        self,
        model: str,
        view: str,
        query_params: Optional[Dict] = None,
    ) -> MaterializePDT:
        """
        Submits a PDT materialization job to Looker.

        :param model: Required. The model of the PDT to start building.
        :param view: Required. The view of the PDT to start building.
        :param query_params: Optional. Additional materialization parameters.
        """
        self.log.info("Submitting PDT materialization job. Model: '%s', view: '%s'.", model, view)
        self.log.debug("PDT materialization job source: '%s'.", self.source)

        sdk = self.get_looker_sdk()
        looker_ver = sdk.versions().looker_release_version
        if parse_version(looker_ver) < parse_version("22.2.0"):
            raise AirflowException(f'This API requires Looker version 22.2+. Found: {looker_ver}.')

        # TODO: Temporary API response stub. Use Looker API once looker sdk 22.2 released.
        resp = MaterializePDT(materialization_id='366', resp_text=None)
        # unpack query_params dict into kwargs (if not None)
        # if query_params:
        #     resp = sdk.start_pdt_build(model_name=model, view_name=view, source=self.source, **query_params)
        # else:
        #     resp = sdk.start_pdt_build(model_name=model, view_name=view, source=self.source)

        self.log.info("Start PDT build response: '%s'.", resp)

        return resp

    def check_pdt_build(
        self,
        materialization_id: str,
    ) -> MaterializePDT:
        """
        Gets the PDT materialization job status from Looker.

        :param materialization_id: Required. The materialization id to check status for.
        """
        self.log.info("Requesting PDT materialization job status. Job id: %s.", materialization_id)

        # TODO: Temporary API response stub. Use Looker API once looker sdk 22.2 released.
        resp = MaterializePDT(
            materialization_id='366',
            resp_text='{"status":"running","runtime":null,'
            '"message":"Materialize 100m_sum_aggregate_sdt, ",'
            '"task_slug":"f522424e00f0039a8c8f2d53d773f310"}',
        )
        sdk = self.get_looker_sdk()  # NOQA
        # resp = sdk.check_pdt_build(materialization_id=materialization_id)

        self.log.info("Check PDT build response: '%s'.", resp)
        return resp

    def pdt_build_status(
        self,
        materialization_id: str,
    ) -> Dict:
        """
        Gets the PDT materialization job status.

        :param materialization_id: Required. The materialization id to check status for.
        """
        resp = self.check_pdt_build(materialization_id=materialization_id)

        status_json = resp['resp_text']
        status_dict = json.loads(status_json)

        self.log.info(
            "PDT materialization job id: %s. Status: '%s'.", materialization_id, status_dict['status']
        )

        return status_dict

    def stop_pdt_build(
        self,
        materialization_id: str,
    ) -> MaterializePDT:
        """
        Starts a PDT materialization job cancellation request.

        :param materialization_id: Required. The materialization id to stop.
        """
        self.log.info("Stopping PDT materialization. Job id: %s.", materialization_id)
        self.log.debug("PDT materialization job source: '%s'.", self.source)

        # TODO: Temporary API response stub. Use Looker API once looker sdk 22.2 released.
        resp = MaterializePDT(
            materialization_id='366',
            resp_text='{"success":true,"connection":"incremental_pdts_test",'
            '"statusment":"KILL 810852",'
            '"task_slug":"6bad8184f94407134251be8fd18af834"}',
        )
        sdk = self.get_looker_sdk()  # NOQA
        # resp = sdk.stop_pdt_build(materialization_id=materialization_id, source=self.source)

        self.log.info("Stop PDT build response: '%s'.", resp)
        return resp

    def wait_for_job(
        self,
        materialization_id: str,
        wait_time: int = 10,
        timeout: Optional[int] = None,
    ) -> None:
        """
        Helper method which polls a PDT materialization job to check if it finishes.

        :param materialization_id: Required. The materialization id to wait for.
        :param wait_time: Optional. Number of seconds between checks.
        :param timeout: Optional. How many seconds wait for job to be ready.
        Used only if ``asynchronous`` is False.
        """
        self.log.info('Waiting for PDT materialization job to complete. Job id: %s.', materialization_id)

        status = None
        start = time.monotonic()

        while status not in (
            JobStatus.DONE.value,
            JobStatus.ERROR.value,
            JobStatus.CANCELLED.value,
            JobStatus.UNKNOWN.value,
        ):

            if timeout and start + timeout < time.monotonic():
                raise AirflowException(
                    f"Timeout: PDT materialization job is not ready after {timeout}s. "
                    f"Job id: {materialization_id}."
                )

            time.sleep(wait_time)

            status_dict = self.pdt_build_status(materialization_id=materialization_id)
            status = status_dict['status']

        if status == JobStatus.ERROR.value:
            msg = status_dict['message']
            raise AirflowException(
                f'PDT materialization job failed. Job id: {materialization_id}. Message:\n"{msg}"'
            )
        if status == JobStatus.CANCELLED.value:
            raise AirflowException(f'PDT materialization job was cancelled. Job id: {materialization_id}.')
        if status == JobStatus.UNKNOWN.value:
            raise AirflowException(
                f'PDT materialization job has unknown status. Job id: {materialization_id}.'
            )

        self.log.info('PDT materialization job completed successfully. Job id: %s.', materialization_id)

    def get_api_source(self):
        """Returns origin of the API call."""

        # Infer API source based on Airflow version, e.g.:
        # "2.1.4+composer" -> Cloud Composer
        # "2.1.4" -> Airflow

        self.log.debug(
            "Got the following version for connection id: '%s'. Version: '%s'.", self.looker_conn_id, version
        )

        return 'composer' if 'composer' in version else 'airflow'

    def get_looker_sdk(self):
        """Returns Looker SDK client for Looker API 4.0."""

        conn = self.get_connection(self.looker_conn_id)
        settings = LookerApiSettings(conn)

        transport = requests_transport.RequestsTransport.configure(settings)
        return methods40.Looker40SDK(
            auth_session.AuthSession(settings, transport, serialize.deserialize40, "4.0"),
            serialize.deserialize40,
            serialize.serialize,
            transport,
            "4.0",
        )


class LookerApiSettings(api_settings.ApiSettings):
    """Custom implementation of Looker SDK's `ApiSettings` class."""

    def __init__(
        self,
        conn: Connection,
    ) -> None:
        self.conn = conn  # need to init before `read_config` is called in super
        super().__init__()

    def read_config(self) -> Dict[str, str]:
        """
        Overrides the default logic of getting connection settings. Fetches
        the connection settings from Airflow's connection object.
        """

        config = {}

        if self.conn.host is None:
            raise AirflowException(f'No `host` was supplied in connection: {self.conn.id}.')

        if self.conn.port:
            config["base_url"] = f"{self.conn.host}:{self.conn.port}"  # port is optional
        else:
            config["base_url"] = self.conn.host

        if self.conn.login:
            config["client_id"] = self.conn.login
        else:
            raise AirflowException(f'No `login` was supplied in connection: {self.conn.id}.')

        if self.conn.password:
            config["client_secret"] = self.conn.password
        else:
            raise AirflowException(f'No `password` was supplied in connection: {self.conn.id}.')

        extras = self.conn.extra_dejson  # type: Dict

        if 'verify_ssl' in extras:
            config["verify_ssl"] = extras["verify_ssl"]  # optional

        if 'timeout' in extras:
            config["timeout"] = extras["timeout"]  # optional

        return config


class JobStatus(Enum):
    """The job status string."""

    PENDING = 'new'
    RUNNING = 'running'
    CANCELLED = 'killed'
    DONE = 'complete'
    ERROR = 'error'
    UNKNOWN = 'unknown'
