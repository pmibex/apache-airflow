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

import asyncio
from typing import Any, AsyncIterator

from botocore.exceptions import ClientError, WaiterError

from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.ecs import EcsHook, EcsTaskLogFetcher
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ClusterWaiterTrigger(BaseTrigger):
    """
    Polls the status of a cluster using a given waiter. Can be used to poll for an active or inactive cluster.

    :param waiter_name: Name of the waiter to use, for instance 'cluster_active' or 'cluster_inactive'
    :param cluster_arn: ARN of the cluster to watch.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The number of times to ping for status.
        Will fail after that many unsuccessful attempts.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region: The AWS region where the cluster is located.
    """

    def __init__(
        self,
        waiter_name: str,
        cluster_arn: str,
        waiter_delay: int | None,
        waiter_max_attempts: int | None,
        aws_conn_id: str | None,
        region: str | None,
    ):
        self.cluster_arn = cluster_arn
        self.waiter_name = waiter_name
        self.waiter_delay = waiter_delay if waiter_delay is not None else 15  # written like this to allow 0
        self.attempts = waiter_max_attempts or 999999999
        self.aws_conn_id = aws_conn_id
        self.region = region

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "waiter_name": self.waiter_name,
                "cluster_arn": self.cluster_arn,
                "waiter_delay": self.waiter_delay,
                "waiter_max_attempts": self.attempts,
                "aws_conn_id": self.aws_conn_id,
                "region": self.region,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async with EcsHook(aws_conn_id=self.aws_conn_id, region_name=self.region).async_conn as client:
            waiter = client.get_waiter(self.waiter_name)
            while self.attempts >= 1:
                self.attempts = self.attempts - 1
                try:
                    await waiter.wait(
                        clusters=[self.cluster_arn],
                        WaiterConfig={
                            "MaxAttempts": 1,
                        },
                    )
                    # we reach this point only if the waiter met a success criteria
                    yield TriggerEvent({"status": "success", "arn": self.cluster_arn})
                    return
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise
                    self.log.info("Status of cluster is %s", error.last_response["clusters"][0]["status"])
                    await asyncio.sleep(int(self.waiter_delay))

        raise AirflowException(
            "Cluster still not in expected status after the max number of tries has been reached"
        )


class TaskDoneTrigger(BaseTrigger):
    """
    Waits for an ECS task to be done, while eventually polling logs.

    :param cluster: short name or full ARN of the cluster where the task is running.
    :param task_arn: ARN of the task to watch.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The number of times to ping for status.
        Will fail after that many unsuccessful attempts.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region: The AWS region where the cluster is located.
    """

    def __init__(
        self,
        cluster: str,
        task_arn: str,
        waiter_delay: int | None,
        aws_conn_id: str | None,
        region: str | None,
        log_group: str | None = None,
        log_stream: str | None = None,
    ):
        self.cluster = cluster
        self.task_arn = task_arn

        self.waiter_delay = waiter_delay or 15
        self.aws_conn_id = aws_conn_id
        self.region = region

        self.log_group = log_group
        self.log_stream = log_stream

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "cluster": self.cluster,
                "task_arn": self.task_arn,
                "waiter_delay": self.waiter_delay,
                "aws_conn_id": self.aws_conn_id,
                "region": self.region,
                "log_group": self.log_group,
                "log_stream": self.log_stream,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        # fmt: off
        async with EcsHook(aws_conn_id=self.aws_conn_id, region_name=self.region).async_conn as ecs_client,\
                AwsLogsHook(aws_conn_id=self.aws_conn_id, region_name=self.region).async_conn as logs_client:
            # fmt: on
            waiter = ecs_client.get_waiter("tasks_stopped")
            logs_token = None
            while True:
                try:
                    await waiter.wait(
                        cluster=self.cluster, tasks=[self.task_arn], WaiterConfig={"MaxAttempts": 1}
                    )
                    break  # we reach this point only if the waiter met a success criteria
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise
                    self.log.info("Status of the task is %s", error.last_response["tasks"][0]["lastStatus"])
                    await asyncio.sleep(int(self.waiter_delay))
                finally:
                    if self.log_group and self.log_stream:
                        logs_token = await self._forward_logs(logs_client, logs_token)

        yield TriggerEvent({"status": "success", "task_arn": self.task_arn})

    async def _forward_logs(self, logs_client, next_token: str | None = None) -> str | None:
        """
        Reads logs from the cloudwatch stream and prints them to the task logs.
        :return: the token to pass to the next iteration to resume where we started
        """
        while True:
            if next_token is not None:
                token_arg: dict[str, str] = {"nextToken": next_token}
            else:
                token_arg = {}
            try:
                response = await logs_client.get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=self.log_stream,
                    startFromHead=True,
                    **token_arg,
                )
            except ClientError as ce:
                if ce.response["Error"]["Code"] == "ResourceNotFoundException":
                    self.log.info(
                        "Tried to get logs from stream %s in group %s but it didn't exist (yet). "
                        "Will try again.",
                        self.log_stream,
                        self.log_group,
                    )
                    return None
                raise

            events = response["events"]
            for log_event in events:
                self.log.info(EcsTaskLogFetcher._event_to_str(log_event))

            if len(events) == 0 or next_token == response["nextForwardToken"]:
                return response["nextForwardToken"]
            next_token = response["nextForwardToken"]
