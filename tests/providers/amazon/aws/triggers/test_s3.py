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

import pytest

from airflow.providers.amazon.aws.triggers.s3 import (
    S3KeyTrigger,
)
from airflow.triggers.base import TriggerEvent
from tests.providers.amazon.aws.compat import async_mock


class TestS3KeyTrigger:
    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = S3KeyTrigger(
            bucket_key="s3://test_bucket/file", bucket_name="test_bucket", wildcard_match=True
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger"
        assert kwargs == {
            "bucket_name": "test_bucket",
            "bucket_key": "s3://test_bucket/file",
            "wildcard_match": True,
            "aws_conn_id": "aws_default",
            "hook_params": {},
            "check_fn": None,
            "poke_interval": 5.0,
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3AsyncHook.get_client_async")
    async def test_run_success(self, mock_client):
        """
        Test if the task is run is in triggerr successfully.
        """
        mock_client.return_value.check_key.return_value = True
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3AsyncHook.check_key")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3AsyncHook.get_client_async")
    async def test_run_pending(self, mock_client, mock_check_key):
        """
        Test if the task is run is in trigger successfully and set check_key to return false.
        """
        mock_check_key.return_value = False
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3AsyncHook.get_client_async")
    async def test_run_exception(self, mock_client):
        """Test if the task is run is in case of exception."""
        mock_client.side_effect = Exception("Unable to locate credentials")
        trigger = S3KeyTrigger(bucket_key="file", bucket_name="test_bucket")
        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent(
                {
                    "message": "Unable to locate credentials",
                    "status": "error",
                }
            )
            == actual
        )
