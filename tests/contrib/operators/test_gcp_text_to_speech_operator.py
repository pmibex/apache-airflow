# -*- coding: utf-8 -*-
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

import unittest

from parameterized import parameterized

from airflow import AirflowException
from airflow.contrib.operators.gcp_text_to_speech_operator import GcpTextToSpeechSynthesizeOperator


try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = "project-id"
GCP_CONN_ID = "gcp-conn-id"
INPUT = {"text": "text"}
VOICE = {"language_code": "en-US"}
AUDIO_CONFIG = {"audio_encoding": "MP3"}
TARGET_BUCKET_NAME = "target_bucket_name"
TARGET_FILENAME = "target_filename"


class CloudSqlTest(unittest.TestCase):
    @mock.patch("airflow.contrib.operators.gcp_text_to_speech_operator.GoogleCloudStorageHook")
    @mock.patch("airflow.contrib.operators.gcp_text_to_speech_operator.GCPTextToSpeechHook")
    def test_synthesize_text_green_path(self, mock_text_to_speech_hook, mock_gcp_hook):
        mocked_response = mock.Mock()
        type(mocked_response).audio_content = mock.PropertyMock(return_value=b"audio")

        mock_text_to_speech_hook.return_value.synthesize_speech.return_value = mocked_response
        mock_gcp_hook.return_value.upload.return_value = True

        GcpTextToSpeechSynthesizeOperator(
            project_id=PROJECT_ID,
            gcp_conn_id=GCP_CONN_ID,
            input_=INPUT,
            voice=VOICE,
            audio_config=AUDIO_CONFIG,
            target_bucket_name=TARGET_BUCKET_NAME,
            target_filename=TARGET_FILENAME,
            task_id="id",
        ).execute(context={"task_instance": mock.Mock()})

        mock_text_to_speech_hook.assert_called_once_with(gcp_conn_id="gcp-conn-id")
        mock_gcp_hook.assert_called_once_with(google_cloud_storage_conn_id="gcp-conn-id")
        mock_text_to_speech_hook.return_value.synthesize_speech.assert_called_once_with(
            input_=INPUT, voice=VOICE, audio_config=AUDIO_CONFIG
        )
        mock_gcp_hook.return_value.upload.assert_called_once()

    @parameterized.expand(
        [
            ("input_", "", VOICE, AUDIO_CONFIG, TARGET_BUCKET_NAME, TARGET_FILENAME),
            ("voice", INPUT, "", AUDIO_CONFIG, TARGET_BUCKET_NAME, TARGET_FILENAME),
            ("audio_config", INPUT, VOICE, "", TARGET_BUCKET_NAME, TARGET_FILENAME),
            ("target_bucket_name", INPUT, VOICE, AUDIO_CONFIG, "", TARGET_FILENAME),
            ("target_filename", INPUT, VOICE, AUDIO_CONFIG, TARGET_BUCKET_NAME, ""),
        ]
    )
    @mock.patch("airflow.contrib.operators.gcp_text_to_speech_operator.GoogleCloudStorageHook")
    @mock.patch("airflow.contrib.operators.gcp_text_to_speech_operator.GCPTextToSpeechHook")
    def test_missing_arguments(
        self,
        missing_arg,
        input_,
        voice,
        audio_config,
        target_bucket_name,
        target_filename,
        mock_text_to_speech_hook,
        mock_gcp_hook,
    ):
        with self.assertRaises(AirflowException) as e:
            GcpTextToSpeechSynthesizeOperator(
                project_id="project-id",
                input_=input_,
                voice=voice,
                audio_config=audio_config,
                target_bucket_name=target_bucket_name,
                target_filename=target_filename,
                task_id="id",
            ).execute(context={"task_instance": mock.Mock()})

        err = e.exception
        self.assertIn(missing_arg, str(err))
        mock_text_to_speech_hook.assert_not_called()
        mock_gcp_hook.assert_not_called()
