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

import boto3
from moto import mock_s3

from airflow.models import DAG, TaskInstance
from airflow.providers.amazon.aws.transfers.s3_to_ftp import S3ToFTPOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars

TASK_ID = 'test_s3_to_ftp'
BUCKET = 'test-s3-bucket'
S3_KEY = 'test/test_1_file.csv'
FTP_PATH = '/tmp/remote_path.txt'
FTP_CONN_ID = 'ftp_default'
S3_CONN_ID = 'aws_default'
LOCAL_FILE_PATH = '/tmp/test_s3_upload'

FTP_MOCK_FILE = 'test_ftp_file.csv'
S3_MOCK_FILES = 'test_1_file.csv'

TEST_DAG_ID = 'unit_tests_s3_to_ftp'
DEFAULT_DATE = datetime(2018, 1, 1)


class TestS3ToSFTPOperator(unittest.TestCase):
    @mock_s3
    def setUp(self):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.ftp.hooks.ftp import FTPHook

        hook = FTPHook(ftp_conn_id='ftp_default')
        s3_hook = S3Hook('aws_default')
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'

        self.ftp_hook = hook
        self.s3_hook = s3_hook

        self.dag = dag
        self.s3_bucket = BUCKET
        self.ftp_path = FTP_PATH
        self.s3_key = S3_KEY

    @mock_s3
    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_s3_to_ftp_operation(self):
        # Setting
        test_remote_file_content = (
            "This is remote file content \n which is also multiline "
            "another line here \n this is last line. EOF"
        )

        # Test for creation of s3 bucket
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=self.s3_bucket)
        self.assertTrue(self.s3_hook.check_for_bucket(self.s3_bucket))

        with open(LOCAL_FILE_PATH, 'w') as file:
            file.write(test_remote_file_content)
        self.s3_hook.load_file(LOCAL_FILE_PATH, self.s3_key, bucket_name=BUCKET)

        # Check if object was created in s3
        objects_in_dest_bucket = conn.list_objects(Bucket=self.s3_bucket, Prefix=self.s3_key)
        # there should be object found, and there should only be one object found
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)

        # the object found should be consistent with dest_key specified earlier
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], self.s3_key)

        # get remote file to local
        run_task = S3ToFTPOperator(
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            ftp_path=FTP_PATH,
            ftp_conn_id=FTP_CONN_ID,
            s3_conn_id=S3_CONN_ID,
            task_id=TASK_ID,
            dag=self.dag,
        )
        self.assertIsNotNone(run_task)

        run_task.execute(None)
