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
import subprocess
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, List, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class PigCliHook(BaseHook):
    """
    Simple wrapper around the pig CLI.

    Note that you can also set default pig CLI properties using the
    ``pig_properties`` to be used in your connection as in
    ``{"pig_properties": "-Dpig.tmpfilecompression=true"}``

    """

    def __init__(self, pig_cli_conn_id: str = "pig_cli_default") -> None:
        super().__init__()
        conn = self.get_connection(pig_cli_conn_id)
        self.pig_properties = conn.extra_dejson.get('pig_properties', '')
        self.conn = conn
        self.sub_process = None

    def run_cli(self, pig: str, pig_opts: Optional[str] = None, verbose: bool = True) -> Any:
        """
        Run an pig script using the pig cli

        >>> ph = PigCliHook()
        >>> result = ph.run_cli("ls /;", pig_opts="-x mapreduce")
        >>> ("hdfs://" in result)
        True
        """
        with TemporaryDirectory(prefix='airflow_pigop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                f.write(pig.encode('utf-8'))
                f.flush()
                fname = f.name
                pig_bin = 'pig'
                cmd_extra: List[str] = []

                pig_cmd = [pig_bin]

                if self.pig_properties:
                    pig_properties_list = self.pig_properties.split()
                    pig_cmd.extend(pig_properties_list)
                if pig_opts:
                    pig_opts_list = pig_opts.split()
                    pig_cmd.extend(pig_opts_list)

                pig_cmd.extend(['-f', fname] + cmd_extra)

                if verbose:
                    self.log.info("%s", " ".join(pig_cmd))
                sub_process: Any = subprocess.Popen(
                    pig_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=tmp_dir, close_fds=True
                )
                self.sub_process = sub_process
                stdout = ''
                for line in iter(sub_process.stdout.readline, b''):
                    stdout += line.decode('utf-8')
                    if verbose:
                        self.log.info(line.strip())
                sub_process.wait()

                if sub_process.returncode:
                    raise AirflowException(stdout)

                return stdout

    def kill(self) -> None:
        """
        Kill Pig job
        """
        if self.sub_process:
            if self.sub_process.poll() is None:
                self.log.info("Killing the Pig job")
                self.sub_process.kill()
