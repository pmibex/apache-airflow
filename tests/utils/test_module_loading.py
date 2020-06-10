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

from airflow.utils.module_loading import import_string


class TestModuleImport(unittest.TestCase):
    def test_import_string(self):
        cls = import_string('airflow.utils.module_loading.import_string')
        self.assertEqual(cls, import_string)

        # Test false_positive_class_names raised
        with self.assertRaises(ImportError):
            import_string('no_dots_in_path')
        msg = 'Module "airflow.utils" does not define a "nonexistent" attribute'
        with self.assertRaisesRegex(ImportError, msg):
            import_string('airflow.utils.nonexistent')
