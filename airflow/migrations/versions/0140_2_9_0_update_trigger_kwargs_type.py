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

"""update trigger kwargs type

Revision ID: 1949afb29106
Revises: ee1467d4aa35
Create Date: 2024-03-17 22:09:09.406395

"""
import sqlalchemy as sa

from airflow.models.trigger import Trigger
from alembic import op

from airflow.utils.sqlalchemy import ExtendedJSON

# revision identifiers, used by Alembic.
revision = "1949afb29106"
down_revision = "ee1467d4aa35"
branch_labels = None
depends_on = None
airflow_version = "2.9.0"


def upgrade():
    """Update trigger kwargs type to string"""
    with op.batch_alter_table("trigger") as batch_op:
        batch_op.alter_column("kwargs", type_=sa.Text(), )


def downgrade():
    """Unapply update trigger kwargs type to string"""
    with op.batch_alter_table("trigger") as batch_op:
        batch_op.alter_column("kwargs", type_=ExtendedJSON(), postgresql_using="kwargs::json")
