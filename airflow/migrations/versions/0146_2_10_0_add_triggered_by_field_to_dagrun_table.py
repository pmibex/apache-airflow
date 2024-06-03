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

"""add triggered_by field to DagRun table.

Revision ID: 7fa05fa4e719
Revises: c4602ba06b4b
Create Date: 2024-05-08 13:36:51.114374

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7fa05fa4e719"
down_revision = "c4602ba06b4b"
branch_labels = None
depends_on = None
airflow_version = "2.10.0"


def upgrade():
    """Apply add triggered_by field to DagRun table."""
    op.add_column("dag_run", sa.Column("triggered_by", sa.String(50), nullable=True))


def downgrade():
    """Unapply add triggered_by field to DagRun table."""
    op.drop_column("dag_run", "triggered_by")
