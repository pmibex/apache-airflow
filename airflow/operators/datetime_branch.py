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

import datetime
from typing import Dict, Iterable, Union

from airflow.exceptions import AirflowException
from airflow.operators.branch_operator import BaseBranchOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class DateTimeBranchOperator(BaseBranchOperator):
    """
    Branches into one of two lists of tasks depending on the current datetime.

    True branch will be returned when `datetime.datetime.now()` falls below
    `target_upper` and above `target_lower`.

    :param follow_task_ids_if_true: task id or task ids to follow if
        `datetime.datetime.now()` falls above target_lower and below `target_upper`.
    :type follow_task_ids_if_true: str or list[str]
    :param follow_task_ids_if_false: task id or task ids to follow if
        `datetime.datetime.now()` falls below target_lower or above `target_upper`.
    :type follow_task_ids_if_false: str or list[str]
    :param target_lower: target lower bound.
    :type target_lower: Optional[datetime.datetime]
    :param target_upper: target upper bound.
    :type target_upper: Optional[datetime.datetime]
    """

    @apply_defaults
    def __init__(
        self,
        *,
        follow_task_ids_if_true: Union[str, Iterable[str]],
        follow_task_ids_if_false: Union[str, Iterable[str]],
        target_lower: Union[datetime.datetime, datetime.time, None],
        target_upper: Union[datetime.datetime, datetime.time, None],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if target_lower is None and target_upper is None:
            raise AirflowException(
                "Both target_upper and target_lower are None. At least one "
                "must be defined to be compared to the current datetime"
            )

        self.target_lower = target_lower
        self.target_upper = target_upper
        self.follow_task_ids_if_true = follow_task_ids_if_true
        self.follow_task_ids_if_false = follow_task_ids_if_false

    def choose_branch(self, context: Dict) -> Union[str, Iterable[str]]:
        now = timezone.make_naive(timezone.utcnow(), self.dag.timezone)

        lower, upper = target_times_as_dates(now, self.target_lower, self.target_upper)

        if upper is not None and upper < now:
            return self.follow_task_ids_if_false

        if lower is not None and lower > now:
            return self.follow_task_ids_if_false

        return self.follow_task_ids_if_true


def target_times_as_dates(
    base_date: datetime.datetime,
    lower: Union[datetime.datetime, datetime.time, None],
    upper: Union[datetime.datetime, datetime.time, None],
):
    """Ensures upper and lower time targets are datetimes by combining them with base_date"""
    if isinstance(lower, datetime.datetime) and isinstance(upper, datetime.datetime):
        return lower, upper

    if lower is not None and isinstance(lower, datetime.time):
        lower = datetime.datetime.combine(base_date, lower)
    if upper is not None and isinstance(upper, datetime.time):
        upper = datetime.datetime.combine(base_date, upper)

    if any(date is None for date in (lower, upper)):
        return lower, upper

    if upper < lower:
        upper += datetime.timedelta(days=1)
    return lower, upper
