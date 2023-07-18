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
from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import PAST_DEPENDS_MET, TaskInstance as TI
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.state import TaskInstanceState


class PrevDagrunDep(BaseTIDep):
    """
    Is the past dagrun in a state that allows this task instance to run.

    For example, did this task instance's task in the previous dagrun complete
    if we are depending on past?
    """

    NAME = "Previous Dagrun State"
    IGNORABLE = True
    IS_TASK_DEP = True

    @staticmethod
    def _push_past_deps_met_xcom_if_needed(ti: TI, dep_context):
        if dep_context.wait_for_past_depends_before_skipping:
            ti.xcom_push(key=PAST_DEPENDS_MET, value=True)

    @staticmethod
    def _get_task_instances(dagrun: DagRun, task_id: str, *, session: Session) -> list[TI]:
        """Get task instances from given DAG run with matching task ID.

        This function exists for easy mocking in tests.
        """
        stmt = select(TI).where(TI.dag_id == dagrun.dag_id, TI.task_id == task_id, TI.run_id == dagrun.run_id)
        return session.scalars(stmt).all()

    @provide_session
    def _get_dep_statuses(self, ti: TI, session: Session, dep_context):
        if dep_context.ignore_depends_on_past:
            self._push_past_deps_met_xcom_if_needed(ti, dep_context)
            reason = "The context specified that the state of past DAGs could be ignored."
            yield self._passing_status(reason=reason)
            return

        if not ti.task.depends_on_past:
            self._push_past_deps_met_xcom_if_needed(ti, dep_context)
            yield self._passing_status(reason="The task did not have depends_on_past set.")
            return

        dr = ti.get_dagrun(session=session)
        if not dr:
            self._push_past_deps_met_xcom_if_needed(ti, dep_context)
            yield self._passing_status(reason="This task instance does not belong to a DAG.")
            return

        # Don't depend on the previous task instance if we are the first task.
        catchup = ti.task.dag and ti.task.dag.catchup
        if catchup:
            last_dagrun = dr.get_previous_scheduled_dagrun(session)
        else:
            last_dagrun = dr.get_previous_dagrun(session=session)

        # First ever run for this DAG.
        if not last_dagrun:
            self._push_past_deps_met_xcom_if_needed(ti, dep_context)
            yield self._passing_status(reason="This task instance was the first task instance for its task.")
            return

        # There was a DAG run, but the task wasn't active back then.
        if catchup and last_dagrun.execution_date < ti.task.start_date:
            self._push_past_deps_met_xcom_if_needed(ti, dep_context)
            yield self._passing_status(reason="This task instance was the first task instance for its task.")
            return

        previous_tis = self._get_task_instances(last_dagrun, ti.task_id, session=session)

        if not previous_tis:
            if ti.task.ignore_first_depends_on_past:
                has_historical_ti = (
                    session.query(func.count(TI.dag_id))
                    .filter(
                        TI.dag_id == ti.dag_id,
                        TI.task_id == ti.task_id,
                        TI.execution_date < ti.execution_date,
                    )
                    .scalar()
                    > 0
                )
                if not has_historical_ti:
                    self._push_past_deps_met_xcom_if_needed(ti, dep_context)
                    yield self._passing_status(
                        reason="ignore_first_depends_on_past is true for this task "
                        "and it is the first task instance for its task."
                    )
                    return

            yield self._failing_status(
                reason="depends_on_past is true for this task's DAG, but the previous "
                "task instance has not run yet."
            )
            return

        unsuccessful_previous_tis = [
            ti
            for ti in previous_tis
            if ti.state not in {TaskInstanceState.SKIPPED, TaskInstanceState.SUCCESS}
        ]
        if unsuccessful_previous_tis:
            ti_str = ",".join(str(ti) for ti in unsuccessful_previous_tis)
            reason = (
                f"depends_on_past is true for this task, but the previous task instance(s) "
                f"{ti_str} are not in a successful state."
            )
            yield self._failing_status(reason=reason)
            return

        if ti.task.wait_for_downstream:

            def _are_depdendencies_done(prev_ti: TI) -> bool:
                prev_ti.task = ti.task
                return prev_ti.are_dependents_done(session=session)

            previous_tis_dependents_not_done = [ti for ti in previous_tis if not _are_depdendencies_done(ti)]
            if previous_tis_dependents_not_done:
                ti_str = ",".join(str(ti) for ti in previous_tis_dependents_not_done)
                yield self._failing_status(
                    reason=(
                        f"The tasks downstream of the previous task instance(s) {ti_str} haven't completed "
                        f"(and wait_for_downstream is True)."
                    )
                )
                return

        self._push_past_deps_met_xcom_if_needed(ti, dep_context)
