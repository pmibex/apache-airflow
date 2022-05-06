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
from typing import Any, Iterable, List, Optional, Tuple, TypeVar

from flask import current_app, request
from marshmallow import ValidationError
from sqlalchemy import and_, func, or_
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.orm.query import Query
from sqlalchemy.sql import ClauseElement

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import format_datetime, format_parameters
from airflow.api_connexion.schemas.task_instance_schema import (
    TaskInstanceCollection,
    TaskInstanceReferenceCollection,
    clear_task_instance_form,
    run_task_instance_form,
    set_task_instance_state_form,
    task_instance_batch_form,
    task_instance_collection_schema,
    task_instance_reference_collection_schema,
    task_instance_schema,
)
from airflow.api_connexion.types import APIResponse
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models import SlaMiss
from airflow.models.dagrun import DagRun as DR
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances
from airflow.security import permissions
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import RUNNING_DEPS
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState, State

T = TypeVar("T")


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def get_task_instance(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get task instance"""
    query = (
        session.query(TI)
        .filter(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .outerjoin(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.execution_date == DR.execution_date,
                SlaMiss.task_id == TI.task_id,
            ),
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )

    try:
        task_instance = query.one_or_none()
    except MultipleResultsFound:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )
    if task_instance is None:
        raise NotFound("Task instance not found")
    if task_instance[0].map_index != -1:
        raise NotFound(
            "Task instance not found", detail="Task instance is mapped, add the map_index value to the URL"
        )

    return task_instance_schema.dump(task_instance)


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def get_mapped_task_instance(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get task instance"""
    query = (
        session.query(TI)
        .filter(
            TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index == map_index
        )
        .join(TI.dag_run)
        .outerjoin(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.execution_date == DR.execution_date,
                SlaMiss.task_id == TI.task_id,
            ),
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instance = query.one_or_none()
    if task_instance is None:
        raise NotFound("Task instance not found")

    return task_instance_schema.dump(task_instance)


@format_parameters(
    {
        "execution_date_gte": format_datetime,
        "execution_date_lte": format_datetime,
        "start_date_gte": format_datetime,
        "start_date_lte": format_datetime,
        "end_date_gte": format_datetime,
        "end_date_lte": format_datetime,
    },
)
@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def get_mapped_task_instances(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    execution_date_gte: Optional[str] = None,
    execution_date_lte: Optional[str] = None,
    start_date_gte: Optional[str] = None,
    start_date_lte: Optional[str] = None,
    end_date_gte: Optional[str] = None,
    end_date_lte: Optional[str] = None,
    duration_gte: Optional[float] = None,
    duration_lte: Optional[float] = None,
    state: Optional[List[str]] = None,
    pool: Optional[List[str]] = None,
    queue: Optional[List[str]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get list of task instances."""
    # Because state can be 'none'
    states = _convert_state(state)

    base_query = (
        session.query(TI)
        .filter(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index >= 0)
        .join(TI.dag_run)
    )

    # 0 can mean a mapped TI that expanded to an empty list, so it is not an automatic 404
    if base_query.with_entities(func.count('*')).scalar() == 0:
        dag = current_app.dag_bag.get_dag(dag_id)
        if not dag:
            error_message = f"DAG {dag_id} not found"
            raise NotFound(error_message)
        task = dag.get_task(task_id)
        if not task:
            error_message = f"Task id {task_id} not found"
            raise NotFound(error_message)
        if not task.is_mapped:
            error_message = f"Task id {task_id} is not mapped"
            raise NotFound(error_message)

    # Other search criteria
    query = _apply_range_filter(
        base_query,
        key=DR.execution_date,
        value_range=(execution_date_gte, execution_date_lte),
    )
    query = _apply_range_filter(query, key=TI.start_date, value_range=(start_date_gte, start_date_lte))
    query = _apply_range_filter(query, key=TI.end_date, value_range=(end_date_gte, end_date_lte))
    query = _apply_range_filter(query, key=TI.duration, value_range=(duration_gte, duration_lte))
    query = _apply_array_filter(query, key=TI.state, values=states)
    query = _apply_array_filter(query, key=TI.pool, values=pool)
    query = _apply_array_filter(query, key=TI.queue, values=queue)

    # Count elements before joining extra columns
    total_entries = query.with_entities(func.count('*')).scalar()

    # Add SLA miss
    query = (
        query.join(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.task_id == TI.task_id,
                SlaMiss.execution_date == DR.execution_date,
            ),
            isouter=True,
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )

    if order_by:
        if order_by == 'state':
            query = query.order_by(TI.state.asc(), TI.map_index.asc())
        elif order_by == '-state':
            query = query.order_by(TI.state.desc(), TI.map_index.asc())
        elif order_by == '-map_index':
            query = query.order_by(TI.map_index.desc())
        else:
            raise BadRequest(detail=f"Ordering with '{order_by}' is not supported")
    else:
        query = query.order_by(TI.map_index.asc())

    task_instances = query.offset(offset).limit(limit).all()
    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


def _convert_state(states: Optional[Iterable[str]]) -> Optional[List[Optional[str]]]:
    if not states:
        return None
    return [State.NONE if s == "none" else s for s in states]


def _apply_array_filter(query: Query, key: ClauseElement, values: Optional[Iterable[Any]]) -> Query:
    if values is not None:
        cond = ((key == v) for v in values)
        query = query.filter(or_(*cond))
    return query


def _apply_range_filter(query: Query, key: ClauseElement, value_range: Tuple[T, T]) -> Query:
    gte_value, lte_value = value_range
    if gte_value is not None:
        query = query.filter(key >= gte_value)
    if lte_value is not None:
        query = query.filter(key <= lte_value)
    return query


@format_parameters(
    {
        "execution_date_gte": format_datetime,
        "execution_date_lte": format_datetime,
        "start_date_gte": format_datetime,
        "start_date_lte": format_datetime,
        "end_date_gte": format_datetime,
        "end_date_lte": format_datetime,
    },
)
@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def get_task_instances(
    *,
    limit: int,
    dag_id: Optional[str] = None,
    dag_run_id: Optional[str] = None,
    execution_date_gte: Optional[str] = None,
    execution_date_lte: Optional[str] = None,
    start_date_gte: Optional[str] = None,
    start_date_lte: Optional[str] = None,
    end_date_gte: Optional[str] = None,
    end_date_lte: Optional[str] = None,
    duration_gte: Optional[float] = None,
    duration_lte: Optional[float] = None,
    state: Optional[List[str]] = None,
    pool: Optional[List[str]] = None,
    queue: Optional[List[str]] = None,
    offset: Optional[int] = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get list of task instances."""
    # Because state can be 'none'
    states = _convert_state(state)

    base_query = session.query(TI).join(TI.dag_run)

    if dag_id != "~":
        base_query = base_query.filter(TI.dag_id == dag_id)
    if dag_run_id != "~":
        base_query = base_query.filter(TI.run_id == dag_run_id)
    base_query = _apply_range_filter(
        base_query,
        key=DR.execution_date,
        value_range=(execution_date_gte, execution_date_lte),
    )
    base_query = _apply_range_filter(
        base_query, key=TI.start_date, value_range=(start_date_gte, start_date_lte)
    )
    base_query = _apply_range_filter(base_query, key=TI.end_date, value_range=(end_date_gte, end_date_lte))
    base_query = _apply_range_filter(base_query, key=TI.duration, value_range=(duration_gte, duration_lte))
    base_query = _apply_array_filter(base_query, key=TI.state, values=states)
    base_query = _apply_array_filter(base_query, key=TI.pool, values=pool)
    base_query = _apply_array_filter(base_query, key=TI.queue, values=queue)

    # Count elements before joining extra columns
    total_entries = base_query.with_entities(func.count('*')).scalar()
    # Add join
    query = (
        base_query.join(
            SlaMiss,
            and_(
                SlaMiss.dag_id == TI.dag_id,
                SlaMiss.task_id == TI.task_id,
                SlaMiss.execution_date == DR.execution_date,
            ),
            isouter=True,
        )
        .add_entity(SlaMiss)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instances = query.offset(offset).limit(limit).all()
    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def get_task_instances_batch(session: Session = NEW_SESSION) -> APIResponse:
    """Get list of task instances."""
    body = request.get_json()
    try:
        data = task_instance_batch_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    states = _convert_state(data['state'])
    base_query = session.query(TI).join(TI.dag_run)

    base_query = _apply_array_filter(base_query, key=TI.dag_id, values=data["dag_ids"])
    base_query = _apply_range_filter(
        base_query,
        key=DR.execution_date,
        value_range=(data["execution_date_gte"], data["execution_date_lte"]),
    )
    base_query = _apply_range_filter(
        base_query,
        key=TI.start_date,
        value_range=(data["start_date_gte"], data["start_date_lte"]),
    )
    base_query = _apply_range_filter(
        base_query, key=TI.end_date, value_range=(data["end_date_gte"], data["end_date_lte"])
    )
    base_query = _apply_range_filter(
        base_query, key=TI.duration, value_range=(data["duration_gte"], data["duration_lte"])
    )
    base_query = _apply_array_filter(base_query, key=TI.state, values=states)
    base_query = _apply_array_filter(base_query, key=TI.pool, values=data["pool"])
    base_query = _apply_array_filter(base_query, key=TI.queue, values=data["queue"])

    # Count elements before joining extra columns
    total_entries = base_query.with_entities(func.count('*')).scalar()
    # Add join
    base_query = base_query.join(
        SlaMiss,
        and_(
            SlaMiss.dag_id == TI.dag_id,
            SlaMiss.task_id == TI.task_id,
            SlaMiss.execution_date == DR.execution_date,
        ),
        isouter=True,
    ).add_entity(SlaMiss)
    ti_query = base_query.options(joinedload(TI.rendered_task_instance_fields))
    task_instances = ti_query.all()

    return task_instance_collection_schema.dump(
        TaskInstanceCollection(task_instances=task_instances, total_entries=total_entries)
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def post_clear_task_instances(*, dag_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Clear task instances."""
    body = request.get_json()
    try:
        data = clear_task_instance_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    dag = current_app.dag_bag.get_dag(dag_id)
    if not dag:
        error_message = f"Dag id {dag_id} not found"
        raise NotFound(error_message)
    reset_dag_runs = data.pop('reset_dag_runs')
    dry_run = data.pop('dry_run')
    # We always pass dry_run here, otherwise this would try to confirm on the terminal!
    task_instances = dag.clear(dry_run=True, dag_bag=current_app.dag_bag, **data)
    if not dry_run:
        clear_task_instances(
            task_instances.all(),
            session,
            dag=dag,
            dag_run_state=DagRunState.QUEUED if reset_dag_runs else False,
        )

    return task_instance_reference_collection_schema.dump(
        TaskInstanceReferenceCollection(task_instances=task_instances.all())
    )


@security.requires_access(
    [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def post_set_task_instances_state(*, dag_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Set a state of task instances."""
    body = request.get_json()
    try:
        data = set_task_instance_state_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    error_message = f"Dag ID {dag_id} not found"
    dag = current_app.dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound(error_message)

    task_id = data['task_id']
    task = dag.task_dict.get(task_id)

    if not task:
        error_message = f"Task ID {task_id} not found"
        raise NotFound(error_message)

    execution_date = data.get('execution_date')
    run_id = data.get('dag_run_id')
    if (
        execution_date
        and (
            session.query(TI)
            .filter(TI.task_id == task_id, TI.dag_id == dag_id, TI.execution_date == execution_date)
            .one_or_none()
        )
        is None
    ):
        raise NotFound(
            detail=f"Task instance not found for task {task_id!r} on execution_date {execution_date}"
        )

    if run_id and not session.query(TI).get(
        {'task_id': task_id, 'dag_id': dag_id, 'run_id': run_id, 'map_index': -1}
    ):
        error_message = f"Task instance not found for task {task_id!r} on DAG run with ID {run_id!r}"
        raise NotFound(detail=error_message)

    tis = dag.set_task_instance_state(
        task_id=task_id,
        run_id=run_id,
        execution_date=execution_date,
        state=data["new_state"],
        upstream=data["include_upstream"],
        downstream=data["include_downstream"],
        future=data["include_future"],
        past=data["include_past"],
        commit=not data["dry_run"],
        session=session,
    )
    return task_instance_reference_collection_schema.dump(TaskInstanceReferenceCollection(task_instances=tis))


@security.requires_access(
    [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
    ],
)
@provide_session
def run_task_instance(
    *, dag_id: str, dag_run_id: str, task_id: str, session: Session = NEW_SESSION
) -> APIResponse:
    """Set a state of task instances."""
    body = request.get_json()
    try:
        data = run_task_instance_form.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    ignore_all_deps = data.get('ignore_all_deps')
    ignore_task_deps = data.get('ignore_task_deps')
    ignore_ti_state = data.get('ignore_ti_state')
    map_index = data.get('map_index')

    executor = ExecutorLoader.get_default_executor()
    if not getattr(executor, "supports_ad_hoc_ti_run", False):
        error_message = "Only works with the Celery, CeleryKubernetes or Kubernetes executors"
        raise BadRequest(detail=error_message)

    error_message = f"Dag ID {dag_id} not found"
    dag = current_app.dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound(error_message)

    error_message = f"Task ID {task_id} not found"
    task = dag.task_dict.get(task_id)
    if not task:
        raise NotFound(error_message)

    dag_run = dag.get_dagrun(run_id=dag_run_id)
    error_message = f"DagRun ID {dag_run_id} not found"
    if not dag_run:
        raise NotFound(error_message)

    task_instance = dag_run.get_task_instance(task_id=task.task_id, map_index=map_index)
    task_instance.refresh_from_task(task)
    if not task_instance:
        error_message = f"Task instance not found for task {task_id!r} on DAG run with ID {dag_run_id!r}"
        raise NotFound(detail=error_message)

    dep_context = DepContext(
        deps=RUNNING_DEPS,
        ignore_all_deps=ignore_all_deps,
        ignore_task_deps=ignore_task_deps,
        ignore_ti_state=ignore_ti_state,
    )

    failed_deps = list(task_instance.get_failed_dep_statuses(dep_context=dep_context))
    if failed_deps:
        failed_deps_str = ", ".join(f"{dep.dep_name}: {dep.reason}" for dep in failed_deps)
        error_message = (
            f"Could not queue task instance for execution, dependencies not met: {failed_deps_str}"
        )
        raise BadRequest(detail=error_message)

    executor.job_id = "manual"
    executor.start()
    executor.queue_task_instance(
        task_instance,
        ignore_all_deps=ignore_all_deps,
        ignore_task_deps=ignore_task_deps,
        ignore_ti_state=ignore_ti_state,
    )
    executor.heartbeat()
    task_instance.queued_dttm = timezone.utcnow()
    session.merge(task_instance)
    return task_instance_schema.dump([task_instance, None])
