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

from datetime import datetime, timezone

from flask import request
from flask_login import current_user
from marshmallow import ValidationError
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload, subqueryload

from airflow import Dataset
from airflow.api_connexion import security
from airflow.api_connexion.endpoints.request_dict import get_json_request_dict
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.dataset_schema import (
    DatasetCollection,
    DatasetEventCollection,
    dataset_change_schema,
    dataset_collection_schema,
    dataset_event_collection_schema,
    dataset_event_schema,
    dataset_schema,
)
from airflow.api_connexion.types import APIResponse
from airflow.datasets.manager import dataset_manager
from airflow.models.dataset import DatasetEvent, DatasetModel
from airflow.security import permissions
from airflow.utils.session import NEW_SESSION, provide_session


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET)])
@provide_session
def get_dataset(uri: str, session: Session = NEW_SESSION) -> APIResponse:
    """Get a Dataset."""
    dataset = (
        session.query(DatasetModel)
        .filter(DatasetModel.uri == uri)
        .options(joinedload(DatasetModel.consuming_dags), joinedload(DatasetModel.producing_tasks))
        .one_or_none()
    )
    if not dataset:
        raise NotFound(
            "Dataset not found",
            detail=f"The Dataset with uri: `{uri}` was not found",
        )
    return dataset_schema.dump(dataset)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET)])
@format_parameters({"limit": check_limit})
@provide_session
def get_datasets(
    *,
    limit: int,
    offset: int = 0,
    uri_pattern: str | None = None,
    order_by: str = "id",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get datasets."""
    allowed_attrs = ["id", "uri", "created_at", "updated_at"]

    total_entries = session.query(func.count(DatasetModel.id)).scalar()
    query = session.query(DatasetModel)
    if uri_pattern:
        query = query.filter(DatasetModel.uri.ilike(f"%{uri_pattern}%"))
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    datasets = (
        query.options(subqueryload(DatasetModel.consuming_dags), subqueryload(DatasetModel.producing_tasks))
        .offset(offset)
        .limit(limit)
        .all()
    )
    return dataset_collection_schema.dump(DatasetCollection(datasets=datasets, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET)])
@provide_session
@format_parameters({"limit": check_limit})
def get_dataset_events(
    *,
    limit: int,
    offset: int = 0,
    order_by: str = "timestamp",
    dataset_id: int | None = None,
    source_dag_id: str | None = None,
    source_task_id: str | None = None,
    source_run_id: str | None = None,
    source_map_index: int | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get dataset events."""
    allowed_attrs = ["source_dag_id", "source_task_id", "source_run_id", "source_map_index", "timestamp"]

    query = session.query(DatasetEvent)

    if dataset_id:
        query = query.filter(DatasetEvent.dataset_id == dataset_id)
    if source_dag_id:
        query = query.filter(DatasetEvent.source_dag_id == source_dag_id)
    if source_task_id:
        query = query.filter(DatasetEvent.source_task_id == source_task_id)
    if source_run_id:
        query = query.filter(DatasetEvent.source_run_id == source_run_id)
    if source_map_index:
        query = query.filter(DatasetEvent.source_map_index == source_map_index)

    query = query.options(subqueryload(DatasetEvent.created_dagruns))

    total_entries = query.count()
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    events = query.offset(offset).limit(limit).all()
    return dataset_event_collection_schema.dump(
        DatasetEventCollection(dataset_events=events, total_entries=total_entries)
    )


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DATASET)])
@provide_session
def post_dataset_event(session: Session = NEW_SESSION) -> APIResponse:
    """Create an external dataset event. This endpoint is useful if you want to update a dataset and
    trigger downstream DAG runs from external services.
    """
    try:
        json_body = dataset_change_schema.load(get_json_request_dict())
    except ValidationError as err:
        raise BadRequest(detail=str(err))
    uri = json_body["dataset_uri"]
    external_source = request.remote_addr
    user_id = getattr(current_user, "id", None)
    timestamp = json_body.get("timestamp", datetime.now())
    timestamp = timestamp.astimezone(timezone.utc)
    extra = json_body.get("extra", {})
    dataset_event = dataset_manager.register_external_dataset_change(
        dataset=Dataset(uri),
        external_source=external_source,
        user_id=user_id,
        timestamp=timestamp,
        extra=extra,
        session=session,
    )

    if dataset_event:
        event_json = dataset_event_schema.dump(dataset_event)
        # removing created_dagruns, since they will be created asynchronously in the scheduler
        event_json.pop("created_dagruns")
        return event_json

    raise BadRequest(
        "Dataset not found",
        detail=f"The Dataset with uri: `{uri}` was not found",
    )
