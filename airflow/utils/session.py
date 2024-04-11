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

import contextlib
from functools import wraps
from inspect import signature
from typing import Callable, Generator, TypeVar, cast

from sqlalchemy.orm import Session as SASession

from airflow import settings
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.settings import TracebackSession
from airflow.typing_compat import ParamSpec


@contextlib.contextmanager
def create_session() -> Generator[SASession, None, None]:
    """Contextmanager that will create and teardown a session."""
    if InternalApiConfig.get_use_internal_api():
        yield cast("SASession", TracebackSession())
        return
    Session = getattr(settings, "Session", None)
    if Session is None:
        raise RuntimeError("Session must be set before!")
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


PS = ParamSpec("PS")
RT = TypeVar("RT")


def find_session_idx(func: Callable[PS, RT]) -> int:
    """Find session index in function call parameter."""
    func_params = signature(func).parameters
    try:
        # func_params is an ordered dict -- this is the "recommended" way of getting the position
        session_args_idx = tuple(func_params).index("session")
    except ValueError:
        raise ValueError(f"Function {func.__qualname__} has no `session` argument") from None

    return session_args_idx


def provide_session(func: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Provide a session if it isn't provided.

    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """

    @wraps(func)
    def wrapper(*args: PS.args, **kwargs: PS.kwargs) -> RT:
        sig = signature(func)
        bind_args = sig.bind_partial(*args, **kwargs)
        session = bind_args.arguments.get("session")

        if session is None:
            with create_session() as session:
                bind_args.arguments["session"] = session
                return func(*bind_args.args, **bind_args.kwargs)
        return func(*args, **kwargs)

    return wrapper


# A fake session to use in functions decorated by provide_session. This allows
# the 'session' argument to be of type Session instead of Session | None,
# making it easier to type hint the function body without dealing with the None
# case that can never happen at runtime.
NEW_SESSION: SASession = cast(SASession, None)
