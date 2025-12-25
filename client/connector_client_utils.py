import asyncio
import json
from contextlib import asynccontextmanager
from typing import (Any, AsyncIterator, Awaitable, Callable, Dict, Iterable,
                    List, Mapping, TypeVar)

import pandas as pd
from aiokafka import AIOKafkaConsumer
from connector.messages.datamodel_base import MsgModel

T = TypeVar("T")


def _strip_uri(value: object) -> object:
    """Strip URI prefixes from a SPARQL value dict or return value unchanged.

    This helper expects SPARQL JSON result values of the form::

        {"type": "uri", "value": "http://example.com#LocalName"}

    Args:
        value: Either a SPARQL value dict or any other value.

    Returns:
        The simplified string (last fragment after '#' or '/'), or the original
        value if it cannot be simplified.
    """
    if isinstance(value, Mapping) and "value" in value:
        val_str = value["value"]
        if not isinstance(val_str, str):
            return value
        if "#" in val_str:
            return val_str.split("#")[-1]
        if "/" in val_str:
            return val_str.split("/")[-1]
        return val_str
    return value


def sparql_results_to_dataframe(
    bindings: List[Mapping[str, Mapping[str, Any]]],
    simplify: bool = True,
) -> pd.DataFrame:
    """Convert SPARQL query JSON bindings to a pandas DataFrame.

    This function expects the standard SPARQL JSON format where each binding is a
    dict mapping variable names to dicts containing at least a ``"value"`` key.

    Example input element::

        {
            "var1": {"type": "uri", "value": "http://example.com#Foo"},
            "var2": {"type": "literal", "value": "bar"}
        }

    Args:
        bindings: The ``"bindings"`` list from a SPARQL query result.
        simplify: If True, URI values are simplified to their last segment
            (after ``#`` or ``/``). If False, the raw string in ``"value"`` is
            used.

    Returns:
        pd.DataFrame: A flattened DataFrame with one row per binding and one
        column per variable.
    """
    rows: List[Dict[str, Any]] = []

    for binding in bindings:
        row: Dict[str, Any] = {}
        for key, value_dict in binding.items():
            if simplify:
                row[key] = _strip_uri(value_dict)
            else:
                row[key] = value_dict.get("value", None)
        rows.append(row)

    return pd.DataFrame(rows)


def run_async_in_sync(
    async_func: Callable[..., Awaitable[T]],
    *args: object,
    **kwargs: object,
) -> T:
    """Run an async function from synchronous code.

    This is intended for use from *synchronous* contexts such as a plain Python
    script or REPL. If called from within an already running event loop (for
    example inside an ``async def`` function or some notebook environments), a
    ``RuntimeError`` is raised – in that case you should simply use ``await``
    directly instead of this helper.

    Args:
        async_func: The async callable to execute.
        *args: Positional arguments passed to ``async_func``.
        **kwargs: Keyword arguments passed to ``async_func``.

    Returns:
        The result returned by ``async_func``.

    Raises:
        RuntimeError: If an event loop is already running in the current thread.
    """
    try:
        # If this succeeds, we are already inside an event loop.
        asyncio.get_running_loop()
        raise RuntimeError(
            "run_async_in_sync() cannot be used when an event loop is already "
            "running. Use `await` directly in async code."
        )
    except RuntimeError:
        # No running loop in this thread: safe to use asyncio.run()
        return asyncio.run(async_func(*args, **kwargs))


def dump_payload(msg: MsgModel) -> str:
    """Serialize a message model to pretty-printed JSON.

    Args:
        msg: Message model instance derived from ``MsgModel``.

    Returns:
        A JSON string representation of the message, indented for readability.
    """
    return msg.model_dump_json(indent=4)


@asynccontextmanager
async def get_kafka_consumer(
    topic: str,
    bootstrap_servers: List[str],
) -> AsyncIterator[AIOKafkaConsumer]:
    """Create and manage the lifecycle of an ``AIOKafkaConsumer``.

    This is an async context manager that handles starting and stopping the
    consumer for a given topic.

    Example:
        >>> async with get_kafka_consumer("my-topic", ["kafka:9092"]) as consumer:
        ...     async for msg in consumer:
        ...         print(msg.value)

    Args:
        topic: Kafka topic to subscribe to.
        bootstrap_servers: List of Kafka bootstrap server addresses, e.g.
            ``["localhost:9092"]``.

    Yields:
        An initialized and started :class:`AIOKafkaConsumer` instance.

    Raises:
        aiokafka.errors.KafkaError: If the consumer fails to start or communicate
        with the Kafka cluster.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


def simplify_sparql_results(
    results: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Simplify SPARQL JSON bindings by stripping URI prefixes.

    This function is similar in spirit to :func:`sparql_results_to_dataframe`,
    but operates on a list of dicts and returns a list of simplified dicts
    instead of a DataFrame.

    Args:
        results: An iterable of SPARQL result rows. Each row is expected to be a
            mapping from variable name to either a SPARQL value dict (with a
            ``"value"`` key) or a plain value.

    Returns:
        A list of dictionaries where URI-like values are simplified to their
        local name (the last segment after ``#`` or ``/``) and non-URI values
        are returned unchanged.
    """
    simplified: List[Dict[str, Any]] = []

    for row in results:
        simplified_row: Dict[str, Any] = {key: _strip_uri(value) for key, value in row.items()}
        simplified.append(simplified_row)

    return simplified
