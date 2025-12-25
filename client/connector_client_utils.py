import asyncio
import json
from contextlib import asynccontextmanager
from typing import List

import pandas as pd
from aiokafka import AIOKafkaConsumer
from connector.messages.datamodel_base import MsgModel


def sparql_results_to_dataframe(bindings: list, simplify: bool = True) -> pd.DataFrame:
    """Convert SPARQL query JSON bindings to a pandas DataFrame.

    Args:
        bindings (list): The 'bindings' list from a SPARQL query result.
        simplify (bool): Whether to simplify URIs in the values.

    Returns:
        pd.DataFrame: Flattened and optionally simplified DataFrame.
    """

    def strip_uri(val):
        if isinstance(val, dict) and "value" in val:
            value = val["value"]
            if "#" in value:
                return value.split("#")[-1]
            elif "/" in value:
                return value.split("/")[-1]
            return value
        return val

    rows = []
    for binding in bindings:
        row = {}
        for key, value_dict in binding.items():
            if simplify:
                row[key] = strip_uri(value_dict)
            else:
                row[key] = value_dict.get("value", None)
        rows.append(row)

    return pd.DataFrame(rows)


def sparql_results_to_dataframe_old(bindings: list) -> pd.DataFrame:
    """Convert SPARQL query JSON bindings to a pandas DataFrame.

    Args:
        bindings (list): The 'bindings' list from a SPARQL query result.

    Returns:
        pd.DataFrame: Flattened and human-readable DataFrame.
    """
    rows = []

    for binding in bindings:
        row = {}
        for key, value_dict in binding.items():
            row[key] = value_dict.get("value", None)
        rows.append(row)

    return pd.DataFrame(rows)


# Helper function to run async methods in sync mode
def run_async_in_sync(async_func, *args, **kwargs):
    """Run async functions in sync mode in Python console"""
    loop = asyncio.get_event_loop()

    if loop.is_running():
        # In some environments like Jupyter, loop is already running
        # So we create a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    result = loop.run_until_complete(async_func(*args, **kwargs))
    return result


def dump_payload(msg: MsgModel):
    return msg.model_dump_json(indent=4)


@asynccontextmanager
async def get_kafka_consumer(topic: str, bootstrap_servers: List[str]):
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


def simplify_sparql_results(results):
    def strip_uri(val):
        if isinstance(val, dict) and "value" in val:
            value = val["value"]
            if "#" in value:
                return value.split("#")[-1]
            elif "/" in value:
                return value.split("/")[-1]
            return value
        return val

    simplified = []
    for row in results:
        simplified_row = {k: strip_uri(v) for k, v in row.items()}
        simplified.append(simplified_row)
    return simplified
