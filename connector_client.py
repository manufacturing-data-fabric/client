import logging
from aiokafka import AIOKafkaProducer
from typing import Type
from os import environ
from dotenv import load_dotenv
import aiohttp
from pprint import pprint

from connector.messages.message_generator import create_request
from connector.messages.datamodel_utils import BasePayload, SubscriptionUnregisterRequest
from connector.messages.datamodel_base import ReadCommand, SubscribeCommand, UnsubscribeCommand, ActionCommand

from connector_client_utils import *
from sparql_queries import *

import pandas as pd
import asyncio

load_dotenv()

# overall todo: provid the possibility to "wrap" this class similarly like it is done with the connector (abstract)!!!
# overall todo: check the abstract broker implementation and see whether there is something to use for here!

class ConnectorClient:
    """A class that has the intention to be wrapping methods for communicating with the connector itself

    The connector should be usable for three different use-cases:
    - providing the possibility for the user to perform scripting tasks (manual interaction with the Connector)
    - providing the capability for the user to perform tool calling using langchain
    - providing the capability for the user to perform comprehensive testing - unit as well as integration

    """
    def __init__(self, bootstrap_servers: List[str]):
        self.bootstrap_server = bootstrap_servers
        self.producer = None

        # initializing with default .env connector
        self.connector_type = environ.get("MODULE_TYPE")
        self.module_id = environ.get("MODULE_ID")
        self.request_topic = f'request.{self.connector_type}.{self.module_id}'
        self.response_topic = f'response.{self.connector_type}.{self.module_id}'
        self.telemetry_topic = f'telemetry.{self.connector_type}.{self.module_id}'

        # list of potential connectors
        self.connectors = {}
        self.active_connector = None

        # graphdb settings
        self.graphdb_host = environ.get("GRAPHDB_HOST")
        self.graphdb_repository = environ.get("GRAPHDB_REPOSITORY_ID")
        self.sparql_endpoint = self.graphdb_host + "/repositories/" + self.graphdb_repository

        self.subscriptions = [] # todo: is it working?
        self._logger = logging.getLogger(__name__)
        self._logger.info(
            f"ConnectorClient initialized with module_id={self.module_id} and bootstrap_servers"
            f"={self.bootstrap_server}")

    async def switch_connector(self, module_id: str):
        if module_id in self.connectors:
            config = self.connectors[module_id]
            self.request_topic = config["request"]
            self.response_topic = config["response"]
            self.telemetry_topic = config["telemetry"]
            self.active_connector = config
            self._logger.info(f"Switched to topic config for module_id={module_id}")
        else:
            raise ValueError(f"No topic config found for module_id={module_id}")

    async def return_connectors(self):
        return self.connectors.keys()

    async def load_connector_config(self):
        result = await self.query_graphdb(build_connector_metadata_query(), pretty=False)

        for connector in result:
            module_id = connector['moduleId']['value']
            module_type = connector['moduleType']['value']
            new_config = {}
            new_config["request"] = f"request.{module_type}.{module_id}"
            new_config["response"] = f"response.{module_type}.{module_id}"
            new_config["telemetry"] = f"telemetry.{module_type}.{module_id}"
            self.connectors[f"{module_id}"] = new_config
            self._logger.info(f"Added new Connector {module_id} to the Topic Configurations.")


    async def _create_producer(self):
        self._logger.info("Creating Kafka producer...")
        self.producer = AIOKafkaProducer(
            value_serializer=lambda m: m.encode('utf-8'),
            bootstrap_servers=self.bootstrap_server
        )
        await self.producer.start()
        self._logger.info("Kafka producer created and started.")

    async def close(self):
        if self.producer:
            await self.producer.stop()
            self._logger.info("Kafka producer stopped.")

    async def _send_request(self, request_message: str, topic_name: str): # todo: do type annotation here if necessary!!!
        if not self.producer or self.producer._closed:
            await self._create_producer()
        try:
            self._logger.info(f"Sending request to topic {topic_name}...")
            await self.producer.send_and_wait(topic=topic_name, value=request_message)
            self._logger.info(f"Request sent to {topic_name} with message: {request_message}")
        except Exception as e:
            self._logger.error(f"Failed to send request to {topic_name}: {e}")

    async def send_read_request(self, base_payload: BasePayload):
        """ Read the image out of an attached camera"""
        self._logger.info(f"Preparing read request for payload: {base_payload}")
        request_message = create_request(base_payload=base_payload, command_type=ReadCommand)
        await self._send_request(dump_payload(request_message), self.request_topic)
        self._logger.info(
            f"Read request sent to {self.request_topic} with payload: {request_message.model_dump_json(indent=4)}")

    async def send_subscribe_request(self, base_payload: BasePayload):
        # todo: important note!!! the subscription_id which is necessary for unsubscribing is located in BasePayload!
        request_message = create_request(base_payload=base_payload, command_type=SubscribeCommand)
        await self._send_request(dump_payload(request_message), self.request_topic)

    async def send_unsubscribe_request(self, base_payload: BasePayload):
        """ Unsubscribe to an image feed of an attached camera"""

        request_message = create_request(base_payload=base_payload, command_type=UnsubscribeCommand)
        await self._send_request(dump_payload(request_message), self.request_topic)

    async def send_action_request(self, base_payload: BasePayload):
        self._logger.info(f"Preparing trigger action request for payload: {base_payload}")

        request_message = create_request(base_payload=base_payload, command_type=ActionCommand)
        await self._send_request(dump_payload(request_message), self.request_topic)

        self._logger.info(f"Trigger action request sent to {self.request_topic} with payload: {request_message.model_dump_json(indent=4)}")

    async def read_data_source(self, base_payload: BasePayload, timeout: int = 5):
        """Send a ReadCommand and wait for the matching response."""

        # todo: timeout does not work yet!
        if not self.producer:
            await self._create_producer()

        request_message = create_request(base_payload=base_payload, command_type=ReadCommand)

        correlation_id = request_message.root.correlation_id

        async with get_kafka_consumer(self.response_topic, self.bootstrap_server) as consumer:
            await self._send_request(dump_payload(request_message), self.request_topic)
            await self.close()
            async def wait_for_response():
                async for message in consumer:
                    msg = MsgModel.model_validate(message.value)
                    if msg.root.correlation_id == correlation_id:
                        print(f"Matching response received:\n"
                              f"{msg.root.payload.base_payload.model_dump_json(indent=4)}")
                        return msg
            try:
                result = await asyncio.wait_for(wait_for_response(), timeout=timeout)
                return result
            except asyncio.TimeoutError:
                print(f"Timeout: No response received for correlation_id {correlation_id} within {timeout} seconds.")

    async def subscribe_data_source(self, base_payload, timeout: int=5):

        if not self.producer:
            await self._create_producer()

        request_message = create_request(base_payload=base_payload, command_type=SubscribeCommand)

        correlation_id = request_message.root.correlation_id
        print(f"Correlation_id of sent message: {correlation_id}")

        async with get_kafka_consumer(self.response_topic, self.bootstrap_server) as consumer:
            await self._send_request(dump_payload(request_message), self.request_topic)
            await self.close()
            print("waiting for a message")

            matched_responses = []

            async def wait_for_response():
                async for message in consumer:
                    print("Received message") # todo: logging command
                    pprint(message.value) # todo: debug command
                    msg = MsgModel.model_validate(message.value)
                    print(f"message type: {type(msg.root.payload.base_payload)}")
                    if msg.root.correlation_id == correlation_id:
                        matched_responses.append(msg)
                        if len(matched_responses) == 2:
                            break
            try:
                result = await asyncio.wait_for(wait_for_response(), timeout=timeout)
                for entry in matched_responses:
                    if entry.root.payload.base_payload.device_origin=="MetadataTopic":
                        print("MetdataTopic received")
                        self.subscriptions.append(entry.root.payload.base_payload.response)

                return matched_responses
            except asyncio.TimeoutError:
                print(f"Timeout: No response received for correlation_id {correlation_id} within {timeout} seconds.")

    async def unsubscribe_data_source(self, base_payload, timeout: int=5):

        if not self.producer:
            await self._create_producer()

        request_message = create_request(base_payload=base_payload, command_type=UnsubscribeCommand)

        correlation_id = request_message.root.correlation_id

        async with get_kafka_consumer(self.response_topic, self.bootstrap_server) as consumer:
            await self._send_request(dump_payload(request_message), self.request_topic)
            await self.close()
            async def wait_for_response():
                async for message in consumer:
                    msg = MsgModel.model_validate(message.value)
                    print(f"message type: {type(msg.root.payload.base_payload)}")
                    if msg.root.correlation_id == correlation_id:
                        print(f"Matching response received:\n"
                              f"{msg.root.payload.base_payload.model_dump_json(indent=4)}")
                        return msg
            try:
                result = await asyncio.wait_for(wait_for_response(), timeout=timeout)
                return result
            except asyncio.TimeoutError:
                print(f"Timeout: No response received for correlation_id {correlation_id} within {timeout} seconds.")

    async def collect_data_for_duration(self, base_payload, duration: int=10) -> list:
        collected_data = []

        msg = await self.subscribe_data_source(base_payload=base_payload)

        subscription_id = msg.root.payload.base_payload.subscription_id

        async with get_kafka_consumer(self.telemetry_topic, self.bootstrap_server) as consumer:
            start_time = asyncio.get_event_loop().time()
            async for message in consumer:
                msg = MsgModel.model_validate(message.value)
                collected_data.append(msg.root.payload.base_payload)

                if asyncio.get_event_loop().time() - start_time > duration:
                    break

        unsubscribe_payload = SubscriptionUnregisterRequest(subscription_identifier=subscription_id,
                                                            device_origin="client")
        #unsubscribe_payload = OPCUAUnsubscribeCommandPayload(subscription_id=subscription_id, device_origin="client")
        await self.unsubscribe_data_source(base_payload=unsubscribe_payload)

        rows = []
        for payload in collected_data:
            for nodeid, value in payload.values.items():
                rows.append({
                    "timestamp": payload.timestamp,
                    "nodeid": nodeid,
                    "value": value,
                })
        return pd.DataFrame(rows)

    async def collect_data_from_stream(self, subscription_id: str, duration: int = 10):

        collected_data = []
        result = await self.query_graphdb(build_subscription_by_id_query((subscription_id)))

        # check if the subscription is there
        if result is None:
            return
        # check if the subscription is closed


        # todo: subscribe_from_stream
        async with get_kafka_consumer(self.telemetry_topic, self.bootstrap_server) as consumer:
            start_time = asyncio.get_event_loop().time()
            async for message in consumer:
                msg = MsgModel.model_validate(message.value)
                payload = msg.root.payload.base_payload

                if str(payload.subscription_id) !=subscription_id:
                    continue

                collected_data.append(payload)

                if asyncio.get_event_loop().time() - start_time > duration:
                    break


        # Convert collected data to DataFrame
        rows = []
        for payload in collected_data:
            for nodeid, value in payload.values.items():
                rows.append({
                    "timestamp": payload.timestamp,
                    "nodeid": nodeid,
                    "value": value,
                })

        return pd.DataFrame(rows)


    # Sync handler functions for the scripting use
    def get_subscriptions(self):
        """ Helper method for returning maintained subscriptions by this client"""

        # todo: needs to be checked!!!
        subscription_ids = []
        for entry in self.subscriptions:
            subscription_ids.append({
                "message_id": entry.root.message_id,
                "timestamp": entry.root.timestamp
            })
        return subscription_ids

####### sparql stuff

    async def query_graphdb(self, query: str, pretty=True):
        async with aiohttp.ClientSession() as session:
            headers = {
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            data = {"query": query}

            async with session.post(self.sparql_endpoint, headers=headers, data=data) as response:
                if response.status == 200:
                    result = await response.json()
                    if pretty == True:
                        return sparql_results_to_dataframe(result["results"]["bindings"])
                    else:
                        return result["results"]["bindings"]
                else:
                    text = await response.text()
                    print(f"SPARQL query failed: {response.status}")
                    print(text)
                    return []

####### sync methods

    def send_request_sync(self, request_message, topic_name: str):
        run_async_in_sync(self._send_request, request_message, topic_name)
        run_async_in_sync(self.close)

    def send_read_request_sync(self, base_payload: Type[BasePayload]):
        run_async_in_sync(self.send_read_request, base_payload)
        run_async_in_sync(self.close)

    def send_subscribe_request_sync(self, base_payload: Type[BasePayload]):
        run_async_in_sync(self.send_subscribe_request, base_payload)
        run_async_in_sync(self.close)

    def send_unsubscribe_request_sync(self, base_payload: Type[BasePayload]):
        run_async_in_sync(self.send_unsubscribe_request, base_payload)
        run_async_in_sync(self.close)

    def send_action_request_sync(self, base_payload: BasePayload):
        run_async_in_sync(self.send_action_request, base_payload)
        run_async_in_sync(self.close)

    def read_data_source_sync(self, base_payload: BasePayload, timeout: int):
        run_async_in_sync(self.read_data_source, base_payload, timeout)


# # todo: make this subscribe_to_nodes generic as it needs to be used for (a) testing as well as for (b) user-scripting
# #  and (c) tool-calling

if __name__ == "__main__":
    # todo: exchange these for e.g. a argparser function
    logging.basicConfig(level=logging.INFO)
    bootstrap_servers = ["localhost:9092"]
    module_id = "my_module"
    client = ConnectorClient(bootstrap_servers=bootstrap_servers, module_id=module_id)