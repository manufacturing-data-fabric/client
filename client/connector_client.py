import logging
#from idlelib.colorizer import matched_named_groups #todo: remove?

from aiokafka import AIOKafkaProducer # todo: exchange via the KafkaBroker class?
from typing import Type
from os import environ
from dotenv import load_dotenv
import aiohttp
from uuid import uuid4

from rdflib import URIRef

from connector.messages.message_generator import create_request
from connector.messages.datamodel_utils import BasePayload, SubscriptionUnregisterRequest, SubscriptionRegisterRequest
from connector.messages.datamodel_base import ReadCommand, SubscribeCommand, UnsubscribeCommand, ActionCommand

from .connector_client_utils import *
from .sparql_builder import *

# Namespace constants (until namespace_resolver is integrated)
DF = "http://stephantrattnig.org/data_fabric_ontology#"
DF_INSTANCE = "http://stephantrattnig.org/instances#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
from messaging.datamodel import OPCUAReadPayload, \
    OPCUAWritePayload  # todo: for a protocol-neutral client, this needs to be removed

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

        self.builder = SPARQLBuilder()

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

    def return_connectors(self):
        return self.connectors.keys()

    async def load_connector_config(self):
        # todo: think about executing this during __init__
        # todo: the df: was necessary but was just adapted here for convenience. Reason are new sparql_builder methods
        # todo: in future, eventually clean up and use the namespace_resolver here?
        df = "http://stephantrattnig.org/data_fabric_ontology#"
        query = self.builder.build_list_instances_query(
            class_uri=f"{df}Connector",
            optional_props=[
                f"{df}moduleId",
                f"{df}moduleType"
            ]
        )
        result = await self.query_graphdb(query, pretty=False)

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

    # todo: gather read, subscribe, unsubscribe and action request in one method!
    async def publish_read_command(self, base_payload: BasePayload):
        """ Senad a ReadCommand message to a dedicated request topic"""
        self._logger.info(f"Preparing read request for payload: {base_payload}")
        request_message = create_request(base_payload=base_payload, command_type=ReadCommand)
        await self._send_request(dump_payload(request_message), self.request_topic)
        self._logger.info(
            f"Read request sent to {self.request_topic} with payload: {request_message.model_dump_json(indent=4)}")

    async def publish_subscribe_command(self, base_payload: BasePayload):
        """ Send a Subscription message to a dedicated request topic"""
        request_message = create_request(base_payload=base_payload, command_type=SubscribeCommand)
        await self._send_request(dump_payload(request_message), self.request_topic)

    async def publish_unsubscribe_command(self, base_payload: BasePayload):
        """ Unsubscribe to an image feed of an attached camera"""

        request_message = create_request(base_payload=base_payload, command_type=UnsubscribeCommand)
        await self._send_request(dump_payload(request_message), self.request_topic)

    async def publish_action_command(self, base_payload: BasePayload):
        self._logger.info(f"Preparing trigger action request for payload: {base_payload}")

        request_message = create_request(base_payload=base_payload, command_type=ActionCommand)
        await self._send_request(dump_payload(request_message), self.request_topic)

        self._logger.info(f"Trigger action request sent to {self.request_topic} with payload: {request_message.model_dump_json(indent=4)}")

    async def send_read_and_await_response(self, base_payload: BasePayload, timeout: int = 5):
        """Send a ReadCommand and wait for the matching response."""

        # todo: timeout does not work yet!
        if not self.producer:
            await self._create_producer()

        request_message = create_request(base_payload=base_payload, command_type=ReadCommand)
        correlation_id = request_message.root.correlation_id

        self._logger.info(f"Sent read request with correlation_id: {correlation_id}")
        self._logger.debug(f"Read payload: {request_message.model_dump_json(indent=2)}")

        async with get_kafka_consumer(self.response_topic, self.bootstrap_server) as consumer:
            await self._send_request(dump_payload(request_message), self.request_topic)
            await self.close()

            async def wait_for_response():
                async for message in consumer:
                    msg = MsgModel.model_validate(message.value)
                    if msg.root.correlation_id == correlation_id:
                        base_payload = msg.root.payload.base_payload
                        self._logger.info(f"Received read response from: {base_payload.device_origin}")
                        self._logger.debug(f"Message payload: {base_payload.model_dump_json(indent=2)}")
                        return base_payload

            try:
                result = await asyncio.wait_for(wait_for_response(), timeout=timeout)
                print(result)
                return {
                    "status": "success",
                    "device_origin": result.device_origin,
                    "message": f"✅ Read successful from {result.device_origin}",
                    "values": getattr(result, "value_dict", None),
                }

            except asyncio.TimeoutError:
                return {
                    "status": "timeout",
                    "message": f"❌ No read response within {timeout} seconds.",
                    "values": None
                }

    async def send_subscribe_and_await_response(self, base_payload, timeout: int = 5):
        """Send a subscribe request and return concise, user-friendly feedback."""

        if not self.producer:
            await self._create_producer()

        request_message = create_request(base_payload=base_payload, command_type=SubscribeCommand)
        correlation_id = request_message.root.correlation_id

        self._logger.info(f"Sent subscribe request with correlation_id: {correlation_id}")
        self._logger.debug(f"Subscribe payload: {request_message.model_dump_json(indent=2)}")

        async with (get_kafka_consumer(self.response_topic, self.bootstrap_server) as consumer):
            await self._send_request(dump_payload(request_message), self.request_topic)
            await self.close()

            matched_responses = []

            async def wait_for_response():
                async for message in consumer:
                    msg = MsgModel.model_validate(message.value)
                    if msg.root.correlation_id == correlation_id:
                        matched_responses.append(msg)
                        base_payload = msg.root.payload.base_payload
                        self._logger.info(f"Received response from: {base_payload.device_origin}")
                        self._logger.debug(f"Message payload: {base_payload.model_dump_json(indent=2)}")
                        if len(matched_responses) == 2:
                            break

            try:
                await asyncio.wait_for(wait_for_response(), timeout=timeout)
                subscription_id = request_message.root.payload.base_payload.model_dump()
                self.subscriptions.append(subscription_id)

                return matched_responses[0].model_dump(), matched_responses[1].model_dump()

            except asyncio.TimeoutError:
                print(matched_responses) # todo: remove this!
                # todo: these matched responses should
                if matched_responses:
                    self._logger.warning("Timeout occurred, but one response was received.")
                    base = matched_responses[0].root.payload.base_payload
                    return base.model_dump()
                else:
                    return {
                        "status": "timeout",
                        "subscription_id": None,
                        "message": f"No response within {timeout} seconds.",
                    }

    async def send_unsubscribe_and_await_response(self, base_payload, timeout: int=5):

        if not self.producer:
            await self._create_producer()

        request_message = create_request(base_payload=base_payload, command_type=UnsubscribeCommand)
        correlation_id = request_message.root.correlation_id

        self._logger.info(f"Sent unsubscribe request with correlation_id: {correlation_id}")
        self._logger.debug(f"Unsubscribe payload: {request_message.model_dump_json(indent=2)}")

        async with get_kafka_consumer(self.response_topic, self.bootstrap_server) as consumer:
            await self._send_request(dump_payload(request_message), self.request_topic)
            await self.close()

            matched_responses = []

            async def wait_for_response():
                async for message in consumer:
                    msg = MsgModel.model_validate(message.value)
                    if msg.root.correlation_id == correlation_id:
                        matched_responses.append(msg)
                        base_payload = msg.root.payload.base_payload
                        self._logger.info(f"Received response from: {base_payload.device_origin}")
                        self._logger.debug(f"Message payload: {base_payload.model_dump_json(indent=2)}")
                        if len(matched_responses) == 2:
                            break

            try:
                await asyncio.wait_for(wait_for_response(), timeout=timeout)
                return matched_responses[0].model_dump(), matched_responses[1].model_dump()

            except asyncio.TimeoutError:
                if matched_responses:
                    self._logger.warning("Timeout occurred, but one response was received.")
                    base = matched_responses[0].root.payload.base_payload
                    return base.model_dump()
                else:
                    return {
                        "status": "timeout",
                        "message": f"No response within {timeout} seconds.",
                    }

    async def send_action_and_await_response(self, base_payload, timeout: int=5):

        if not self.producer:
            await self._create_producer()

        request_message = create_request(base_payload=base_payload, command_type=ActionCommand)
        correlation_id = request_message.root.correlation_id

        self._logger.info(f"Sent action command with correlation_id: {correlation_id}")
        self._logger.debug(f"Action payload: {request_message.model_dump_json(indent=2)}")

        async with get_kafka_consumer(self.response_topic, self.bootstrap_server) as consumer:
            await self._send_request(dump_payload(request_message), self.request_topic)
            await self.close()

            async def wait_for_response():
                async for message in consumer:
                    msg = MsgModel.model_validate(message.value)
                    if msg.root.correlation_id == correlation_id:
                        base_payload = msg.root.payload.base_payload
                        self._logger.info(f"Received response from: {base_payload.device_origin}")
                        self._logger.debug(f"Message payload: {base_payload.model_dump_json(indent=2)}")
                        return base_payload

            try:
                result = await asyncio.wait_for(wait_for_response(), timeout=timeout)
                return result.model_dump()

            except asyncio.TimeoutError:
                return {
                    "status": "success",
                    "message": f"No response received for correlation_id {correlation_id} within {timeout} seconds.",
                }

    async def resolve_connector_from_datapoint(self, datapoint_uri: str):
        """Resolve all KG information needed for subscription or read"""

        # Get DataPoint identifier
        props = await self.get_properties(subject_uri=datapoint_uri, property_uris=[f"{DF}dataPointIdentifier"], pretty=True)
        datapoint_identifier = props.iloc[0]["object"]

        # Trace to device
        device_result = await self.get_related_inverse(object_uri=datapoint_uri, predicate_uri=f"{DF}hasDataPoint", pretty=True)
        device_uri = f"{DF_INSTANCE}{device_result.iloc[0]['subjectIri']}"

        # Trace to service
        service_result = await self.get_related(subject_uri=device_uri, predicate_uri=f"{DF}providesService", pretty=True)
        service_uri = f"{DF_INSTANCE}{service_result.iloc[0]['object']}"

        # Trace to connector
        connector_result = await self.get_related_inverse(object_uri=service_uri, predicate_uri=f"{DF}connectedTo", pretty=True)
        connector_uri = f"{DF_INSTANCE}{connector_result.iloc[0]['subjectIri']}"

        module_result = await self.get_properties(subject_uri=connector_uri, property_uris=[f"{DF}moduleId"], pretty=True)
        module_id = module_result.iloc[0]["object"]

        return {
            "datapoint_identifier": datapoint_identifier,
            "device_uri": device_uri,
            "service_uri": service_uri,
            "connector_uri": connector_uri,
            "module_id": module_id,
        }

    async def resolve_connector_from_subscription(self, subscription_uri: str):

        result = await self.get_related_inverse(object_uri=subscription_uri, predicate_uri=f"{DF}tracksSubscription", pretty=True)
        connector_uri = f"{DF_INSTANCE}{result.iloc[0]['subjectIri']}"  # this should be the connector URI

        module_result = await self.get_properties(subject_uri=connector_uri, property_uris=[f"{DF}moduleId"], pretty=True)
        module_id = module_result.iloc[0]["object"]

        return {
            "connector_uri": connector_uri,
            "module_id": module_id,
            "subscription_uri": subscription_uri
        }

    async def resolve_and_subscribe(self, datapoint_uri: str, timeout: int=5):
        """High-level method to trace, switch connector, and subscribe."""
        result = await self.resolve_connector_from_datapoint(datapoint_uri)

        datapoint_identifier = result["datapoint_identifier"]

        base_payload = SubscriptionRegisterRequest(
            datapoint_identifier=[datapoint_identifier],
            device_origin="client",
            subscription_identifier=uuid4()
        )

        await self.switch_connector(result["module_id"])
        return await self.send_subscribe_and_await_response(base_payload, timeout)

    async def resolve_and_read_datapoint(self, datapoint_uri: str, timeout: int = 5):
        """High-level method to trace, switch connector, and read data."""
        result = await self.resolve_connector_from_datapoint(datapoint_uri)

        # todo: at a later point, this payload should be fetched from the knowledge graph coupled with the interaction module!
        # await self.get_related(subject_uri=result["connector_uri"], predicate_uri="df:hasCapability")
        # then query the  capabilities if the read capability is there
        # then pull the related datamodel (a property)
        # then continue

        nodeid_list = result["datapoint_identifier"]

        read_payload = OPCUAReadPayload(
            device_origin="client",
            nodeid_list=[nodeid_list],
        )

        await self.switch_connector(result["module_id"])
        return await self.send_read_and_await_response(read_payload, timeout)

    async def resolve_and_unsubscribe(self, subscription_uri: str, timeout: int=5):

        result = await self.resolve_connector_from_subscription(subscription_uri)

        print(result["subscription_uri"].split("#")[-1])
        base_payload = SubscriptionUnregisterRequest(
            subscription_identifier=result["subscription_uri"].split("#")[-1],
            device_origin="client"
        )

        await self.switch_connector(result["module_id"])
        return await self.send_unsubscribe_and_await_response(base_payload, timeout)

    async def resolve_and_trigger_action(self, value_dict: str, timeout: int=5):
        # todo: at a later point, datapoint_uri should be a generic dict with data payloads for actions

        # todo: resolve payload from knowledge graph (e.g. trace back the action from the KG
        result = {}
        result["module_id"] = "opcua_dev_connector"

        base_payload = OPCUAWritePayload(
            device_origin="client",
            value_dict=value_dict
        )

        await self.switch_connector(result["module_id"])
        return await self.send_action_and_await_response(base_payload, timeout)

    # todo: old and potentially deprecated - not of interest for core client
    async def collect_data_for_duration(self, base_payload, duration: int=10) -> list:
        collected_data = []

        msg = await self.send_subscribe_and_await_response(base_payload=base_payload)

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
        await self.send_unsubscribe_and_await_response(base_payload=unsubscribe_payload)

        rows = []
        for payload in collected_data:
            for nodeid, value in payload.values.items():
                rows.append({
                    "timestamp": payload.timestamp,
                    "nodeid": nodeid,
                    "value": value,
                })
        return pd.DataFrame(rows)

    # todo: old and potentially deprecated - not of interest for core client
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

    async def query_graphdb(self, query: str, pretty=False):
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

    # basic intents
    async def get_all_classes(self, pretty: bool=False):
        query = self.builder.get_all_classes()
        return await self.query_graphdb(query, pretty=pretty)

    async def list_instances(self, class_uri: str, optional_props: list[str] = None, pretty: bool = False):
        query = self.builder.build_list_instances_query(class_uri, optional_props)
        return await self.query_graphdb(query, pretty=pretty)

    async def get_properties(self, subject_uri: str, property_uris: list[str] = None, pretty: bool = False):
        query = self.builder.build_get_properties_query(subject_uri, property_uris)
        return await self.query_graphdb(query, pretty=pretty)

    async def get_related(self, subject_uri: str, predicate_uri: str, pretty: bool = False):
        query = self.builder.build_get_related_query(subject_uri, predicate_uri)
        return await self.query_graphdb(query, pretty=pretty)

    async def get_related_inverse(self, object_uri: str, predicate_uri: str, optional_props: list[str] = None, pretty=False):
        query = self.builder. build_get_related_inverse_query(object_uri, predicate_uri, optional_props)
        return await self.query_graphdb(query, pretty=pretty)

    async def search_entity(self, keyword: str, class_uri: str = None, property_uri: str = "rdfs:label",
                     match_mode: str = "fuzzy", pretty: bool = False):
        query = self.builder.build_search_entity_query(keyword, class_uri, property_uri, match_mode)
        return await self.query_graphdb(query, pretty=pretty)

####### sync methods

    def resolve_connector_and_payload_sync(self, datapoint_uri: str):
        return run_async_in_sync(self.resolve_connector_from_datapoint, datapoint_uri)

    def send_request_sync(self, request_message, topic_name: str):
        run_async_in_sync(self._send_request, request_message, topic_name)
        run_async_in_sync(self.close)

    def publish_read_command_sync(self, base_payload: Type[BasePayload]):
        run_async_in_sync(self.publish_read_command, base_payload)
        run_async_in_sync(self.close)

    def publish_subscribe_command_sync(self, base_payload: Type[BasePayload]):
        run_async_in_sync(self.publish_subscribe_command, base_payload)
        run_async_in_sync(self.close)

    def publish_unsubscribe_command_sync(self, base_payload: Type[BasePayload]):
        run_async_in_sync(self.publish_unsubscribe_command, base_payload)
        run_async_in_sync(self.close)

    def publish_action_command_sync(self, base_payload: BasePayload):
        run_async_in_sync(self.publish_action_command, base_payload)
        run_async_in_sync(self.close)

    def send_read_and_await_response_sync(self, base_payload: BasePayload, timeout: int):
        run_async_in_sync(self.send_read_and_await_response, base_payload, timeout)

    def query_graphdb_sync(self, query: str, pretty=True):
        return run_async_in_sync(self.query_graphdb, query, pretty)

    def get_all_classes_sync(self):
        return run_async_in_sync(self.get_all_classes)

    def list_instances_sync(self, class_uri: str, optional_props: list[str] = None):
        return run_async_in_sync(self.list_instances, class_uri, optional_props)

    def get_properties_sync(self, subject_uri: str, property_uris: list[str] = None):
        return run_async_in_sync(self.get_properties, subject_uri, property_uris)

    def get_related_sync(self, subject_uri: str, predicate_uri: str):
        return run_async_in_sync(self.get_related, subject_uri, predicate_uri)

    def get_related_inverse_sync(self, object_uri: str, predicate_uri: str, optional_props: list[str] = None):
        return run_async_in_sync(self.get_related_inverse, object_uri, predicate_uri, optional_props)

    def search_entity_sync(self, keyword: str, class_uri: str = None, property_uri: str = "rdfs:label",
                           match_mode: str = "fuzzy"):
        return run_async_in_sync(self.search_entity, keyword, class_uri, property_uri, match_mode)

    def load_connector_config_sync(self):
        run_async_in_sync(self.load_connector_config)

# # todo: make this subscribe_to_nodes generic as it needs to be used for (a) testing as well as for (b) user-scripting
# #  and (c) tool-calling

if __name__ == "__main__":
    # todo: exchange these for e.g. a argparser function
    logging.basicConfig(level=logging.INFO)
    bootstrap_servers = ["localhost:9092"]
    module_id = "my_module"
    client = ConnectorClient(bootstrap_servers=bootstrap_servers, module_id=module_id)