# wir benötigen wieder ein paar python imports...
import sys
from os import environ
import os

sys.path.append(os.path.abspath(".."))

from client.client.sparql_builder import * # todo: remove later
from client.client.connector_client import ConnectorClient

client = ConnectorClient(bootstrap_servers=[environ.get("KAFKA_BROKER")])
builder = SPARQLBuilder()


query = builder.build_list_instances_query("df:Device")
output = client.query_graphdb_sync(query)
print(output)

device = builder._wrap_uri(output.iloc[0]["instance"])
output = client.get_related_sync(subject_uri=device, predicate_uri="df:hasDataPoint")
print(output)

datapoint_uri = builder._wrap_uri(output.iloc[0]["object"])
output = client.get_properties_sync(subject_uri=datapoint_uri)
print(output)

client.load_connector_config_sync()


#base_payload, connector = client.resolve_connector_and_payload_sync(datapoint_uri)

client.resolve_and_subscribe(datapoint_uri)