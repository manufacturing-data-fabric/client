from scripts.connector_client import ConnectorClient
from messaging.base_payload_examples import example_read_payload, example_subscribe_payload


from dotenv import load_dotenv
from os import environ
from uuid import UUID

"""
This script uses the ClientConnector class for demonstrating the exemplary interaction with an instance of 
connector_idscamera.
"""
# todo: at a later point transfer this here to e.g. a jupyter notebook!

load_dotenv()

#module_id = environ.get("MODULE_ID")
client = ConnectorClient(bootstrap_servers=[environ.get("KAFKA_BROKER")], module_id = environ.get("MODULE_ID"))

# read out an image using the request command
#client.send_read_request_sync(base_payload=example_read_payload)

# send a SubscribeCommand
client.send_subscribe_request_sync(base_payload=example_subscribe_payload)

# send an UnsubscribeCommand
#unsubscribe_payload = generate_unsubscribe_payload(
#    subscription_id=UUID("6fe8f974-99cd-44f5-a26b-40fc565a3168"),
#    device_origin="agent",
#    status="closed"
#)
#client.send_unsubscribe_request_sync(base_payload=unsubscribe_payload)

# send an ActionCommand with a trigger
#client.send_action_request_sync(base_payload=example_ids_trigger_payload_minio)
# todo: find a way to preserve the subscription id throughout requests for doing a subscribe/unsubscribe cycle!

#out = client.read_data_source_sync(base_payload=example_read_payload, timeout=1)


