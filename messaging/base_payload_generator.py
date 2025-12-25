from typing import List

from client.messaging.datamodel import BasePayload, OPCUAReadPayload


def generate_read_payload(nodeid_list: List[str]) -> BasePayload:
    """Generate an OPCUAReadPayload for reading the specified NodeIDs.

    Args:
        nodeid_list: OPC UA NodeIDs to read.

    Returns:
        OPCUAReadPayload with "client" as device_origin.
    """
    return OPCUAReadPayload(device_origin="client", nodeid_list=nodeid_list)
