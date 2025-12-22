from messaging.datamodel import *
from typing import List

def generate_read_payload(nodeid_list: List[str]) -> BasePayload:
    return OPCUAReadPayload(
        device_origin="client",
        nodeid_list = nodeid_list
    )
