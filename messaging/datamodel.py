from datetime import datetime
from typing import Any, Dict, List, Literal
from uuid import UUID

from connector.messages.datamodel_utils import BasePayload, register_payload
from pydantic import Field


# Read Command:
@register_payload
class OPCUAReadPayload(BasePayload):
    type: Literal["OPCUAReadPayload"] = "OPCUAReadPayload"
    nodeid_list: List[str] = Field(..., description="List of OPC UA NodeIDs to read from")
    # todo: ggfs. in "identifier" umbenennnen?

@register_payload
class OPCUAReadResponsePayload(BasePayload):
    type: Literal["OPCUAReadResponsePayload"] = "OPCUAReadResponsePayload"
    value_dict: Dict[str, Any]

# Subscribe Command:
@register_payload
class OPCUASubscribePayload(BasePayload):
    type: Literal["OPCUASubscribePayload"] = "OPCUASubscribePayload"
    nodeid_list: List[str] = Field(..., description="List of OPC UA NodeIDs to subscribe to")

@register_payload
class OPCUASubscribeResponsePayload(BasePayload):
    type: Literal["OPCUASubscribeResponsePayload"] = "OPCUASubscribeResponsePayload"
    subscription_id: UUID = Field(..., description="Unique Identifier for the active subscription")
    subscription_results: Dict[str, Literal["success", "failed"]] = Field(..., description="Result of each "
                                                                                           "subscription attempt, "
                                                                                           "keyed by node ID")

@register_payload
class OPCUAWritePayload(BasePayload):
    type: Literal["OPCUAWritePayload"] = "OPCUAWritePayload"
    value_dict: Dict[str, Any] = Field(..., description="Dictionary mapping OPC UA NodeIDs to values to write")

@register_payload
class OPCUAWriteResponsePayload(BasePayload):
    type: Literal["OPCUAWriteResponsePayload"] = "OPCUAWriteResponsePayload"
    write_results: Dict[str, Literal["success", "failed"]] = Field(..., description="Result of each write attempt keyed by OPC UA NodeID.")

@register_payload
class OPCUAUnsubscribePayload(BasePayload):
    type: Literal["OPCUAUnsubscribePayload"] = "OPCUAUnsubscribePayload"
    subscription_id: UUID = Field(..., description="Unique Identifier for the subscription to be unsubscribed")

@register_payload
class OPCUAUnsubscribeResponsePayload(BasePayload):
    type: Literal["OPCUAUnsubscribeResponsePayload"] = "OPCUAUnsubscribeResponsePayload"
    subscription_id: UUID = Field(..., description="ID of the Subscription attempted to be unsubscribed")
    unsubscription_results: Literal["success", "failed"] = Field(..., description="Indicator whether the Unsubscription was successful")

@register_payload
class OPCUATelemetryPayload(BasePayload):
    type: Literal["OPCUATelemetryPayload"] = "OPCUATelemetryPayload"
    timestamp: datetime = Field(..., description="Timestampe delivered from the OPC UA Server")
    values: Dict[str, Any] = Field(..., description="Singular Values that are sent upon occurrence due to Subscription")