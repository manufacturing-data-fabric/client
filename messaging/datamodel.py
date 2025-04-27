from connector.messages.datamodel_utils import BasePayload, register_payload

from pydantic import Field, BaseModel
from typing import Literal, Any, Union, Optional, List, Dict
from uuid import UUID
from datetime import datetime

@register_payload
class OPCUAReadCommandPayload(BasePayload):
    type: Literal['OPCUAReadCommandPayload'] = "OPCUAReadCommandPayload"
    nodeid_list: List[str] = Field(..., description="List of OPC UA NodeIDs to read from")
    # todo: ggfs. in "identifier" umbenennnen?

@register_payload
class OPCUAReadCommandResponse(BasePayload):
    type: Literal['OPCUAReadCommandResponse'] = "OPCUAReadCommandResponse"
    value_dict: Dict[str, Any]



@register_payload
class OPCUASubscribeCommandResponse(BasePayload):
    type: Literal['OPCUASubscribeCommandResponse'] = "OPCUASubscribeCommandResponse"
    subscription_id: UUID = Field(..., description="Unique Identifier for the active subscription")
    subscription_results: Dict[str, Literal["success", "failed"]] = Field(..., description="Result of each "
                                                                                           "subscription attempt, "
                                                                                           "keyed by node ID")

@register_payload
class OPCUAUnsubscribeCommandPayload(BasePayload):
    type: Literal['OPCUAUnsubscribeCommandPayload'] = "OPCUAUnsubscribeCommandPayload"
    subscription_id: UUID = Field(..., description="Unique Identifier for the subscription to be unsubscribed")

@register_payload
class OPCUAUnsubscribeCommandResponse(BasePayload):
    type: Literal['OPCUAUnsubscribeCommandResponse'] = "OPCUAUnsubscribeCommandResponse"
    subscription_id: UUID = Field(..., description="ID of the Subscription attempted to be unsubscribed")
    unsubscription_results: Literal["success", "failed"] = Field(..., description="Indicator whether the Unsubscription was successful")

@register_payload
class OPCUATelemetryPayload(BasePayload):
    type: Literal['OPCUATelemetryPayload'] = "OPCUATelemetryPayload"
    timestamp: datetime = Field(..., description="Timestampe delivered from the OPC UA Server")
    values: Dict[str, Any] = Field(..., description="Singular Values that are sent upon occurrence due to Subscription")