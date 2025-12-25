from datetime import datetime
from typing import Any, Dict, List, Literal
from uuid import UUID

from connector.messages.datamodel_utils import BasePayload, register_payload
from pydantic import Field


@register_payload
class OPCUAReadPayload(BasePayload):
    """Payload for requesting OPC UA read operations.

    This payload instructs the OPC UA connector to read values from one or more
    NodeIDs provided in `nodeid_list`.

    Attributes:
        type: Discriminator identifying the payload type.
        nodeid_list: List of OPC UA NodeIDs whose values should be read.
    """

    type: Literal["OPCUAReadPayload"] = "OPCUAReadPayload"
    nodeid_list: List[str] = Field(..., description="List of OPC UA NodeIDs to read from")


@register_payload
class OPCUAReadResponsePayload(BasePayload):
    """Response payload containing results of an OPC UA read request.

    Attributes:
        type: Discriminator identifying the payload type.
        value_dict: Mapping from NodeID to the value returned by the OPC UA
            server. Values may be any JSON-serializable type.
    """

    type: Literal["OPCUAReadResponsePayload"] = "OPCUAReadResponsePayload"
    value_dict: Dict[str, Any]


@register_payload
class OPCUASubscribePayload(BasePayload):
    """Payload for requesting subscriptions to OPC UA nodes.

    The OPC UA connector is instructed to create monitored subscriptions for the
    given NodeIDs.

    Attributes:
        type: Discriminator identifying the payload type.
        nodeid_list: List of OPC UA NodeIDs to subscribe to.
    """

    type: Literal["OPCUASubscribePayload"] = "OPCUASubscribePayload"
    nodeid_list: List[str] = Field(..., description="List of OPC UA NodeIDs to subscribe to")


@register_payload
class OPCUASubscribeResponsePayload(BasePayload):
    """Response payload for an OPC UA subscription request.

    Attributes:
        type: Discriminator identifying the payload type.
        subscription_id: Unique identifier assigned to the created subscription.
        subscription_results: Mapping from NodeID to subscription result
            indicating whether creating the monitored item succeeded or failed.
    """

    type: Literal["OPCUASubscribeResponsePayload"] = "OPCUASubscribeResponsePayload"
    subscription_id: UUID = Field(..., description="Unique identifier for the active subscription")
    subscription_results: Dict[str, Literal["success", "failed"]] = Field(
        ..., description="Result of each subscription attempt, keyed by NodeID"
    )


@register_payload
class OPCUAWritePayload(BasePayload):
    """Payload for performing write operations on OPC UA nodes.

    Attributes:
        type: Discriminator identifying the payload type.
        value_dict: Mapping from NodeID to the value that should be written to
            that node on the OPC UA server.
    """

    type: Literal["OPCUAWritePayload"] = "OPCUAWritePayload"
    value_dict: Dict[str, Any] = Field(
        ..., description="Dictionary mapping OPC UA NodeIDs to values to write"
    )


@register_payload
class OPCUAWriteResponsePayload(BasePayload):
    """Response payload for OPC UA write operations.

    Attributes:
        type: Discriminator identifying the payload type.
        write_results: Mapping from NodeID to the outcome of the write
            (``success`` or ``failed``).
    """

    type: Literal["OPCUAWriteResponsePayload"] = "OPCUAWriteResponsePayload"
    write_results: Dict[str, Literal["success", "failed"]] = Field(
        ..., description="Result of each write attempt keyed by OPC UA NodeID"
    )


@register_payload
class OPCUAUnsubscribePayload(BasePayload):
    """Payload for unsubscribing from an OPC UA subscription.

    Attributes:
        type: Discriminator identifying the payload type.
        subscription_id: Identifier of the subscription that should be cancelled.
    """

    type: Literal["OPCUAUnsubscribePayload"] = "OPCUAUnsubscribePayload"
    subscription_id: UUID = Field(
        ..., description="Unique identifier for the subscription to unsubscribe from"
    )


@register_payload
class OPCUAUnsubscribeResponsePayload(BasePayload):
    """Response payload for an OPC UA unsubscription request.

    Attributes:
        type: Discriminator identifying the payload type.
        subscription_id: Identifier of the subscription that was targeted for
            unsubscription.
        unsubscription_results: Result flag indicating whether the unsubscription
            succeeded or failed.
    """

    type: Literal["OPCUAUnsubscribeResponsePayload"] = "OPCUAUnsubscribeResponsePayload"
    subscription_id: UUID = Field(
        ..., description="ID of the subscription attempted to be unsubscribed"
    )
    unsubscription_results: Literal["success", "failed"] = Field(
        ..., description="Indicator whether the unsubscription was successful"
    )


@register_payload
class OPCUATelemetryPayload(BasePayload):
    """Telemetry message emitted due to OPC UA subscriptions.

    This payload contains values published by the OPC UA server when monitored
    items change.

    Attributes:
        type: Discriminator identifying the payload type.
        timestamp: Timestamp provided by the OPC UA server for the values.
        values: Mapping from NodeID to the latest telemetry value.
    """

    type: Literal["OPCUATelemetryPayload"] = "OPCUATelemetryPayload"
    timestamp: datetime = Field(..., description="Timestamp delivered from the OPC UA server")
    values: Dict[str, Any] = Field(
        ..., description="Values sent as part of subscription-based telemetry updates"
    )
