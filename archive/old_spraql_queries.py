################ Old ##################
# These are old queries - eventually "remove them sometime, or use them for "hot paths" throughout the query agent


# list instances
def build_all_devices_query():
    """
    Constructs a SPARQL query to retrieve all devices and their basic metadata.

    Returns:
        str: A SPARQL query string.
    """
    return """
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?device ?label ?deviceAddress ?communicationProtocol ?deviceModelName ?deviceManufacturer
    WHERE {
        ?device a df:Device .
        OPTIONAL { ?device rdfs:label ?label . }
        OPTIONAL { ?device df:deviceAddress ?deviceAddress . }
        OPTIONAL { ?device df:communicationProtocol ?communicationProtocol . }
        OPTIONAL { ?device df:deviceModelName ?deviceModelName . }
        OPTIONAL { ?device df:deviceManufacturer ?deviceManufacturer . }
    }
    """


# search entity
def build_device_by_identifier_query(identifier: str):
    return f"""
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    SELECT ?device
    WHERE {{
        ?device a df:Device .
        ?device df:deviceIdentifier "{identifier}" .
    }}
    """


# get properties
def build_device_details_query(device_uri: str) -> str:
    return f"""
PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?device ?label ?identifier ?manufacturer ?model ?type ?address
WHERE {{
  VALUES ?device {{
    <{device_uri}>
  }}
  OPTIONAL {{ ?device rdfs:label ?label . }}
  OPTIONAL {{ ?device df:deviceIdentifier ?identifier . }}
  OPTIONAL {{ ?device df:deviceManufacturer ?manufacturer . }}
  OPTIONAL {{ ?device df:deviceModelName ?model . }}
  OPTIONAL {{ ?device df:deviceType ?type . }}
  OPTIONAL {{ ?device df:deviceAddress ?address . }}
}}
"""


# very specific (but related to get related)
def build_subscription_query(device_identifier=None, service_uri=None):
    device_filter = (
        f'?device df:deviceIdentifier "{device_identifier}" .'
        if device_identifier
        else "?device df:deviceIdentifier ?identifier ."
    )
    service_filter = f"FILTER (?service = <{service_uri}>)" if service_uri else ""

    query = f"""
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>

    SELECT ?subscription ?device ?datapoint ?start ?state
    WHERE {{
      {device_filter}
      ?device df:providesService ?service .
      {service_filter}
      ?service df:providesDataPoint ?datapoint .
      ?subscription df:subscribesToDataPoint ?datapoint .
      OPTIONAL {{ ?subscription df:subscriptionStart ?start . }}
      OPTIONAL {{ ?subscription df:hasSubscriptionState ?state . }}
    }}
    """
    return query


# specific query (multi-hop get_related_inverse)
def build_subscription_query_new(device_identifier=None, service_uri=None):
    device_filter = (
        f'?device df:deviceIdentifier "{device_identifier}" .'
        if device_identifier
        else "?device df:deviceIdentifier ?identifier ."
    )
    service_filter = f"FILTER (?service = <{service_uri}>)" if service_uri else ""

    query = f"""
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?subscription ?subscriptionLabel ?device ?deviceIdentifier ?datapoint ?datapointIdentifier ?start ?state
    WHERE {{
      {device_filter}
      ?device df:providesService ?service .
      {service_filter}
      ?service df:providesDataPoint ?datapoint .
      ?subscription df:subscribesToDataPoint ?datapoint .

      OPTIONAL {{ ?device df:deviceIdentifier ?deviceIdentifier . }}
      OPTIONAL {{ ?subscription rdfs:label ?subscriptionLabel . }}
      OPTIONAL {{ ?datapoint df:dataPointIdentifier ?datapointIdentifier . }}
      OPTIONAL {{ ?subscription df:subscriptionStart ?start . }}
      OPTIONAL {{ ?subscription df:hasSubscriptionState ?state . }}
    }}
    """
    return query


# list instances
def build_all_subscriptions_query():
    return """
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?subscription ?start ?end ?state
    WHERE {
        ?subscription a df:Subscription .
        OPTIONAL { ?subscription df:subscriptionStart ?start . }
        OPTIONAL { ?subscription df:subscriptionEnd ?end . }
        OPTIONAL { ?subscription df:hasSubscriptionState ?state . }
    }
    """


# get related
def get_datapoint_query(service_uri: str) -> str:
    return f"""
        PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?datapoint ?identifier ?unit ?datatype
        WHERE {{
            <{service_uri}> df:providesDataPoint ?datapoint .
            OPTIONAL {{ ?datapoint df:dataPointIdentifier ?identifier . }}
            OPTIONAL {{ ?datapoint df:hasUnit ?unit . }}
            OPTIONAL {{ ?datapoint df:hasDataType ?datatype . }}
        }}
    """


# get related inverse
def build_subscriptions_for_datapoint_query(datapoint_uri: str) -> str:
    return f"""
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    SELECT ?subscription ?start ?end ?state
    WHERE {{
        ?subscription a df:Subscription ;
                      df:subscribesToDataPoint <{datapoint_uri}> .
        OPTIONAL {{ ?subscription df:subscriptionStart ?start . }}
        OPTIONAL {{ ?subscription df:subscriptionEnd ?end . }}
        OPTIONAL {{ ?subscription df:hasSubscriptionState ?state . }}
    }}
    """


# list instances
def build_topics_query() -> str:
    return """
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?topic ?name ?type
    WHERE {
      ?topic a df:Topic .
      OPTIONAL { ?topic df:topicName ?name . }
      OPTIONAL { ?topic df:hasTopicType ?type . }
    }
    """


# special case multi hop most closely related to get_related
def build_topic_query(
    connector_uri: str = None, topic_type_uri: str = None, device_identifier: str = None
) -> str:
    """
    Build a flexible SPARQL query to fetch topics.
    Supports filtering by connector, topic type, and device identifier.

    Args:
        connector_uri (str, optional): URI of the connector.
        topic_type_uri (str, optional): URI of the topic type (e.g., df:telemetryType).
        device_identifier (str, optional): Device identifier string.

    Returns:
        str: SPARQL query string.
    """

    conditions = []
    traversal = "?topic a df:Topic ."

    if connector_uri:
        conditions.append(f"<{connector_uri}> df:publishesTo ?topic .")

    if topic_type_uri:
        conditions.append(f"?topic df:hasTopicType <{topic_type_uri}> .")

    if device_identifier:
        traversal = f"""
            ?device df:deviceIdentifier "{device_identifier}" ;
                    df:providesService ?service .
            ?connector df:connectedTo ?service ;
                       df:publishesTo ?topic .
        """

    return f"""
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?topic ?name ?type
    WHERE {{
        {traversal}
        {' '.join(conditions)}
        OPTIONAL {{ ?topic df:topicName ?name . }}
        OPTIONAL {{ ?topic df:hasTopicType ?type . }}
    }}
    """


# more complex one: multi-hop relationship
def build_subscription_by_id_query(subscription_id: str) -> str:
    """
    Build a SPARQL query for retrieving metadata about a specific subscription, given its ID.

    Args:
        subscription_id (str): The UUID or local name part of the subscription URI.

    Returns:
        str: A SPARQL query string.
    """
    instance_namespace = "http://stephantrattnig.org/instances#"
    subscription_uri = f"{instance_namespace}{subscription_id}"

    return f"""
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?start ?end ?state ?datapoint ?device
    WHERE {{
        <{subscription_uri}> a df:Subscription ;
                             df:subscribesToDataPoint ?datapoint .
        OPTIONAL {{ <{subscription_uri}> df:subscriptionStart ?start . }}
        OPTIONAL {{ <{subscription_uri}> df:subscriptionEnd ?end . }}
        OPTIONAL {{ <{subscription_uri}> df:hasSubscriptionState ?state . }}
        OPTIONAL {{
            ?device df:providesService ?service .
            ?service df:providesDataPoint ?datapoint .
        }}
    }}
    """


# more complex one: multi hop and so on
def build_datapoints_for_subscription_query(subscription_id: str) -> str:
    """
    Build a SPARQL query to get all DataPoints related to a given Subscription.

    Args:
        subscription_id (str): UUID string of the subscription.

    Returns:
        str: SPARQL query string.
    """
    return f"""
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?datapoint ?datapointIdentifier ?unit ?datatype
    WHERE {{
        <http://stephantrattnig.org/instances#{subscription_id}> a df:Subscription ;
                                                                 df:subscribesToDataPoint ?datapoint .
        OPTIONAL {{ ?datapoint df:dataPointIdentifier ?datapointIdentifier . }}
        OPTIONAL {{ ?datapoint df:hasUnit ?unit . }}
        OPTIONAL {{ ?datapoint df:hasDataType ?datatype . }}
    }}
    """


# list instances
def build_connector_metadata_query() -> str:
    """
    Build a SPARQL query to retrieve metadata for all Connector instances,
    including label, address, moduleId, and moduleType.

    Returns:
        str: A SPARQL query string.
    """
    return """
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?connector ?label ?address ?moduleId ?moduleType
    WHERE {
      ?connector a df:Connector .
      OPTIONAL { ?connector rdfs:label ?label . }
      OPTIONAL { ?connector df:ConnectorAddress ?address . }
      OPTIONAL { ?connector df:moduleId ?moduleId . }
      OPTIONAL { ?connector df:moduleType ?moduleType . }
    }
    """


# multi-hop
def build_connector_datapoint_query() -> str:
    """
    Constructs a SPARQL query that retrieves all connectors along with
    their associated data points, including the connector ID,
    data point name, and data point identifier.

    Returns:
        str: A SPARQL query string.
    """
    return """
    PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?connectorId ?dataPointName ?dataPointIdentifier
    WHERE {
      # Find all connectors and get their module ID
      ?connector a df:Connector .
      ?connector df:moduleId ?connectorId .

      # Traverse to services connected to the connector
      ?connector df:connectedTo ?service .

      # Get the datapoints provided by each service
      ?service df:providesDataPoint ?datapoint .

      # Optionally retrieve datapoint metadata
      OPTIONAL { ?datapoint df:dataPointName ?dataPointName . }
      OPTIONAL { ?datapoint df:dataPointIdentifier ?dataPointIdentifier . }
    }
    ORDER BY ?connectorId ?dataPointName
    """
