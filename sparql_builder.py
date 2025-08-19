
class SPARQLBuilder:
    """
    Utility class for generating SPARQL queries from high-level intent.

    Supported query intents:
    - list_instances: list instances of a class with optional properties
    - get_properties: get all or selected properties of a given instance
    - search_entity: search entities by keyword in a given property (label by default)
    - get_related: follow a predicate to related entities (subject → object)
    - get_related_inverse: find subjects that point to a given object via a predicate
    """

    def __init__(self):
        # Define all standard prefixes used in the queries
        self.prefixes = """
        PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX df_instance: <http://stephantrattnig.org/instances#>
        """.strip()

    def _wrap_uri(self, uri: str) -> str:
        """
        Wraps identifiers into full URIs if they are not prefixed names or already complete URIs.
        Assumes local identifiers (e.g., UUIDs) should be resolved against df_instance: prefix.
        """
        if uri.startswith("http://") or uri.startswith("https://") or ":" in uri:
            return f"<{uri}>" if uri.startswith("http") else uri
        # Assume local ID, wrap using df_instance namespace
        return f"<http://stephantrattnig.org/instances#{uri}>"


    def _get_var_name(self, uri: str) -> str:
        """
        Extracts a variable name from a URI or prefixed name.
        Example: df:label → label, http://.../label → label
        """
        if ":" in uri:
            return uri.split(":")[-1]
        if "#" in uri:
            return uri.split("#")[-1]
        return uri.split("/")[-1]

    def get_all_classes(self):
        """
        Returns a query to list all unique RDF classes used in the dataset.
        """
        return self.prefixes + """

        SELECT DISTINCT ?class WHERE {
            ?s a ?class .
        } LIMIT 100
        """

    def build_list_instances_query(self, class_uri: str, optional_props: list[str] = None):
        """
        Build a query to list all instances of a given class, optionally including properties.

        Args:
            class_uri: the class to filter on (e.g., df:DataPoint)
            optional_props: list of property URIs to return if available

        Returns:
            SPARQL query string
        """
        optional_props = optional_props or []
        select_vars = "?instance"
        optional_blocks = ""

        for prop in optional_props:
            var = self._get_var_name(prop)
            select_vars += f" ?{var}"
            optional_blocks += f"OPTIONAL {{ ?instance {self._wrap_uri(prop)} ?{var} . }}\n"

        return f"""
        {self.prefixes}

        SELECT {select_vars} WHERE {{
            ?instance a {self._wrap_uri(class_uri)} .
            {optional_blocks}
        }}
        """.strip()

    def build_get_properties_query(self, subject_uri: str, property_uris: list[str] = None):
        """
        Build a query to retrieve all or selected properties of a subject.

        Args:
            subject_uri: full URI or prefixed name of the entity
            property_uris: list of specific property URIs to fetch, or None for all

        Returns:
            SPARQL query string
        """
        if property_uris is None:
            return self.prefixes + f"""
            SELECT ?p ?o WHERE {{
              {self._wrap_uri(subject_uri)} ?p ?o .
            }}
            """

        query_body = f"VALUES ?s {{ {self._wrap_uri(subject_uri)} }}\n"
        select_vars = []

        for prop in property_uris:
            var = self._get_var_name(prop)
            query_body += f"OPTIONAL {{ ?s {self._wrap_uri(prop)} ?{var} . }}\n"
            select_vars.append(f"?{var}")

        return self.prefixes + f"""
        SELECT ?s {' '.join(select_vars)} WHERE {{
            {query_body}
        }}
        """.strip()

    def build_search_entity_query(self, keyword: str, class_uri: str = None,
                                  property_uri: str = "rdfs:label", match_mode: str = "fuzzy"):
        """
        Build a query to search for entities by keyword in a label or other property.

        Args:
            keyword: search string
            class_uri: optional RDF class restriction
            property_uri: property to search (e.g. rdfs:label or df:deviceIdentifier)
            match_mode: 'fuzzy' for contains, 'exact' for exact match

        Returns:
            SPARQL query string
        """
        class_filter = f"?instance a {self._wrap_uri(class_uri)} ." if class_uri else ""

        if match_mode == "fuzzy":
            filter_clause = f"""
                ?instance {self._wrap_uri(property_uri)} ?label .
                FILTER(CONTAINS(LCASE(STR(?label)), LCASE(\"{keyword}\")))
            """
        elif match_mode == "exact":
            filter_clause = f"?instance {self._wrap_uri(property_uri)} \"{keyword}\" ."
        else:
            raise ValueError("match_mode must be 'fuzzy' or 'exact'.")

        return self.prefixes + f"""
        SELECT DISTINCT ?instance ?label WHERE {{
            {class_filter}
            {filter_clause}
            OPTIONAL {{ ?instance rdfs:label ?label }}
        }}
        LIMIT 50
        """.strip()

    def build_get_related_query(self, subject_uri: str, predicate_uri: str):
        """
        Build a query to get objects linked by a property from a subject.

        Args:
            subject_uri: URI of the subject entity
            predicate_uri: URI of the outgoing predicate

        Returns:
            SPARQL query string
        """
        return self.prefixes + f"""
        SELECT ?object ?label WHERE {{
            {self._wrap_uri(subject_uri)} {self._wrap_uri(predicate_uri)} ?object .
            OPTIONAL {{ ?object rdfs:label ?label }}
        }}
        """.strip()

    def build_get_related_inverse_query(self, object_uri: str, predicate_uri: str,
                                        optional_props: list[str] = None):
        """
        Build a query to find subjects that reference an object via a predicate.

        Args:
            object_uri: URI of the object
            predicate_uri: property URI used in reverse
            optional_props: additional properties to return for the subject

        Returns:
            SPARQL query string
        """
        optional_props = optional_props or []
        select_vars = "?subject ?label"
        optional_clauses = "OPTIONAL { ?subject rdfs:label ?label }\n"

        for prop in optional_props:
            var = self._get_var_name(prop)
            select_vars += f" ?{var}"
            optional_clauses += f"OPTIONAL {{ ?subject {self._wrap_uri(prop)} ?{var} }}\n"

        return self.prefixes + f"""
        SELECT {select_vars} WHERE {{
            ?subject {self._wrap_uri(predicate_uri)} {self._wrap_uri(object_uri)} .
            {optional_clauses}
        }}
        """.strip()







    #### old deprecated methods - todo: delete

    def build_list_instances_query_old(self, class_uri: str, optional_props: list[str] = None):
        """
        Build a SPARQL query to list instances of a given class and optionally include metadata.

        Args:
            class_uri (str): The URI of the class (e.g., df:Device).
            optional_props (list[str]): List of property URIs to include as OPTIONAL fields.

        Returns:
            str: A SPARQL query string.
        """
        optional_props = optional_props or []

        prefixes = self.prefixes

        select_vars = "?instance"
        optional_blocks = ""

        for prop_uri in optional_props:
            var_name = prop_uri.split(":")[-1] if ":" in prop_uri else prop_uri.split("#")[-1]
            select_vars += f" ?{var_name}"
            optional_blocks += f"OPTIONAL {{ ?instance {prop_uri} ?{var_name} . }}\n"

        query = f"""
        {prefixes}

        SELECT {select_vars}
        WHERE {{
            ?instance a {class_uri} .
            {optional_blocks}
        }}
        """
        return query.strip()

    def build_get_properties_query_old(self, subject_uri: str, property_uris: list[str] = None) -> str:
        prefix = self.prefixes

        if property_uris is None:
            return prefix + f"""
        SELECT ?p ?o WHERE {{
          <{subject_uri}> ?p ?o .
        }}
        """

        query_body = f"VALUES ?s {{ <{subject_uri}> }}\n"
        select_vars = []

        for prop in property_uris:
            if prop.startswith("?"):
                raise ValueError(f"Invalid property name: {prop}")
            # Handle prefixed or full URI forms
            if ":" in prop and not prop.startswith("http"):
                # prefixed name like df:hasUnit
                var = prop.split(":")[-1]
                query_body += f"OPTIONAL {{ ?s {prop} ?{var} . }}\n"
            elif prop.startswith("http"):
                # full URI
                var = prop.split("#")[-1] if "#" in prop else prop.split("/")[-1]
                query_body += f"OPTIONAL {{ ?s <{prop}> ?{var} . }}\n"
            else:
                raise ValueError(f"Unsupported property URI format: {prop}")

            select_vars.append(f"?{var}")

        return prefix + f"""
        SELECT ?s {' '.join(select_vars)}
        WHERE {{
          {query_body}
        }}
        """


    def search_entity_query_old(
        self,
        keyword: str,
        class_uri: str = None,
        property_uri: str = "rdfs:label",
        match_mode: str = "fuzzy"  # or "exact"
    ) -> str:
        """
        Build a flexible SPARQL query to search for entities by keyword.

        Supports:
          - Fuzzy search on rdfs:label (default)
          - Exact match on any property
          - Optional class restriction

        Args:
            keyword (str): The search term (either fuzzy keyword or exact value).
            class_uri (str, optional): Restrict results to instances of this class (e.g., df:Device).
            property_uri (str): The property to search (e.g., rdfs:label, df:deviceIdentifier).
            match_mode (str): "fuzzy" for partial match, "exact" for literal match.

        Returns:
            str: SPARQL query string.
        """
        prefixes = """
            PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        """

        class_filter = f"?instance a {class_uri} ." if class_uri else ""

        if match_mode == "fuzzy":
            filter_clause = f"""
                ?instance {property_uri} ?label .
                FILTER(CONTAINS(LCASE(STR(?label)), LCASE("{keyword}")))
            """
        elif match_mode == "exact":
            filter_clause = f"""
                ?instance {property_uri} "{keyword}" .
            """
        else:
            raise ValueError("match_mode must be 'fuzzy' or 'exact'.")

        return prefixes + f"""
            SELECT DISTINCT ?instance ?label WHERE {{
                {class_filter}
                {filter_clause}
                OPTIONAL {{ ?instance rdfs:label ?label }}
            }}
            LIMIT 50
        """.strip()

    def build_get_related_query_old(self, subject_uri: str, predicate_uri: str) -> str:
        """
        Build a SPARQL query to get related entities via an outgoing property.

        Args:
            subject_uri (str): The URI of the subject entity (e.g., a Device).
            predicate_uri (str): The URI of the property to follow (e.g., df:hasDataPoint).

        Returns:
            str: A SPARQL query string returning related objects and their labels.
        """
        prefixes = """
        PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        """

        query = f"""
        {prefixes}
        SELECT ?object ?label WHERE {{
            <{subject_uri}> <{predicate_uri}> ?object .
            OPTIONAL {{ ?object rdfs:label ?label }}
        }}
        """
        return query.strip()

    def build_get_related_inverse_query_old(
        self,
        object_uri: str,
        predicate_uri: str,
        optional_props: list[str] = None
    ) -> str:
        prefixes = """
        PREFIX df: <http://stephantrattnig.org/data_fabric_ontology#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        """

        optional_props = optional_props or []
        select_vars = "?subject ?label"
        optional_clauses = "OPTIONAL { ?subject rdfs:label ?label }\n"

        for prop in optional_props:
            var_name = prop.split("#")[-1] if "#" in prop else prop.split("/")[-1]
            select_vars += f" ?{var_name}"
            optional_clauses += f"OPTIONAL {{ ?subject <{prop}> ?{var_name} }}\n"

        return f"""
        {prefixes}
        SELECT {select_vars} WHERE {{
            ?subject <{predicate_uri}> <{object_uri}> .
            {optional_clauses}
        }}
        """.strip()


################ Old ##################


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
    device_filter = f'?device df:deviceIdentifier "{device_identifier}" .' if device_identifier else '?device df:deviceIdentifier ?identifier .'
    service_filter = f'FILTER (?service = <{service_uri}>)' if service_uri else ''

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
        else '?device df:deviceIdentifier ?identifier .'
    )
    service_filter = f'FILTER (?service = <{service_uri}>)' if service_uri else ''

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
    connector_uri: str = None,
    topic_type_uri: str = None,
    device_identifier: str = None
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
