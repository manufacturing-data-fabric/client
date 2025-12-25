import re

RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


def _wrap_uri(uri: str) -> str:
    """Wrap a full IRI in SPARQL angle brackets.

    If `uri` is already wrapped as `<...>`, it is returned unchanged.

    Args:
        uri: Full IRI string (e.g., "https://example.org/foo") or a string
            already wrapped as "<https://example.org/foo>".

    Returns:
        The IRI token safe for SPARQL usage, e.g. "<https://example.org/foo>".

    Raises:
        ValueError: If `uri` is not already wrapped and is not an absolute IRI
            starting with "http://" or "https://".
    """
    # uri may already be <...>
    if uri.startswith("<") and uri.endswith(">"):
        return uri
    # accept only full IRIs here
    if uri.startswith("http://") or uri.startswith("https://"):
        return f"<{uri}>"
    raise ValueError(f"_wrap_uri expects a full IRI, got: {uri}")


def _get_var_name(uri: str) -> str:
    """Derive a SPARQL variable name from a URI/IRI.

    The method extracts the local name (fragment after `#` or last path
    segment after `/`), normalizes it to a valid SPARQL variable identifier,
    and falls back to a stable placeholder if needed.

    Normalization rules:
      - Non-alphanumeric/underscore characters are replaced with underscores.
      - If the name does not start with a letter or underscore, it is prefixed
        with "v_".
      - If the extracted local name is empty, returns "v".

    Args:
        uri: Full IRI string or wrapped "<...>" form.

    Returns:
        A variable-safe local name, e.g. "connectedTo" for a property IRI.
    """
    # strip angle brackets if present
    if uri.startswith("<") and uri.endswith(">"):
        uri = uri[1:-1]
    local = uri.split("#")[-1].split("/")[-1]
    local = re.sub(r"[^A-Za-z0-9_]", "_", local)
    if not re.match(r"^[A-Za-z_]", local):
        local = f"v_{local}"
    return local or "v"


def _escape_literal(s: str) -> str:
    """Escape a Python string for safe use as a SPARQL string literal.

    This performs minimal escaping appropriate for embedding user-provided
    input into double-quoted SPARQL literals by escaping backslashes and
    double quotes.

    Args:
        s: Raw string to embed in a SPARQL literal.

    Returns:
        The escaped string suitable for insertion into `"..."` in SPARQL.
    """
    # minimal safe escaping for string literals
    return s.replace("\\", "\\\\").replace('"', '\\"')


class SPARQLBuilder:
    """Utility class for generating SPARQL queries from high-level intent.

    Supported query intents:
    - list_instances: list instances of a class with optional properties
    - get_properties: get all or selected properties of a given instance
    - search_entity: search entities by keyword in a given property (label by default)
    - get_related: follow a predicate to related entities (subject → object)
    - get_related_inverse: find subjects that point to a given object via a predicate
    """

    def __init__(self) -> None:
        """Initialize a SPARQLBuilder instance.

        The builder intentionally keeps `prefixes` empty because this implementation
        uses full IRIs (wrapped in angle brackets) in generated queries.

        Attributes:
            prefixes: SPARQL prefix declarations prepended to generated queries.
                Kept empty by default; may be set by callers if desired.
        """
        self.prefixes = ""
        # todo: think about this "emptyness" as with the agent, a proper namespace was necessary

    def get_all_classes_query(self) -> str:
        """Build a SPARQL query that lists unique RDF classes used in the dataset.

        The query returns distinct `?class` values from triples of the form
        `?s a ?class`. A LIMIT is applied to keep results bounded.

        Returns:
            A SPARQL query string that selects distinct classes as `?class`.
        """
        return (
            self.prefixes
            + """

        SELECT DISTINCT ?class WHERE {
            ?s a ?class .
        } LIMIT 100
        """
        )

    def build_list_instances_query(self, class_uri: str, optional_props: list[str] = None) -> str:
        """Build a SPARQL query to list instances of a class with optional columns.

        The result schema always includes canonical columns:
          - ?entityIri (instance IRI)
          - ?entityLabel (optional rdfs:label of the instance)
          - ?entityType (optional rdf:type of the instance)

        Additional properties may be requested via `optional_props`. Each requested
        property is added as an OPTIONAL pattern and selected as a variable derived
        from the property's local name.

        Args:
            class_uri: Full IRI of the RDF/OWL class whose instances should be
                listed.
            optional_props: Optional list of property IRIs to include as extra
                columns. Each will be retrieved via an OPTIONAL triple pattern.

        Returns:
            A SPARQL query string selecting distinct instances and requested
            optional values.

        Raises:
            ValueError: If `class_uri` or any property IRI in `optional_props` is
                not a full IRI (or already wrapped) accepted by `_wrap_uri()`.
        """
        optional_props = optional_props or []

        # canonical names for entity list outputs
        select_vars = ["?entityIri", "?entityLabel", "?entityType"]
        optional_blocks = [
            "OPTIONAL { ?entityIri rdfs:label ?entityLabel . }",
            "OPTIONAL { ?entityIri a ?entityType . }",
        ]

        # optional extra properties requested by the caller
        for prop in optional_props:
            var = _get_var_name(prop)  # e.g. df:connectedTo -> "connectedTo"
            optional_blocks.append(f"OPTIONAL {{ ?entityIri {_wrap_uri(prop)} ?{var} . }}")
            select_vars.append(f"?{var}")

        return f"""
        {self.prefixes}

        SELECT DISTINCT {" ".join(select_vars)} WHERE {{
            ?entityIri a {_wrap_uri(class_uri)} .
            {" ".join(optional_blocks)}
        }}
        """.strip()

    def build_get_properties_query(
        self, subject_uri: str, property_uris: list[str] | None = None
    ) -> str:
        """Build a SPARQL query to retrieve properties of a subject resource.

        The query returns triples about the given subject in a column-aligned
        format suitable for tabular processing. Each result row represents one
        triple and includes optional labels and rdf:types for both subject and
        object.

        Returned columns:
            - subjectIri:   IRI of the subject resource
            - subjectType:  rdf:type of the subject (optional)
            - predicate:    Predicate IRI of the triple
            - object:       Object of the triple (IRI or literal)
            - subjectLabel: rdfs:label of the subject (optional)
            - objectLabel:  rdfs:label of the object (optional)
            - objectType:   rdf:type of the object (optional)

        If `property_uris` is not provided or empty, all predicates associated
        with the subject are returned. If `property_uris` is provided, the query
        restricts results to those predicates using a VALUES clause.

        Args:
            subject_uri: Full IRI of the subject resource to describe.
            property_uris: Optional list of predicate IRIs to restrict the query to.
                If None or empty, all properties of the subject are returned.

        Returns:
            A SPARQL query string that retrieves the subject's properties along
            with optional labels and rdf:types.

        Raises:
            ValueError: If `subject_uri` or any entry in `property_uris` is not a
                full IRI (or already wrapped) accepted by `_wrap_uri()`.
        """
        subject_token = _wrap_uri(subject_uri)

        # Case 1: No predicate filter → return all triples about this subject
        if not property_uris:
            return (
                self.prefixes
                + f"""
            SELECT ?subjectIri ?subjectType ?predicate ?object
                   ?subjectLabel ?objectLabel ?objectType
            WHERE {{
              VALUES ?subjectIri {{ {subject_token} }}

              ?subjectIri ?predicate ?object .

              OPTIONAL {{ ?subjectIri rdfs:label ?subjectLabel }}
              OPTIONAL {{ ?subjectIri rdf:type   ?subjectType }}
              OPTIONAL {{ ?object    rdfs:label ?objectLabel }}
              OPTIONAL {{ ?object    rdf:type   ?objectType }}
            }}
            """.strip()
            )

        # Case 2: Restrict to selected predicates only
        values_props = " ".join(_wrap_uri(p) for p in property_uris)

        return (
            self.prefixes
            + f"""
        SELECT ?subjectIri ?subjectType ?predicate ?object ?subjectLabel ?objectLabel ?objectType
        WHERE {{
          VALUES ?subjectIri {{ {subject_token} }}
          VALUES ?predicate {{ {values_props} }}

          ?subjectIri ?predicate ?object .

          OPTIONAL {{ ?subjectIri rdfs:label ?subjectLabel }}
          OPTIONAL {{ ?subjectIri rdf:type   ?subjectType }}
          OPTIONAL {{ ?object    rdfs:label ?objectLabel }}
          OPTIONAL {{ ?object    rdf:type   ?objectType }}
        }}
        """.strip()
        )

    def build_search_entity_query(
        self,
        keyword: str,
        class_uri: str | None = None,
        property_uri: str | None = None,
        match_mode: str = "fuzzy",
    ) -> str:
        """Build a SPARQL query to search for entities by matching a literal value.

        The query searches for entities where the given `property_uri` contains
        (fuzzy) or equals (exact) the provided keyword, returning:

          - ?entityIri
          - ?matchLiteral  (the matched literal value)
          - ?matchScore    (currently constant 1.0; reserved for future scoring)
          - ?entityLabel   (optional rdfs:label)
          - ?entityType    (optional rdf:type)

        By default, the search uses rdfs:label as the match property. If `class_uri`
        is provided, the query adds a type constraint to restrict results to
        instances of that class.

        Note:
            This implementation includes a namespace filter that restricts results
            to IRIs starting with "http://stephantrattnig.org/instances#".

        Args:
            keyword: Search term to match against the selected property.
            class_uri: Optional class IRI to restrict matched entities by rdf:type.
            property_uri: Optional property IRI to match against. Defaults to
                rdfs:label.
            match_mode: Match strategy, either "fuzzy" (case-insensitive substring
                match via CONTAINS/LCASE) or "exact" (string equality).

        Returns:
            A SPARQL query string that selects matching entities and metadata.

        Raises:
            ValueError: If `match_mode` is not "fuzzy" or "exact".
            ValueError: If `class_uri` or `property_uri` is provided and is not a
                full IRI (or already wrapped) accepted by `_wrap_uri()`.
        """
        # Default to rdfs:label (full IRI)
        prop = property_uri or RDFS_LABEL
        kw = _escape_literal(keyword)

        class_filter = f"?entityIri a {_wrap_uri(class_uri)} .\n" if class_uri else ""

        # --- MATCH CLAUSE ---------------------------------------------------------
        if match_mode == "fuzzy":
            filter_clause = (
                f"?entityIri {_wrap_uri(prop)} ?matchLiteral .\n"
                f'FILTER(CONTAINS(LCASE(STR(?matchLiteral)), LCASE("{kw}")))'
            )
            # SPARQL cannot compute a fuzzy similarity – set matchScore = 1.0
            score_binding = "BIND(1.0 AS ?matchScore)"
        elif match_mode == "exact":
            filter_clause = (
                f'?entityIri {_wrap_uri(prop)} ?matchLiteral .\nFILTER(STR(?matchLiteral) = "{kw}")'
            )
            score_binding = "BIND(1.0 AS ?matchScore)"
        else:
            raise ValueError("match_mode must be 'fuzzy' or 'exact'.")

        # --- FINAL QUERY ----------------------------------------------------------
        return (
            self.prefixes
            + f"""
        SELECT DISTINCT ?entityIri ?matchLiteral ?matchScore ?entityLabel ?entityType WHERE {{
            {class_filter}
            {filter_clause}
            {score_binding}
            OPTIONAL {{ ?entityIri {_wrap_uri(RDFS_LABEL)} ?entityLabel . }}
            OPTIONAL {{ ?entityIri rdf:type ?entityType . }}
            
        # Instance-only filter by namespace
        FILTER STRSTARTS(STR(?entityIri), "http://stephantrattnig.org/instances#")
        }}
        LIMIT 50
        """.strip()
        )

    def build_get_related_query(self, subject_uri: str, predicate_uri: str) -> str:
        """Build a SPARQL query to follow a predicate from subject to related objects.

        The query returns a row per matching triple `subject_uri predicate_uri ?object`
        along with optional labels and rdf:types for both subject and object:

          - ?subjectIri, ?subjectLabel, ?subjectType
          - ?predicate
          - ?object, ?objectLabel, ?objectType

        Args:
            subject_uri: Full IRI of the subject resource.
            predicate_uri: Full IRI of the predicate to follow.

        Returns:
            A SPARQL query string that selects related objects and optional metadata.

        Raises:
            ValueError: If `subject_uri` or `predicate_uri` is not a full IRI (or
                already wrapped) accepted by `_wrap_uri()`.
        """
        return (
            self.prefixes
            + f"""
        SELECT
            ?subjectIri ?subjectLabel ?subjectType
            ?predicate
            ?object ?objectLabel ?objectType
        WHERE {{
            BIND({_wrap_uri(subject_uri)} AS ?subjectIri)
            BIND({_wrap_uri(predicate_uri)} AS ?predicate)

            # subject label & type
            OPTIONAL {{ ?subjectIri {_wrap_uri(RDFS_LABEL)} ?subjectLabel . }}
            OPTIONAL {{ ?subjectIri rdf:type ?subjectType . }}

            # actual relation
            ?subjectIri ?predicate ?object .

            # object label & type
            OPTIONAL {{ ?object {_wrap_uri(RDFS_LABEL)} ?objectLabel . }}
            OPTIONAL {{ ?object rdf:type ?objectType . }}
        }}
        """.strip()
        )

    def build_get_related_inverse_query(
        self,
        object_uri: str,
        predicate_uri: str,
        optional_props: list[str] = None,
    ) -> str:
        """Build a SPARQL query to find subjects that point to an object via a predicate.

        This is the inverse of `build_get_related_query`: it finds triples of the form
        `?subjectIri predicate_uri object_uri` and returns aligned columns:

          - ?subjectIri, ?subjectLabel, ?subjectType
          - ?predicate
          - ?object, ?objectLabel, ?objectType

        Additional subject properties may be requested via `optional_props`. Each
        requested property is added as an OPTIONAL pattern and selected as a variable
        derived from the property's local name.

        Args:
            object_uri: Full IRI of the object resource (the target of incoming links).
            predicate_uri: Full IRI of the predicate to match incoming links.
            optional_props: Optional list of additional subject property IRIs to
                include as extra columns.

        Returns:
            A SPARQL query string that selects matching subjects and optional metadata.

        Raises:
            ValueError: If `object_uri`, `predicate_uri`, or any IRI in
                `optional_props` is not a full IRI (or already wrapped) accepted by
                `_wrap_uri()`.
        """
        optional_props = optional_props or []

        # Add object label/type so output aligns with get_related
        select_vars = (
            "?subjectIri ?subjectLabel ?subjectType ?predicate ?object ?objectLabel ?objectType"
        )

        optional_clauses = """
            OPTIONAL { ?subjectIri rdfs:label ?subjectLabel . }
            OPTIONAL { ?subjectIri rdf:type ?subjectType . }

            OPTIONAL { ?object rdfs:label ?objectLabel . }
            OPTIONAL { ?object rdf:type ?objectType . }
        """

        # optional additional subject properties
        for prop in optional_props:
            var = _get_var_name(prop)
            select_vars += f" ?{var}"
            optional_clauses += f"OPTIONAL {{ ?subjectIri {_wrap_uri(prop)} ?{var} . }}\n"

        return (
            self.prefixes
            + f"""
        SELECT DISTINCT {select_vars} WHERE {{
            ?subjectIri {_wrap_uri(predicate_uri)} {_wrap_uri(object_uri)} .

            # Bind predicate + object so they appear as columns
            BIND({_wrap_uri(predicate_uri)} AS ?predicate)
            BIND({_wrap_uri(object_uri)} AS ?object)

            {optional_clauses}
        }}
        """.strip()
        )
