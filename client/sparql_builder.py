import re
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

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
        # keep empty; we use full IRIs everywhere
        self.prefixes = ""

    def _wrap_uri(self, uri: str) -> str:
        # uri may already be <...>
        if uri.startswith("<") and uri.endswith(">"):
            return uri
        # accept only full IRIs here
        if uri.startswith("http://") or uri.startswith("https://"):
            return f"<{uri}>"
        raise ValueError(f"_wrap_uri expects a full IRI, got: {uri}")

    def _get_var_name(self, uri: str) -> str:
        # strip angle brackets if present
        if uri.startswith("<") and uri.endswith(">"):
            uri = uri[1:-1]
        local = uri.split('#')[-1].split('/')[-1]
        local = re.sub(r'[^A-Za-z0-9_]', '_', local)
        if not re.match(r'^[A-Za-z_]', local):
            local = f"v_{local}"
        return local or "v"

    def _escape_literal(self, s: str) -> str:
        # minimal safe escaping for string literals
        return s.replace("\\", "\\\\").replace("\"", "\\\"")

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
        optional_props = optional_props or []

        # canonical names for entity list outputs
        select_vars = ["?entityIri", "?entityLabel", "?entityType"]
        optional_blocks = [
            "OPTIONAL { ?entityIri rdfs:label ?entityLabel . }",
            "OPTIONAL { ?entityIri a ?entityType . }"
        ]

        # optional extra properties requested by the caller
        for prop in optional_props:
            var = self._get_var_name(prop)  # e.g. df:connectedTo -> "connectedTo"
            optional_blocks.append(
                f"OPTIONAL {{ ?entityIri {self._wrap_uri(prop)} ?{var} . }}"
            )
            select_vars.append(f"?{var}")

        return f"""
        {self.prefixes}

        SELECT DISTINCT {' '.join(select_vars)} WHERE {{
            ?entityIri a {self._wrap_uri(class_uri)} .
            {' '.join(optional_blocks)}
        }}
        """.strip()

    def build_get_properties_query(
            self,
            subject_uri: str,
            property_uris: list[str] | None = None
    ) -> str:
        """
        Build a SPARQL query that returns all (or selected) properties of a subject
        as triples (?subjectIri ?predicate ?object), including:

        - ?subjectLabel   (optional, rdfs:label of subject)
        - ?subjectType    (optional, rdf:type  of subject)
        - ?objectLabel    (optional, rdfs:label of object)
        - ?objectType     (optional, rdf:type   of object)

        Column alignment for the DataFrame:
          - subjectIri
          - subjectType
          - predicate
          - object
          - subjectLabel
          - objectLabel
          - objectType
        """

        subject_token = self._wrap_uri(subject_uri)

        # Case 1: No predicate filter → return all triples about this subject
        if not property_uris:
            return self.prefixes + f"""
            SELECT ?subjectIri ?subjectType ?predicate ?object ?subjectLabel ?objectLabel ?objectType
            WHERE {{
              VALUES ?subjectIri {{ {subject_token} }}

              ?subjectIri ?predicate ?object .

              OPTIONAL {{ ?subjectIri rdfs:label ?subjectLabel }}
              OPTIONAL {{ ?subjectIri rdf:type   ?subjectType }}
              OPTIONAL {{ ?object    rdfs:label ?objectLabel }}
              OPTIONAL {{ ?object    rdf:type   ?objectType }}
            }}
            """.strip()

        # Case 2: Restrict to selected predicates only
        values_props = " ".join(self._wrap_uri(p) for p in property_uris)

        return self.prefixes + f"""
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

    def build_search_entity_query(
            self,
            keyword: str,
            class_uri: str | None = None,
            property_uri: str | None = None,
            match_mode: str = "fuzzy",
    ):
        # Default to rdfs:label (full IRI)
        prop = property_uri or RDFS_LABEL
        kw = self._escape_literal(keyword)

        class_filter = (
            f"?entityIri a {self._wrap_uri(class_uri)} .\n" if class_uri else ""
        )

        # --- MATCH CLAUSE ---------------------------------------------------------
        if match_mode == "fuzzy":
            filter_clause = (
                f"?entityIri {self._wrap_uri(prop)} ?matchLiteral .\n"
                f"FILTER(CONTAINS(LCASE(STR(?matchLiteral)), LCASE(\"{kw}\")))"
            )
            # SPARQL cannot compute a fuzzy similarity – set matchScore = 1.0
            score_binding = "BIND(1.0 AS ?matchScore)"
        elif match_mode == "exact":
            filter_clause = (
                f"?entityIri {self._wrap_uri(prop)} ?matchLiteral .\n"
                f"FILTER(STR(?matchLiteral) = \"{kw}\")"
            )
            score_binding = "BIND(1.0 AS ?matchScore)"
        else:
            raise ValueError("match_mode must be 'fuzzy' or 'exact'.")

        # --- FINAL QUERY ----------------------------------------------------------
        return self.prefixes + f"""
        SELECT DISTINCT ?entityIri ?matchLiteral ?matchScore ?entityLabel ?entityType WHERE {{
            {class_filter}
            {filter_clause}
            {score_binding}
            OPTIONAL {{ ?entityIri {self._wrap_uri(RDFS_LABEL)} ?entityLabel . }}
            OPTIONAL {{ ?entityIri rdf:type ?entityType . }}
            
        # Instance-only filter by namespace
        FILTER STRSTARTS(STR(?entityIri), "http://stephantrattnig.org/instances#")
        }}
        LIMIT 50
        """.strip()

    def build_get_related_query(self, subject_uri: str, predicate_uri: str):
        return self.prefixes + f"""
        SELECT
            ?subjectIri ?subjectLabel ?subjectType
            ?predicate
            ?object ?objectLabel ?objectType
        WHERE {{
            BIND({self._wrap_uri(subject_uri)} AS ?subjectIri)
            BIND({self._wrap_uri(predicate_uri)} AS ?predicate)

            # subject label & type
            OPTIONAL {{ ?subjectIri {self._wrap_uri(RDFS_LABEL)} ?subjectLabel . }}
            OPTIONAL {{ ?subjectIri rdf:type ?subjectType . }}

            # actual relation
            ?subjectIri ?predicate ?object .

            # object label & type
            OPTIONAL {{ ?object {self._wrap_uri(RDFS_LABEL)} ?objectLabel . }}
            OPTIONAL {{ ?object rdf:type ?objectType . }}
        }}
        """.strip()

    def build_get_related_inverse_query(
            self,
            object_uri: str,
            predicate_uri: str,
            optional_props: list[str] = None,
    ):
        optional_props = optional_props or []

        # Add object label/type so output aligns with get_related
        select_vars = (
            "?subjectIri ?subjectLabel ?subjectType "
            "?predicate ?object ?objectLabel ?objectType"
        )

        optional_clauses = """
            OPTIONAL { ?subjectIri rdfs:label ?subjectLabel . }
            OPTIONAL { ?subjectIri rdf:type ?subjectType . }

            OPTIONAL { ?object rdfs:label ?objectLabel . }
            OPTIONAL { ?object rdf:type ?objectType . }
        """

        # optional additional subject properties
        for prop in optional_props:
            var = self._get_var_name(prop)
            select_vars += f" ?{var}"
            optional_clauses += (
                f"OPTIONAL {{ ?subjectIri {self._wrap_uri(prop)} ?{var} . }}\n"
            )

        return self.prefixes + f"""
        SELECT DISTINCT {select_vars} WHERE {{
            ?subjectIri {self._wrap_uri(predicate_uri)} {self._wrap_uri(object_uri)} .

            # Bind predicate + object so they appear as columns
            BIND({self._wrap_uri(predicate_uri)} AS ?predicate)
            BIND({self._wrap_uri(object_uri)} AS ?object)

            {optional_clauses}
        }}
        """.strip()
