"""Unit tests for the SPARQL builder module.

These tests are pure unit tests with no external dependencies.
"""

import pytest

from client.sparql_builder import SPARQLBuilder, _escape_literal, _get_var_name, _wrap_uri


class TestWrapUri:
    """Tests for _wrap_uri helper function."""

    def test_wrap_uri_http_prefix(self):
        """URI with http:// prefix should be wrapped in angle brackets."""
        result = _wrap_uri("http://example.org/foo")
        assert result == "<http://example.org/foo>"

    def test_wrap_uri_https_prefix(self):
        """URI with https:// prefix should be wrapped in angle brackets."""
        result = _wrap_uri("https://example.org/foo")
        assert result == "<https://example.org/foo>"

    def test_wrap_uri_already_wrapped(self):
        """Already wrapped URI should be returned unchanged."""
        result = _wrap_uri("<http://example.org/foo>")
        assert result == "<http://example.org/foo>"

    def test_wrap_uri_invalid_raises_valueerror(self):
        """Non-IRI string should raise ValueError."""
        with pytest.raises(ValueError, match="expects a full IRI"):
            _wrap_uri("not_a_uri")

    def test_wrap_uri_relative_path_raises_valueerror(self):
        """Relative path should raise ValueError."""
        with pytest.raises(ValueError, match="expects a full IRI"):
            _wrap_uri("/relative/path")

    def test_wrap_uri_prefixed_name_raises_valueerror(self):
        """Prefixed name (e.g., 'rdf:type') should raise ValueError."""
        with pytest.raises(ValueError, match="expects a full IRI"):
            _wrap_uri("rdf:type")


class TestGetVarName:
    """Tests for _get_var_name helper function."""

    def test_get_var_name_fragment(self):
        """Extract local name from fragment identifier."""
        result = _get_var_name("http://example.org/ontology#connectedTo")
        assert result == "connectedTo"

    def test_get_var_name_path_segment(self):
        """Extract local name from last path segment."""
        result = _get_var_name("http://example.org/path/localName")
        assert result == "localName"

    def test_get_var_name_wrapped_uri(self):
        """Handle already wrapped URIs."""
        result = _get_var_name("<http://example.org/ontology#myProperty>")
        assert result == "myProperty"

    def test_get_var_name_special_chars_replaced(self):
        """Special characters should be replaced with underscores."""
        result = _get_var_name("http://example.org/my-property.name")
        assert result == "my_property_name"

    def test_get_var_name_starts_with_number(self):
        """Names starting with numbers should be prefixed with v_."""
        result = _get_var_name("http://example.org/123abc")
        assert result == "v_123abc"

    def test_get_var_name_empty_local_returns_v_prefix(self):
        """Empty local name should return 'v_' (prefixed empty string)."""
        result = _get_var_name("http://example.org/")
        assert result == "v_"

    def test_get_var_name_starts_with_underscore(self):
        """Names starting with underscore should be valid."""
        result = _get_var_name("http://example.org/_private")
        assert result == "_private"


class TestEscapeLiteral:
    """Tests for _escape_literal helper function."""

    def test_escape_backslash(self):
        """Backslashes should be escaped."""
        result = _escape_literal("path\\to\\file")
        assert result == "path\\\\to\\\\file"

    def test_escape_double_quotes(self):
        """Double quotes should be escaped."""
        result = _escape_literal('He said "hello"')
        assert result == 'He said \\"hello\\"'

    def test_escape_combined(self):
        """Combined escaping of backslashes and quotes."""
        result = _escape_literal('path\\to\\"file"')
        assert result == 'path\\\\to\\\\\\"file\\"'

    def test_escape_empty_string(self):
        """Empty string should return empty string."""
        result = _escape_literal("")
        assert result == ""

    def test_escape_no_special_chars(self):
        """String without special chars should be unchanged."""
        result = _escape_literal("simple text")
        assert result == "simple text"


class TestSPARQLBuilderInit:
    """Tests for SPARQLBuilder initialization."""

    def test_init_empty_prefixes(self):
        """Builder should initialize with empty prefixes."""
        builder = SPARQLBuilder()
        assert builder.prefixes == ""


class TestGetAllClassesQuery:
    """Tests for get_all_classes_query method."""

    def test_get_all_classes_query_structure(self):
        """Query should contain SELECT DISTINCT ?class."""
        builder = SPARQLBuilder()
        query = builder.get_all_classes_query()
        assert "SELECT DISTINCT ?class" in query

    def test_get_all_classes_query_triple_pattern(self):
        """Query should contain ?s a ?class pattern."""
        builder = SPARQLBuilder()
        query = builder.get_all_classes_query()
        assert "?s a ?class" in query

    def test_get_all_classes_query_limit(self):
        """Query should contain LIMIT 100."""
        builder = SPARQLBuilder()
        query = builder.get_all_classes_query()
        assert "LIMIT 100" in query


class TestBuildListInstancesQuery:
    """Tests for build_list_instances_query method."""

    def test_list_instances_basic(self):
        """Basic query should contain class URI and canonical variables."""
        builder = SPARQLBuilder()
        query = builder.build_list_instances_query("http://example.org/MyClass")

        assert "<http://example.org/MyClass>" in query
        assert "?entityIri" in query
        assert "?entityLabel" in query
        assert "?entityType" in query

    def test_list_instances_with_optional_props(self):
        """Optional properties should be included in query."""
        builder = SPARQLBuilder()
        query = builder.build_list_instances_query(
            "http://example.org/MyClass",
            optional_props=["http://example.org/propA", "http://example.org/propB"],
        )

        assert "?propA" in query
        assert "?propB" in query
        assert "OPTIONAL { ?entityIri <http://example.org/propA> ?propA" in query
        assert "OPTIONAL { ?entityIri <http://example.org/propB> ?propB" in query

    def test_list_instances_invalid_uri_raises(self):
        """Invalid class URI should raise ValueError."""
        builder = SPARQLBuilder()
        with pytest.raises(ValueError, match="expects a full IRI"):
            builder.build_list_instances_query("InvalidClass")

    def test_list_instances_invalid_prop_raises(self):
        """Invalid property URI should raise ValueError."""
        builder = SPARQLBuilder()
        with pytest.raises(ValueError, match="expects a full IRI"):
            builder.build_list_instances_query(
                "http://example.org/MyClass", optional_props=["invalidProp"]
            )

    def test_list_instances_empty_optional_props(self):
        """Empty optional_props list should work like None."""
        builder = SPARQLBuilder()
        query = builder.build_list_instances_query("http://example.org/MyClass", optional_props=[])
        assert "<http://example.org/MyClass>" in query


class TestBuildGetPropertiesQuery:
    """Tests for build_get_properties_query method."""

    def test_get_properties_all(self):
        """Query without filter should return all properties."""
        builder = SPARQLBuilder()
        query = builder.build_get_properties_query("http://example.org/Subject1")

        assert "<http://example.org/Subject1>" in query
        assert "?predicate" in query
        assert "?object" in query
        assert "VALUES ?predicate" not in query

    def test_get_properties_filtered(self):
        """Query with filter should include VALUES clause."""
        builder = SPARQLBuilder()
        query = builder.build_get_properties_query(
            "http://example.org/Subject1",
            property_uris=["http://example.org/prop1", "http://example.org/prop2"],
        )

        assert "VALUES ?predicate" in query
        assert "<http://example.org/prop1>" in query
        assert "<http://example.org/prop2>" in query

    def test_get_properties_includes_labels(self):
        """Query should include optional label patterns."""
        builder = SPARQLBuilder()
        query = builder.build_get_properties_query("http://example.org/Subject1")

        assert "?subjectLabel" in query
        assert "?objectLabel" in query

    def test_get_properties_includes_types(self):
        """Query should include optional type patterns."""
        builder = SPARQLBuilder()
        query = builder.build_get_properties_query("http://example.org/Subject1")

        assert "?subjectType" in query
        assert "?objectType" in query


class TestBuildSearchEntityQuery:
    """Tests for build_search_entity_query method."""

    def test_search_entity_fuzzy(self):
        """Fuzzy search should use CONTAINS and LCASE."""
        builder = SPARQLBuilder()
        query = builder.build_search_entity_query("test", match_mode="fuzzy")

        assert "CONTAINS(LCASE" in query
        assert "test" in query

    def test_search_entity_exact(self):
        """Exact search should use string equality."""
        builder = SPARQLBuilder()
        query = builder.build_search_entity_query("test", match_mode="exact")

        assert 'STR(?matchLiteral) = "test"' in query

    def test_search_entity_with_class_filter(self):
        """Query with class filter should include type constraint."""
        builder = SPARQLBuilder()
        query = builder.build_search_entity_query("test", class_uri="http://example.org/MyClass")

        assert "?entityIri a <http://example.org/MyClass>" in query

    def test_search_entity_invalid_mode_raises(self):
        """Invalid match_mode should raise ValueError."""
        builder = SPARQLBuilder()
        with pytest.raises(ValueError, match="must be 'fuzzy' or 'exact'"):
            builder.build_search_entity_query("test", match_mode="invalid")

    def test_search_entity_escapes_keyword(self):
        """Special characters in keyword should be escaped."""
        builder = SPARQLBuilder()
        query = builder.build_search_entity_query('test"quote')

        assert '\\"' in query

    def test_search_entity_includes_score(self):
        """Query should include matchScore binding."""
        builder = SPARQLBuilder()
        query = builder.build_search_entity_query("test")

        assert "?matchScore" in query
        assert "BIND(1.0 AS ?matchScore)" in query

    def test_search_entity_limit(self):
        """Query should have LIMIT 50."""
        builder = SPARQLBuilder()
        query = builder.build_search_entity_query("test")

        assert "LIMIT 50" in query


class TestBuildGetRelatedQuery:
    """Tests for build_get_related_query method."""

    def test_get_related_structure(self):
        """Query should have correct structure."""
        builder = SPARQLBuilder()
        query = builder.build_get_related_query(
            "http://example.org/Subject1", "http://example.org/relatesTo"
        )

        assert "<http://example.org/Subject1>" in query
        assert "<http://example.org/relatesTo>" in query
        assert "?object" in query

    def test_get_related_includes_bindings(self):
        """Query should include BIND statements."""
        builder = SPARQLBuilder()
        query = builder.build_get_related_query(
            "http://example.org/Subject1", "http://example.org/relatesTo"
        )

        assert "BIND(" in query
        assert "?subjectIri" in query
        assert "?predicate" in query

    def test_get_related_includes_labels(self):
        """Query should include optional labels."""
        builder = SPARQLBuilder()
        query = builder.build_get_related_query(
            "http://example.org/Subject1", "http://example.org/relatesTo"
        )

        assert "?subjectLabel" in query
        assert "?objectLabel" in query


class TestBuildGetRelatedInverseQuery:
    """Tests for build_get_related_inverse_query method."""

    def test_get_related_inverse_basic(self):
        """Basic inverse query should have correct triple pattern."""
        builder = SPARQLBuilder()
        query = builder.build_get_related_inverse_query(
            "http://example.org/Object1", "http://example.org/relatesTo"
        )

        # Triple pattern: ?subjectIri <predicate> <object>
        assert "?subjectIri <http://example.org/relatesTo> <http://example.org/Object1>" in query

    def test_get_related_inverse_with_optional_props(self):
        """Optional props should add extra variables."""
        builder = SPARQLBuilder()
        query = builder.build_get_related_inverse_query(
            "http://example.org/Object1",
            "http://example.org/relatesTo",
            optional_props=["http://example.org/extraProp"],
        )

        assert "?extraProp" in query
        assert "OPTIONAL { ?subjectIri <http://example.org/extraProp> ?extraProp" in query

    def test_get_related_inverse_includes_object_columns(self):
        """Query should include object label and type columns."""
        builder = SPARQLBuilder()
        query = builder.build_get_related_inverse_query(
            "http://example.org/Object1", "http://example.org/relatesTo"
        )

        assert "?objectLabel" in query
        assert "?objectType" in query

    def test_get_related_inverse_distinct(self):
        """Query should use SELECT DISTINCT."""
        builder = SPARQLBuilder()
        query = builder.build_get_related_inverse_query(
            "http://example.org/Object1", "http://example.org/relatesTo"
        )

        assert "SELECT DISTINCT" in query
