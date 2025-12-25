"""Unit tests for connector_client_utils module."""

import asyncio

import pandas as pd

from client.connector_client_utils import (
    dump_payload,
    run_async_in_sync,
    simplify_sparql_results,
    sparql_results_to_dataframe,
)


class TestSparqlResultsToDataframe:
    """Tests for sparql_results_to_dataframe function."""

    def test_empty_bindings(self):
        """Empty bindings should return empty DataFrame."""
        result = sparql_results_to_dataframe([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_single_binding(self):
        """Single binding should produce single row."""
        bindings = [{"name": {"type": "literal", "value": "Test"}}]
        result = sparql_results_to_dataframe(bindings, simplify=False)

        assert len(result) == 1
        assert result.iloc[0]["name"] == "Test"

    def test_multiple_bindings(self):
        """Multiple bindings should produce multiple rows."""
        bindings = [
            {"name": {"type": "literal", "value": "First"}},
            {"name": {"type": "literal", "value": "Second"}},
            {"name": {"type": "literal", "value": "Third"}},
        ]
        result = sparql_results_to_dataframe(bindings, simplify=False)

        assert len(result) == 3

    def test_simplify_true_strips_fragment(self):
        """With simplify=True, URI fragments should be stripped."""
        bindings = [{"entityIri": {"type": "uri", "value": "http://example.org/instances#Entity1"}}]
        result = sparql_results_to_dataframe(bindings, simplify=True)

        assert result.iloc[0]["entityIri"] == "Entity1"

    def test_simplify_true_strips_path(self):
        """With simplify=True, URI paths should be stripped."""
        bindings = [{"entityIri": {"type": "uri", "value": "http://example.org/path/Entity1"}}]
        result = sparql_results_to_dataframe(bindings, simplify=True)

        assert result.iloc[0]["entityIri"] == "Entity1"

    def test_simplify_false_preserves_uri(self):
        """With simplify=False, full URIs should be preserved."""
        bindings = [{"entityIri": {"type": "uri", "value": "http://example.org/instances#Entity1"}}]
        result = sparql_results_to_dataframe(bindings, simplify=False)

        assert result.iloc[0]["entityIri"] == "http://example.org/instances#Entity1"

    def test_multiple_columns(self):
        """Multiple columns should be handled correctly."""
        bindings = [
            {
                "name": {"type": "literal", "value": "Test"},
                "value": {"type": "literal", "value": "42"},
                "uri": {"type": "uri", "value": "http://example.org#item"},
            }
        ]
        result = sparql_results_to_dataframe(bindings, simplify=False)

        assert "name" in result.columns
        assert "value" in result.columns
        assert "uri" in result.columns


class TestSimplifySparlResults:
    """Tests for simplify_sparql_results function."""

    def test_simplify_list(self):
        """List of dicts should be simplified."""
        results = [
            {"uri": {"value": "http://example.org#Item1"}},
            {"uri": {"value": "http://example.org#Item2"}},
        ]
        simplified = simplify_sparql_results(results)

        assert simplified[0]["uri"] == "Item1"
        assert simplified[1]["uri"] == "Item2"

    def test_simplify_preserves_plain_values(self):
        """Plain values (not URIs) should pass through."""
        results = [{"name": {"value": "Simple Text"}}]
        simplified = simplify_sparql_results(results)

        assert simplified[0]["name"] == "Simple Text"

    def test_simplify_fragment_extraction(self):
        """Fragment after # should be extracted."""
        results = [{"id": {"value": "http://example.org/ontology#MyClass"}}]
        simplified = simplify_sparql_results(results)

        assert simplified[0]["id"] == "MyClass"

    def test_simplify_path_extraction(self):
        """Last path segment should be extracted."""
        results = [{"id": {"value": "http://example.org/path/to/resource"}}]
        simplified = simplify_sparql_results(results)

        assert simplified[0]["id"] == "resource"

    def test_simplify_empty_list(self):
        """Empty list should return empty list."""
        result = simplify_sparql_results([])
        assert result == []


class TestRunAsyncInSync:
    """Tests for run_async_in_sync function."""

    def test_run_simple_coroutine(self):
        """Simple async function should be executed."""

        async def simple_async():
            return 42

        result = run_async_in_sync(simple_async)
        assert result == 42

    def test_run_with_args(self):
        """Args should be passed correctly."""

        async def async_with_args(a, b):
            return a + b

        result = run_async_in_sync(async_with_args, 10, 20)
        assert result == 30

    def test_run_with_kwargs(self):
        """Kwargs should be passed correctly."""

        async def async_with_kwargs(x, y=5):
            return x * y

        result = run_async_in_sync(async_with_kwargs, 10, y=3)
        assert result == 30

    def test_run_with_await(self):
        """Async function with await should work."""

        async def async_with_await():
            await asyncio.sleep(0.01)
            return "done"

        result = run_async_in_sync(async_with_await)
        assert result == "done"


class TestDumpPayload:
    """Tests for dump_payload function."""

    def test_dump_returns_string(self):
        """dump_payload should return a JSON string."""

        # Create a minimal mock that has model_dump_json
        class MockMsg:
            def model_dump_json(self, indent=None):
                return '{"test": "value"}'

        msg = MockMsg()
        result = dump_payload(msg)

        assert isinstance(result, str)
        assert "test" in result

    def test_dump_uses_indent(self):
        """dump_payload should use indent=4."""
        indent_used = None

        class MockMsg:
            def model_dump_json(self, indent=None):
                nonlocal indent_used
                indent_used = indent
                return "{}"

        dump_payload(MockMsg())
        assert indent_used == 4
