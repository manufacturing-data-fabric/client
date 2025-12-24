# Tests for the Data Fabric Client

## Created Test Files

```
tests/
├── __init__.py
├── conftest.py                    # Fixtures & Mocks
├── test_sparql_builder.py         # 45 Unit Tests
├── test_connector_client_utils.py # 18 Unit Tests
└── test_connector_client.py       # 30 Integration Tests
```

## Test Results: 93 Tests

### test_sparql_builder.py (45 Tests)
- `_wrap_uri()` - URI wrapping
- `_get_var_name()` - Variable name extraction
- `_escape_literal()` - String escaping
- `SPARQLBuilder` query methods

### test_connector_client_utils.py (18 Tests)
- `sparql_results_to_dataframe()` - DataFrame conversion
- `simplify_sparql_results()` - URI simplification
- `run_async_in_sync()` - Async-sync bridge
- `dump_payload()` - JSON serialization

### test_connector_client.py (30 Tests)
- Initialization & topic configuration
- Connector switching
- Kafka producer lifecycle
- Publish commands (Read, Subscribe, Unsubscribe, Action)
- GraphDB queries (query_graphdb, get_all_classes, list_instances, etc.)
- Sync wrappers

## Running Tests

```bash
pytest tests/ -v
```

## Known Notes

`search_entity()` uses `property_uri="rdfs:label"` as default, which is a prefixed name, but `_wrap_uri()` expects a full IRI. Therefore, the full IRI is used in tests:

```python
client.search_entity(
    "keyword",
    property_uri="http://www.w3.org/2000/01/rdf-schema#label"
)
```

## Tests Still To Be Done

Based on the project architecture, the following areas lack dedicated test coverage:

1. **`messaging/`** - OPC UA payload types (Read, Write, Subscribe, Unsubscribe, Telemetry payloads)
2. **`connector_base/connector/connector.py`** - Coordinator logic between external commands and telemetry
3. **`connector_base/connector/kafka.py`** - KafkaBroker reconnection logic
4. **`connector_base/connector/messages/`** - Pydantic message model validation
5. **Query Intent Abstraction** - The 5 core intents (`list_instances`, `search_entity`, `get_properties`, `get_related`, `get_related_inverse`) may need more comprehensive integration tests
6. **Connector Resolution Pattern** - Testing the `DataPoint -> Device -> Service -> Connector` graph traversal
7. **End-to-end Kafka workflows** - Full request-response correlation via UUIDs

Note: Some of these may already have tests in `connector_base/tests/` (the submodule), which would be separate from this client test suite.
