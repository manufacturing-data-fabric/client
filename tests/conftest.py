"""Shared pytest fixtures and mocks for client tests."""

import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest


# =============================================================================
# Environment Variable Fixtures
# =============================================================================

@pytest.fixture
def mock_env(monkeypatch):
    """Set up test environment variables."""
    monkeypatch.setenv("MODULE_TYPE", "test_connector")
    monkeypatch.setenv("MODULE_ID", "test_001")
    monkeypatch.setenv("GRAPHDB_HOST", "http://localhost:7200")
    monkeypatch.setenv("GRAPHDB_REPOSITORY_ID", "test_repo")


# =============================================================================
# Kafka Mocks
# =============================================================================

@pytest.fixture
def mock_kafka_producer():
    """Mock AIOKafkaProducer with async methods."""
    producer = AsyncMock()
    producer._closed = False
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock()
    return producer


class MockKafkaMessage:
    """Mock Kafka message with value attribute."""

    def __init__(self, value: dict):
        self._value = value

    @property
    def value(self):
        return self._value


class MockKafkaConsumer:
    """Mock async Kafka consumer that yields controlled messages."""

    def __init__(self, messages: list[dict] = None):
        self.messages = messages or []
        self._index = 0
        self._started = False
        self._stopped = False

    async def start(self):
        self._started = True

    async def stop(self):
        self._stopped = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self.messages):
            raise StopAsyncIteration
        msg = MockKafkaMessage(self.messages[self._index])
        self._index += 1
        return msg


@pytest.fixture
def mock_kafka_consumer_factory():
    """Factory to create mock Kafka consumers with specific messages."""
    def _create(messages: list[dict] = None):
        return MockKafkaConsumer(messages)
    return _create


# =============================================================================
# GraphDB / aiohttp Mocks
# =============================================================================

@pytest.fixture
def mock_graphdb_response():
    """Create a mock aiohttp response for GraphDB queries."""
    def _create(bindings: list, status: int = 200):
        mock_response = AsyncMock()
        mock_response.status = status
        mock_response.json = AsyncMock(return_value={
            "results": {"bindings": bindings}
        })
        mock_response.text = AsyncMock(return_value="Error")
        return mock_response
    return _create


@pytest.fixture
def mock_aiohttp_session(mock_graphdb_response):
    """Mock aiohttp.ClientSession for GraphDB SPARQL queries."""
    def _create(bindings: list, status: int = 200):
        response = mock_graphdb_response(bindings, status)

        # Create async context manager for response
        mock_post_cm = AsyncMock()
        mock_post_cm.__aenter__ = AsyncMock(return_value=response)
        mock_post_cm.__aexit__ = AsyncMock(return_value=None)

        # Create session mock
        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_post_cm)

        # Create async context manager for session
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        return mock_session_cm
    return _create


# =============================================================================
# Sample Test Data
# =============================================================================

@pytest.fixture
def sample_sparql_bindings():
    """Sample SPARQL query result bindings."""
    return [
        {
            "entityIri": {"type": "uri", "value": "http://example.org/instances#Entity1"},
            "entityLabel": {"type": "literal", "value": "Entity One"},
            "entityType": {"type": "uri", "value": "http://example.org/ontology#MyClass"}
        },
        {
            "entityIri": {"type": "uri", "value": "http://example.org/instances#Entity2"},
            "entityLabel": {"type": "literal", "value": "Entity Two"},
            "entityType": {"type": "uri", "value": "http://example.org/ontology#MyClass"}
        }
    ]


@pytest.fixture
def sample_class_bindings():
    """Sample class list bindings."""
    return [
        {"class": {"type": "uri", "value": "http://example.org/ontology#Device"}},
        {"class": {"type": "uri", "value": "http://example.org/ontology#Connector"}},
        {"class": {"type": "uri", "value": "http://example.org/ontology#DataPoint"}}
    ]


@pytest.fixture
def sample_properties_bindings():
    """Sample properties query bindings."""
    return [
        {
            "subjectIri": {"type": "uri", "value": "http://example.org/instances#Device1"},
            "subjectType": {"type": "uri", "value": "http://example.org/ontology#Device"},
            "predicate": {"type": "uri", "value": "http://www.w3.org/2000/01/rdf-schema#label"},
            "object": {"type": "literal", "value": "Device One"},
            "subjectLabel": {"type": "literal", "value": "Device One"}
        }
    ]


@pytest.fixture
def sample_connector_bindings():
    """Sample connector list bindings for load_connector_config."""
    return [
        {
            "entityIri": {"type": "uri", "value": "http://example.org/instances#Connector1"},
            "entityLabel": {"type": "literal", "value": "OPC UA Connector"},
            "moduleId": {"type": "literal", "value": "opcua_connector_001"},
            "moduleType": {"type": "literal", "value": "opcua"}
        }
    ]


@pytest.fixture
def sample_correlation_id():
    """Generate a sample correlation ID."""
    return str(uuid4())


@pytest.fixture
def sample_kafka_response(sample_correlation_id):
    """Create a sample Kafka response message."""
    return {
        "message_id": str(uuid4()),
        "timestamp": "2024-01-01T00:00:00Z",
        "correlation_id": sample_correlation_id,
        "payload": {
            "command_type": "response",
            "base_payload": {
                "device_origin": "test_connector",
                "value_dict": {"node1": 42}
            }
        }
    }


# =============================================================================
# ConnectorClient Fixture with Mocks
# =============================================================================

@pytest.fixture
def connector_client(mock_env, mock_kafka_producer):
    """Create a ConnectorClient with mocked Kafka producer."""
    with patch('client.connector_client.AIOKafkaProducer', return_value=mock_kafka_producer):
        from client.connector_client import ConnectorClient
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])
        client.producer = mock_kafka_producer
        yield client
