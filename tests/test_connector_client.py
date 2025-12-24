"""Integration tests for ConnectorClient.

These tests require mocking Kafka and GraphDB dependencies.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from client.connector_client import ConnectorClient


class TestConnectorClientInit:
    """Tests for ConnectorClient initialization."""

    def test_init_topics_from_env(self, mock_env):
        """Topics should be built from MODULE_TYPE and MODULE_ID."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])

        assert client.request_topic == "request.test_connector.test_001"
        assert client.response_topic == "response.test_connector.test_001"
        assert client.telemetry_topic == "telemetry.test_connector.test_001"

    def test_init_sparql_endpoint(self, mock_env):
        """SPARQL endpoint should be built from GRAPHDB_HOST and GRAPHDB_REPOSITORY_ID."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])

        assert client.sparql_endpoint == "http://localhost:7200/repositories/test_repo"

    def test_init_bootstrap_servers(self, mock_env):
        """Bootstrap servers should be stored."""
        servers = ["broker1:9092", "broker2:9092"]
        client = ConnectorClient(bootstrap_servers=servers)

        assert client.bootstrap_server == servers

    def test_init_producer_is_none(self, mock_env):
        """Producer should be None initially."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])

        assert client.producer is None

    def test_init_connectors_empty(self, mock_env):
        """Connectors dict should be empty initially."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])

        assert client.connectors == {}


class TestConnectorSwitching:
    """Tests for connector switching functionality."""

    @pytest.mark.asyncio
    async def test_switch_connector_success(self, mock_env):
        """Switching to a known connector should update topics."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])
        client.connectors = {
            "other_connector": {
                "request": "request.other.001",
                "response": "response.other.001",
                "telemetry": "telemetry.other.001"
            }
        }

        await client.switch_connector("other_connector")

        assert client.request_topic == "request.other.001"
        assert client.response_topic == "response.other.001"
        assert client.telemetry_topic == "telemetry.other.001"

    @pytest.mark.asyncio
    async def test_switch_connector_not_found_raises(self, mock_env):
        """Switching to unknown connector should raise ValueError."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])
        client.connectors = {}

        with pytest.raises(ValueError, match="No topic config found"):
            await client.switch_connector("unknown_connector")

    def test_return_connectors(self, mock_env):
        """return_connectors should return connector keys."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])
        client.connectors = {
            "connector_a": {},
            "connector_b": {}
        }

        keys = list(client.return_connectors())

        assert "connector_a" in keys
        assert "connector_b" in keys


class TestKafkaProducerLifecycle:
    """Tests for Kafka producer lifecycle."""

    @pytest.mark.asyncio
    async def test_create_producer(self, mock_env, mock_kafka_producer):
        """Producer should be created and started."""
        with patch('client.connector_client.AIOKafkaProducer', return_value=mock_kafka_producer):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            await client._create_producer()

            mock_kafka_producer.start.assert_called_once()
            assert client.producer == mock_kafka_producer

    @pytest.mark.asyncio
    async def test_close_producer(self, mock_env, mock_kafka_producer):
        """Close should stop the producer."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])
        client.producer = mock_kafka_producer

        await client.close()

        mock_kafka_producer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_no_producer(self, mock_env):
        """Close with no producer should not raise."""
        client = ConnectorClient(bootstrap_servers=["localhost:9092"])
        client.producer = None

        await client.close()  # Should not raise


class TestPublishCommands:
    """Tests for Kafka publish command methods."""

    @pytest.mark.asyncio
    async def test_publish_read_command(self, mock_env, mock_kafka_producer):
        """publish_read_command should send ReadCommand to request topic."""
        with patch('client.connector_client.AIOKafkaProducer', return_value=mock_kafka_producer):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])

            # Create a mock payload
            mock_payload = MagicMock()
            mock_payload.device_origin = "test_client"

            await client.publish_read_command(mock_payload)

            mock_kafka_producer.start.assert_called_once()
            mock_kafka_producer.send_and_wait.assert_called_once()

            call_kwargs = mock_kafka_producer.send_and_wait.call_args.kwargs
            assert call_kwargs['topic'] == "request.test_connector.test_001"

    @pytest.mark.asyncio
    async def test_publish_subscribe_command(self, mock_env, mock_kafka_producer):
        """publish_subscribe_command should send SubscribeCommand."""
        with patch('client.connector_client.AIOKafkaProducer', return_value=mock_kafka_producer):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])

            mock_payload = MagicMock()

            await client.publish_subscribe_command(mock_payload)

            mock_kafka_producer.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_unsubscribe_command(self, mock_env, mock_kafka_producer):
        """publish_unsubscribe_command should send UnsubscribeCommand."""
        with patch('client.connector_client.AIOKafkaProducer', return_value=mock_kafka_producer):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])

            mock_payload = MagicMock()

            await client.publish_unsubscribe_command(mock_payload)

            mock_kafka_producer.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_action_command(self, mock_env, mock_kafka_producer):
        """publish_action_command should send ActionCommand."""
        with patch('client.connector_client.AIOKafkaProducer', return_value=mock_kafka_producer):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])

            mock_payload = MagicMock()

            await client.publish_action_command(mock_payload)

            mock_kafka_producer.send_and_wait.assert_called_once()


class TestQueryGraphDB:
    """Tests for GraphDB query methods."""

    @pytest.mark.asyncio
    async def test_query_graphdb_success_raw(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """query_graphdb should return raw bindings when pretty=False."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.query_graphdb("SELECT * WHERE { ?s ?p ?o }", pretty=False)

            assert result == sample_sparql_bindings

    @pytest.mark.asyncio
    async def test_query_graphdb_success_pretty(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """query_graphdb should return DataFrame when pretty=True."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.query_graphdb("SELECT * WHERE { ?s ?p ?o }", pretty=True)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2

    @pytest.mark.asyncio
    async def test_query_graphdb_error_returns_empty(self, mock_env, mock_aiohttp_session):
        """query_graphdb should return empty list on HTTP error."""
        session = mock_aiohttp_session([], status=400)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.query_graphdb("INVALID QUERY", pretty=False)

            assert result == []


class TestGraphDBQueryIntents:
    """Tests for high-level GraphDB query intent methods."""

    @pytest.mark.asyncio
    async def test_get_all_classes(self, mock_env, mock_aiohttp_session, sample_class_bindings):
        """get_all_classes should return class bindings."""
        session = mock_aiohttp_session(sample_class_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.get_all_classes(pretty=False)

            assert len(result) == 3

    @pytest.mark.asyncio
    async def test_list_instances(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """list_instances should query instances of a class."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.list_instances(
                "http://example.org/ontology#Device",
                pretty=False
            )

            assert len(result) == 2

    @pytest.mark.asyncio
    async def test_list_instances_with_optional_props(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """list_instances should support optional properties."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.list_instances(
                "http://example.org/ontology#Device",
                optional_props=["http://example.org/ontology#status"],
                pretty=False
            )

            assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_properties(self, mock_env, mock_aiohttp_session, sample_properties_bindings):
        """get_properties should return subject properties."""
        session = mock_aiohttp_session(sample_properties_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.get_properties(
                "http://example.org/instances#Device1",
                pretty=False
            )

            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_related(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """get_related should follow predicate to related objects."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.get_related(
                "http://example.org/instances#Device1",
                "http://example.org/ontology#hasDataPoint",
                pretty=False
            )

            assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_related_inverse(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """get_related_inverse should find subjects pointing to object."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = await client.get_related_inverse(
                "http://example.org/instances#DataPoint1",
                "http://example.org/ontology#hasDataPoint",
                pretty=False
            )

            assert len(result) == 2

    @pytest.mark.asyncio
    async def test_search_entity(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """search_entity should search by keyword."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            # Use full IRI for property_uri (the default "rdfs:label" is a prefixed name)
            result = await client.search_entity(
                "Entity",
                property_uri="http://www.w3.org/2000/01/rdf-schema#label",
                pretty=False
            )

            assert len(result) == 2


class TestLoadConnectorConfig:
    """Tests for load_connector_config method."""

    @pytest.mark.asyncio
    async def test_load_connector_config(self, mock_env, mock_aiohttp_session, sample_connector_bindings):
        """load_connector_config should populate connectors dict."""
        session = mock_aiohttp_session(sample_connector_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            await client.load_connector_config()

            assert "opcua_connector_001" in client.connectors
            config = client.connectors["opcua_connector_001"]
            assert config["request"] == "request.opcua.opcua_connector_001"
            assert config["response"] == "response.opcua.opcua_connector_001"
            assert config["telemetry"] == "telemetry.opcua.opcua_connector_001"


class TestSyncWrappers:
    """Tests for synchronous wrapper methods."""

    def test_query_graphdb_sync(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """query_graphdb_sync should call async version."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = client.query_graphdb_sync("SELECT * WHERE { ?s ?p ?o }", pretty=False)

            assert result == sample_sparql_bindings

    def test_get_all_classes_sync(self, mock_env, mock_aiohttp_session, sample_class_bindings):
        """get_all_classes_sync should return class bindings."""
        session = mock_aiohttp_session(sample_class_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = client.get_all_classes_sync()

            assert len(result) == 3

    def test_list_instances_sync(self, mock_env, mock_aiohttp_session, sample_sparql_bindings):
        """list_instances_sync should query instances."""
        session = mock_aiohttp_session(sample_sparql_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            result = client.list_instances_sync("http://example.org/ontology#Device")

            assert len(result) == 2

    def test_load_connector_config_sync(self, mock_env, mock_aiohttp_session, sample_connector_bindings):
        """load_connector_config_sync should populate connectors."""
        session = mock_aiohttp_session(sample_connector_bindings)

        with patch('aiohttp.ClientSession', return_value=session):
            client = ConnectorClient(bootstrap_servers=["localhost:9092"])
            client.load_connector_config_sync()

            assert "opcua_connector_001" in client.connectors
