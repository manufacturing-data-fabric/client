# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Fabric Client - A Python client for interacting with a Knowledge Graph (GraphDB) and Kafka-based Connectors within an industrial data fabric framework. Primary interface is Jupyter notebooks.

## Development Commands

```bash
# Start Jupyter Lab
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
pip install -e connector_base/
```

## Architecture

### Core Components

**`client/`** - Main client package
- `connector_client.py` - Primary API class (~40 methods) for GraphDB queries and Kafka connector commands
- `sparql_builder.py` - SPARQL query builder abstracting RDF complexity
- `connector_client_utils.py` - Async-sync bridges, Kafka consumers, DataFrame conversion

**`connector_base/`** - Git submodule for connector infrastructure
- `connector/connector.py` - Coordinator between external commands and telemetry
- `connector/kafka.py` - KafkaBroker with reconnection logic
- `connector/messages/` - Pydantic-based message models
- `connector/testing/` - Event-driven test runner framework

**`messaging/`** - OPC UA specific payload types (Read, Write, Subscribe, Unsubscribe, Telemetry)

**`notebooks/`** - User-facing Jupyter workflows (QueryKG, Workflow_Subscribe, Workflow_Read, etc.)

### Kafka Topic Pattern

Connectors use three topics per module:
- `request.{connector_type}.{module_id}` - Commands to connector
- `response.{connector_type}.{module_id}` - Responses from connector
- `telemetry.{connector_type}.{module_id}` - Subscribed data streams

### Query Intent Abstraction

The client abstracts SPARQL into 5 core intents:
1. `list_instances` - Get all instances of a class
2. `search_entity` - Keyword search over labels
3. `get_properties` - Describe an entity's properties
4. `get_related` - Follow outgoing relationships
5. `get_related_inverse` - Follow incoming relationships

Users interact with human-readable labels; the client resolves to URIs internally.

### Connector Resolution Pattern

To send commands, the client traces through the knowledge graph:
`DataPoint → Device → Service → Connector` to find the appropriate Kafka topic.

## Code Patterns

- **Async-first with sync wrappers**: All major methods have async versions; sync versions wrap for Jupyter compatibility
- **Pydantic models**: Strict message validation for Kafka payloads
- **Correlation IDs**: Request-response matching via UUIDs
- **RDF namespace**: `http://stephantrattnig.org/`

## Environment Configuration

Key `.env` variables:
- `KAFKA_BROKER`, `KAFKA_PORT` - Kafka infrastructure
- `GRAPHDB_HOST`, `GRAPHDB_REPOSITORY_ID` - GraphDB connection
- `MODULE_TYPE`, `MODULE_ID` - Connector identification

## Testing

Tests are in `connector_base/tests/` using Python's `IsolatedAsyncioTestCase` and the custom event-driven `TestRunner`.
