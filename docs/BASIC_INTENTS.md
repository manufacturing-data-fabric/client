Certainly! Below is an **extensive description of each of the five core query intents**, explaining their **purpose**, **conceptual scope**, **technical details**, **usage patterns**, and **value in a knowledge graph context**.

---

## 🔎 1. `list_instances`

### ✅ Purpose:

Retrieve all known instances (individuals) of a given class/type in the knowledge graph.

### 🧠 Conceptual Meaning:

This intent allows users to explore *"What kinds of things exist?"* under a semantic class — e.g., all `Device`, `Sensor`, `DataPoint`, or `ProductionLine` entities.

### 🔧 SPARQL Pattern:

```sparql
SELECT ?instance ?label WHERE {
  ?instance a <ClassURI> .
  OPTIONAL { ?instance rdfs:label ?label }
}
```

### 📌 Inputs:

* `Class URI` (e.g., `ex:Device`)

### 📤 Outputs:

* A list of instances (URIs) with optional human-readable labels

### 💡 Use Cases:

* Show all known machines or sensors in a plant
* Populate dropdowns or selection menus in a UI
* Serve as a starting point for further exploration

### ⚠️ Considerations:

* May need pagination or limit for large classes
* Results can be cached if class membership is stable

---

## 🔍 2. `search_entity`

### ✅ Purpose:

Find entities in the graph by performing a **keyword-based search** over `rdfs:label` or other identifying properties.

### 🧠 Conceptual Meaning:

This enables intuitive *lookup by name*, simulating "search as you type" behavior — even if the user doesn’t know the exact class or URI.

### 🔧 SPARQL Pattern:

```sparql
SELECT ?entity ?label WHERE {
  ?entity rdfs:label ?label .
  FILTER(CONTAINS(LCASE(?label), LCASE("pump")))
  OPTIONAL { ?entity a ?type }
}
```

*(Optionally add a `?entity a <ClassURI>` clause to filter by type.)*

### 📌 Inputs:

* Keyword string (e.g., `"pump"` or `"inlet sensor"`)
* Optional `Class URI` to narrow the scope

### 📤 Outputs:

* List of matching URIs + labels

### 💡 Use Cases:

* "Find Pump A"
* Fuzzy autocomplete in interfaces
* Quickly locate a datapoint or resource

### ⚠️ Considerations:

* May return multiple matches → disambiguation needed
* Language and case handling should be normalized

---

## 📝 3. `get_properties`

### ✅ Purpose:

Retrieve **all known properties** (data and object properties) of a specific instance.

### 🧠 Conceptual Meaning:

This is equivalent to "describe this entity" — pulling out its values, attributes, or connections in a flat key-value view.

### 🔧 SPARQL Pattern:

```sparql
SELECT ?property ?value WHERE {
  <InstanceURI> ?property ?value .
}
```

### 📌 Inputs:

* `Instance URI`

### 📤 Outputs:

* List of `(property, value)` pairs
* `value` may be a literal (number, string) or a URI (linked entity)

### 💡 Use Cases:

* Show a detailed info panel for a device
* Allow LLMs to inspect facts about an entity
* Generate documentation for a selected node

### ⚠️ Considerations:

* May need filtering (e.g., exclude OWL/system properties)
* For `value` URIs, a follow-up label lookup might be helpful

---

## 🔗 4. `get_related`

### ✅ Purpose:

Traverse a **known outgoing property** from a subject entity to its related objects.

### 🧠 Conceptual Meaning:

This allows users or agents to **"follow a relation"** — e.g., from a `Device` to its `DataPoints`, from a `Sensor` to its `Unit`, etc.

### 🔧 SPARQL Pattern:

```sparql
SELECT ?object ?label WHERE {
  <SubjectURI> <PredicateURI> ?object .
  OPTIONAL { ?object rdfs:label ?label }
}
```

### 📌 Inputs:

* `Subject URI` (e.g., `Pump123`)
* `Predicate URI` (e.g., `ex:hasDataPoint`)

### 📤 Outputs:

* List of related objects with labels

### 💡 Use Cases:

* Get all sensors attached to a machine
* Traverse a part-of structure (e.g., Line → Segment → Station)
* Support chaining and graph traversal in scripts

### ⚠️ Considerations:

* You may want to infer labels for objects
* Result cardinality may vary — needs iteration or selection

---

## 🔁 5. `get_related_inverse`

### ✅ Purpose:

Traverse a **known incoming property** — i.e., find all subjects that point to a given object via a specific predicate.

### 🧠 Conceptual Meaning:

This is useful when you only have a downstream resource and want to know *“who uses/references this?”* — e.g., find the owning device for a `DataPoint`.

### 🔧 SPARQL Pattern:

```sparql
SELECT ?subject ?label WHERE {
  ?subject <PredicateURI> <ObjectURI> .
  OPTIONAL { ?subject rdfs:label ?label }
}
```

### 📌 Inputs:

* `Object URI` (e.g., `DataPointXYZ`)
* `Predicate URI` (e.g., `ex:hasDataPoint`)

### 📤 Outputs:

* List of subject entities that link to the object

### 💡 Use Cases:

* "Which device owns this datapoint?"
* Reverse navigation for LLM agents
* Disambiguate indirect references

### ⚠️ Considerations:

* Often returns a single subject — but not always
* Same logic as `get_related`, just reversed

---

## ✅ Final Remarks

Together, these five intents form a **minimal, complete interaction model** for:

* Navigating RDF graphs
* Building CLI interfaces
* Powering LangChain tools or agents
* Supporting exploratory or structured semantic queries

They’re **generic**, **composable**, and **implementation-agnostic**, which is key for maintainable tooling across different domains or ontologies.

Would you like this turned into a Markdown doc or code docstring template?
