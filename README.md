# AZ AI Ingestion Processor (Experimental)

## Quick Start

### Run the examples

#### It's a RAG example

![examples/itsarag.md](examples/itsarag.md)

```bash
uv run examples/itsarag.py
```

> [!NOTE]
> `itsarag.py` also generates a Mermaid diagram of its graph in [examples/itsarag.md](examples/itsarag.md).

To visualize the processed fragments you can then use the AZ AI Ingestion CLI:

```bash
uv run az-ai-ingestion show --repository /tmp/itsarag_ingestion/
```

#### Argus example

![examples/argus.md](examples/argus.md)

```bash
uv run examples/argus.py
```

> [!NOTE]  
> `argus.py` also generates a Mermaid diagram of its graph in [examples/argus.md](examples/argus.md).

To visualize the processed fragments you can then use the AZ AI Ingestion CLI:

```bash
uv run az-ai-ingestion show --repository /tmp/argus_ingestion/
```

### Run the tests

```bash
uv run pytest
```

