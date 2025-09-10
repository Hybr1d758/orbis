"""Orbis data pipeline package.

Modular namespaces:
- ingest: file ingestion utilities
- clean: cleaning and normalization helpers
- validate: data validation and profiling
- enrich: joins and derived fields
- allocate: cost allocation logic
- export: writers for curated outputs

CLI entry: `python -m orbis_pipeline.cli`
"""

__all__ = [
    "__version__",
]

__version__ = "0.1.0"


