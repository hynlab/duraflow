# Independent implementation and provenance

The authored implementation, tests, examples and schemas are licensed under
Apache License 2.0. The existing repository LICENSE is retained.

Infinitic's separation of workflow interpretation, state coordination, remote
business execution, storage and transport informed the architectural problem
statement. No upstream source, tests, comments, fixtures or serialization code
were copied or translated. This is an independent implementation, not an
affiliated or compatible Python distribution of Infinitic. This statement does
not constitute a legal certification of a formal clean-room process.

Runtime dependencies are installed normally, not vendored: Pydantic; optional
SQLAlchemy, psycopg and Apache Pulsar Python client. Their separate licenses and
native/transitive dependency obligations remain applicable. Distribution images
must inventory the actual installed dependencies; this project's license does
not relicense them.

Architectural/public API references:
- Infinitic concepts: https://docs.infinitic.io/docs/components/terminology
- Python coroutine protocol: https://docs.python.org/3/reference/datamodel.html
- Pydantic TypeAdapter: https://docs.pydantic.dev/latest/concepts/type_adapter/
- SQLAlchemy asyncio: https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
- Pulsar Python client: https://pulsar.apache.org/docs/4.0.x/client-libraries-python/
- Apache License: https://www.apache.org/licenses/LICENSE-2.0
