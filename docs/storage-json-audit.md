# Storage and JSON audit

Audited against release `c98a350` and the local archive on 2026-09-08. This is an
inventory and normalization proposal; no DuckDB schema change is included.

## What is stored as JSON

Stable conversation/message fields, embeddings, tool names, tool status, edit
contents, and graph relationships already have native columns. JSON is used for
conversation/message metadata, variable tool inputs and outputs, a few provenance
collections, and exact signed authority/conflict records. It is not merely
unparsed residue: some fields already control core behavior.

| Data | Current use | Storage direction |
| --- | --- | --- |
| Message content and thinking | Text search and full-text index | Keep queryable text; native DuckDB compression |
| Message `provider_index` | Ordering messages with equal timestamps | Candidate for a core-owned typed projection |
| Message `history_of` | Excluding retained revisions from current reads/embedding | Candidate for a core-owned typed projection |
| Session ID, capture mode, parent/fork evidence | Import identity, capture classification, session topology | Keep existing `provider_sessions` identity table; normalize further where a concrete query requires it |
| Tool arguments/results | Provider-specific objects, arrays, strings; export and edit extraction | Retain lossless JSON; extract frequently queried fields selectively |
| Tool command/workdir/path/text | Potential direct tool-result retrieval | Add a searchable projection only with an explicit retrieval design and index size benchmark |
| Search result groups, citations and content references | Provider evidence/fidelity | Preserve structured payload, use native compression; do not flatten into hundreds of sparse columns |
| Signed controls, certificates, aliases and conflict bodies | Authorization and exact retained logical data | Preserve exact logical representation and existing typed identity/proof columns |

The full-text index currently covers `messages.content` and `messages.thinking`.
It does not index every tool output or metadata body. JSON fields can be queried
directly through SQL, but that is different from inclusion in normal `search`
and `query` results. Flattening JSON alone does not create a search index.

The measured 125.10 MB of message metadata includes 41.51 MB of
`search_result_groups`, 30.38 MB of `aggregate_result`, and 14.73 MB of
`content_references` values. In contrast, the values of `provider_index` occupy
0.25 MB over 75,347 messages, and `history_of` values occupy 13 KB over 727
messages. Promoting these small frequently read fields targets query work;
compression of large bodies targets storage. These figures measure serialized
JSON values before DuckDB compression, not their allocated disk blocks.

Tool outputs contain arrays, objects, and scalar strings. Measured serialized
sizes were respectively 1.306 GB, 42.6 MB, and 372 MB. Any normalization must keep
heterogeneous and unknown provider fields; a single assumed tool-result schema
would lose information.

## Changegraph

The file/conversation/message graph, edit timeline, replay/blame, repository
activity, and commit relationships join typed `file_edits`, `messages`,
`conversations`, and provenance tables. Confirmed `file_edit_evidence` gates
asserted edits. Graph edges do not require parsing raw tool-result JSON.

Some supplementary provenance collections remain JSON: repository roots/remotes
and checkpoint paths. The checkpoint-state view returns its paths collection;
that is separate from the typed graph edge and attribution queries.

## Migration constraint

Released signed logical rows include metadata and tool payloads. New typed
columns must initially be derived projections populated by core, with the
original logical payload preserved. Removing a metadata field from storage
requires a proven, lossless reconstruction of the old signed encoding. Existing
provider parent IDs can differ from normalized physical message IDs; apparent
duplication is not evidence that one can be deleted.

Benchmark native DuckDB compression and typed projections on a private copy
before replacing large JSON bodies with opaque application-compressed blobs.
Native compression preserves SQL access; opaque blobs would require an explicit
decompression/retrieval layer. No full raw-body shadow store is proposed.

A synthetic DuckDB 1.5.5 experiment confirmed that JSON columns can use native
Zstd while remaining directly queryable. Storage compatibility matters:
`JSON USING COMPRESSION zstd` stayed uncompressed with the default `v0.10.2`
write target, whereas creating the same table with an explicit `v1.2.0` target
produced ZSTD segments and returned identical JSON query results. Convos does
not currently override the default write target. The live archive's header was
also verified as storage format 64 (`v1.0.0+`), preceding the format 65 introduced
with DuckDB 1.2.0. A controlled storage-format
upgrade is therefore part of a Zstd proposal; changing the column declaration
alone is insufficient. The synthetic repetition ratio is not an estimate of
the production archive's savings. See DuckDB's
[storage format and compression documentation](https://duckdb.org/docs/current/internals/storage).
