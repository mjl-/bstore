# Query enhancement notes

These notes use query patterns observed in `TheFellow/go-modular-monolith` as
concrete evidence, but propose only behavior that is generally useful for
bstore callers. Application-specific expression syntax, projections,
authorization, joins, and association loading remain outside bstore.

The notes separate improvements that can reuse the current query API from
features that need a deliberate API and planner design. A use case appearing in
one application is motivation, not by itself a reason to add a public API.

## Recommended order

1. [Indexed query performance](indexed-query-performance.md)
   - normalize repeated constraints on the same field;
   - plan multi-value `FilterEqual` as direct primary-key lookups or index
     spans, as appropriate;
   - add an index-aware string-prefix predicate.
2. [Predicate completeness](predicate-completeness.md)
   - represent grouped `AND`, `OR`, and normalized `NOT` predicates;
   - add explicit nil predicates for pointer-backed optional state;
   - support nested scalar field paths;
   - make slice membership behavior explicit for all supported element types;
   - consider scan-only string predicates as convenience operations.
3. Consider extending existing index-only `IDs`, `Count`, and `Exists` execution
   to residual predicates answerable from index keys, after the planner has a
   normalized constraint representation and benchmarks show meaningful record
   decoding cost.
4. [Cross-row boundaries](cross-row-boundaries.md)
   - keep joins, application projections, and full-text indexing out of the
     first two efforts;
   - document application-side alternatives so a narrow bstore feature does
     not accidentally grow into an incomplete relational layer.

## Compatibility rule

Existing `Filter*` calls are conjunctive and must keep their current behavior.
New planner representations should lower those calls into an implicit top-level
`AND`. Query results, explicit ordering, limits, update/delete behavior,
transaction semantics, and cursor reseeking must remain unchanged when a new
plan is chosen. Incidental order without `SortAsc` or `SortDesc` is not a
compatibility guarantee.

Performance work should have tests that assert both results and `Stats` plan
counters. Predicate work should retain a final residual check whenever an index
plan is only a candidate selector.
