# Indexed query performance

## 1. Multi-value equality planning

### Motivation

`FilterEqual("Status", "pending", "completed")` already means that `Status`
may equal any supplied value. Internally it becomes `filterIn`. A fully matched
unique index can turn those values into direct keys, but primary-key and
ordinary-index planning only recognize a single `filterEqual`. The query
therefore falls back to a broader scan even when the field is the primary key
or has an index.

This is the storage-level form of common application predicates such as:

```text
status in ["pending", "completed"]
glass == "coupe" || glass == "rocks"
type == "drink" && owner_id in requested_ids
```

The last shape is common in batched association loading: one exact leading
component followed by many owner IDs in a composite index. Batched existence or
authorization checks also commonly supply several primary keys at once.

The API is already complete. This is primarily planner and executor work, and
`plan.go` already records it as a TODO.

### Proposed execution forms

Use the cheapest representation for each storage shape:

- a multi-value primary-key equality becomes deduplicated direct record keys,
  using the same execution shape as `FilterIDs`;
- a fully matched unique index continues to use direct index keys;
- an ordinary index or partially matched unique index uses one or more disjoint
  spans over the index bucket.

`FilterIDs` remains the clearest API when a caller already has a correctly typed
slice of primary keys. Efficient `FilterEqual` planning is still required for
API consistency and for generic expression adapters that do not special-case a
type's primary key. Direct-key execution should avoid repeated gets for duplicate
inputs; normalization may deduplicate `FilterEqual` values, and `FilterIDs`
should likewise deduplicate its packed-key list.

Generalize the current single `start`/`stop` range into spans, each with its own
inclusive flags. The single-span case should stay allocation-light.

For an indexed scalar field, each distinct equality value produces one prefix
span. Public methods should continue to validate and convert values when the
filter is added; planning then deduplicates and orders their packed
representations. Composite indices can form the product of consecutive
equality/IN values until the first range or unconstrained field.

The implementation must account for:

- repeated `FilterEqual` calls on the same field, whose semantics are
  intersection, not union;
- interaction with `FilterNotEqual` and empty intersections;
- duplicate supplied values;
- composite-index products without integer overflow or unbounded accidental
  allocation; above an explicit span budget, retain a broader plan plus the
  complete residual constraint;
- ascending and descending scans;
- mutation and cursor reseeking during `ForEach`, `Update`, and `Delete`;
- limits only after residual filters and required ordering are satisfied.

### Ordering

If requested order is compatible with the selected index, spans can be scanned
in packed-value order (or reverse order) without an in-memory sort. If the
requested order is not globally preserved across spans, collect and use the
existing sort path. In particular, primary-key ordering inside each equality
prefix is not a global primary-key ordering across multiple prefixes.

A useful composite acceptance case is an exact first field, an `IN` constraint
on the second field, and ordering by the second and third fields. A partially
matched unique index containing all three fields can satisfy that order across
properly ordered spans. Planner scoring should consider that alternative, not
only an ordinary index with the shortest matching prefix.

### Acceptance tests

Cover primary keys, single-field indices, and composite indices with:

- no explicit sort, compatible ascending/descending sort, and incompatible
  sort;
- limits smaller than a span and spanning multiple values;
- duplicate and absent values;
- additional residual filters;
- `Count`, `Exists`, `IDs`, `List`, `Update`, and `Delete`;
- updates/deletes that modify the scanned index;
- `Stats.PlanPK == 1` for primary-key direct gets;
- `Stats.PlanIndexScan == 1` and no table scan for an ordinary or partially
  matched indexed multi-value query;
- no in-memory sort when the multi-span order itself satisfies the request.

## 2. Normalize same-field constraints

Before selecting an index, combine filters for the same field into a normalized
constraint:

- intersect equality/IN sets;
- remove values excluded by not-equal/NOT IN;
- retain the strongest lower and upper bounds;
- detect contradictions without opening a cursor;
- reduce a singleton IN set to equality.

Besides producing better plans, normalization avoids the current "last filter
wins for planning, all filters run residually" behavior. It also provides the
constraint representation needed by multi-span and grouped-predicate planning.

Normalization happens after public calls have performed their existing
validation and conversion. It must preserve error timing and must not silently
reinterpret calls that currently fail.

## 3. Index-aware string prefixes

### API

Add a predicate with explicit string semantics, for example:

```go
q.FilterPrefix("Name", "gin")
```

It should accept string fields only. Empty prefix should select every value and
can be omitted from the plan.

### Execution

On a compatible string primary key or index, a prefix becomes a bounded scan.
Primary-key strings use their raw byte encoding, while index string components
are NUL-terminated. Bound calculation should operate on the appropriate byte
encoding, not on an ad hoc Unicode successor string. This keeps terminators,
maximum byte values, composite-index suffixes, and descending scans correct.

For a composite index, all fields before the prefix field must have exact
constraints. The prefix occupies the first non-exact index component.

Without a usable index, the predicate remains a native residual check using
`strings.HasPrefix`. It still improves completeness and lets future planner
changes recognize the operation without inspecting a callback.

Tests should include string primary keys, empty strings, non-ASCII UTF-8,
prefixes ending in high bytes, composite indices, ascending/descending
ordering, and strings rejected from indices because they contain NUL.

Expression languages layered above bstore must explicitly translate their
`startsWith` operation to `FilterPrefix`; adding the bstore method alone cannot
make opaque `FilterFn` callbacks index-aware.

## 4. Extend index-covered residual filtering

`exec.go` notes that residual filters may be answerable from index keys. Make
this an explicit covered-query optimization after multi-span planning is
stable.

The executor already avoids loading records for `IDs`, `Count`, and `Exists`
when the chosen PK/index bounds represent every predicate and `plan.filters` is
empty. Preserve and test that behavior. The remaining opportunity is to avoid a
record load when a nominally residual predicate can instead be evaluated from
components of the selected index key.

Potential first targets are:

- comparisons or equality checks on fields available in the selected index key
  but not consumed as scan bounds;
- `IDs`, `Count`, and `Exists` with only those index-answerable residuals;
- limits and compatible ordering that require no record value.

This is generally useful, but should be benchmark-driven. Applications that
materialize full rows, hydrate associated data, or perform authorization after
storage selection will not benefit from ID-only coverage on those paths.

The optimization needs a reliable mapping from index key components to
normalized constraints. It must fall back to loading and parsing the record for
callbacks, pointer/nested predicates, schema-version concerns, or any field
absent from the index.

Acceptance tests should first lock in zero `Stats.Records.Get` calls for fully
represented indexed `IDs`, `Count`, and `Exists`, then assert reduced get counts
for newly covered residuals in addition to identical results. Corrupt-index
behavior must remain compatible: operations that currently require the record
must still diagnose a missing reference, while an already index-only operation
must not silently acquire a new validation promise.

## Deferred performance ideas

Prepared/cached plans should wait for benchmarks. Plan construction currently
embeds concrete packed values, and invalidation must account for registered
schema versions. Multi-span execution and index-covered filtering address
observable scans and record decoding before adding cache complexity.
