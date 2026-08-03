# Predicate completeness

## 1. Grouped Boolean predicates

### Motivation

All current `Filter*` calls combine through `AND`. `FilterEqual` with several
values provides one narrow form of `OR`, but bstore cannot directly represent:

```text
(Status == "draft" && CreatedAt >= cutoff) || Owner == actor
```

or a mixed scalar/slice predicate such as:

```text
(ResourceType == typ && ResourceID == id) || Touches contains entity
```

Callers must use `FilterFn`, which hides fields from the planner and forces full
record parsing for each candidate.

### API direction

Introduce immutable predicate values that a query can validate against `T`,
then attach them to the query. Multiple `Filter` calls, like existing
`Filter*` calls, should combine through `AND`. The exact names need API review,
but a shape like this keeps grouping explicit:

```go
q.Filter(Any(
	All(Equal("ResourceType", typ), Equal("ResourceID", id)),
	InSlice("Touches", entity),
))
```

Existing `FilterEqual`, `FilterLess`, and related methods should lower into the
same internal nodes under an implicit top-level `All`. Do not require callers to
adopt a new expression language.

`NOT` should normally be normalized at construction:

- `NOT equal` to not-equal;
- inverted range operators;
- De Morgan transformation for groups when it does not cause uncontrolled tree
  growth;
- a residual negation node for predicates without a native inverse.

### Planning and execution

Start with correctness through residual evaluation, then recognize indexable
branches. An `OR` plan may execute multiple index/PK spans and union primary
keys. It must deduplicate rows, because branches may overlap and multikey slice
indices may produce the same primary key more than once.

Candidate selection for `OR` must be exhaustive. If any branch has no usable
index restriction, the planner must scan a domain that can contain that entire
branch (normally the table) and evaluate the complete predicate. It is invalid
to union only the indexable branches and rely on a residual predicate to recover
rows that were never selected.

Ordering and limits apply to the union, not independently to each branch.
Compatible branches may be merged in index order; incompatible plans require
collection and the existing in-memory sorting path. Update and delete must
visit each selected primary key once.

Retain a complete residual predicate after candidate selection. Index branches
may narrow candidates but must never redefine Boolean semantics.

### Scope guard

The first version should require every branch to query the same stored type.
Cross-type subqueries and joins are a separate boundary described in
`cross-row-boundaries.md`.

## 2. Nil and zero predicates

bstore stores pointer fields but currently rejects filtering, comparison,
sorting, and indices on them. Applications commonly use `*time.Time` for
soft-deletion or completion state and must write callbacks such as:

```go
q.FilterFn(func(v Row) bool { return v.DeletedAt == nil })
```

Add scan-capable presence predicates first, restricted to pointer fields:

```go
q.FilterNil("DeletedAt")
q.FilterNotNil("DeletedAt")
```

A more general `FilterZero`/`FilterNotZero` could cover pointers plus ordinary
Go zero values, but its behavior for slices, maps, structs, and `time.Time`
must exactly follow bstore's existing `fieldType.isZero` rules. Explicit nil
methods are a smaller initial contract.

Indexing pointer presence or pointed-to values should be a later design. Scan
predicates alone remove callbacks, expose intent to the planner, and improve
query completeness without changing the on-disk index format.

This is intentionally separate from `FilterEqual(field, nil)`: current public
value preparation rejects an untyped nil, and changing equality to accept nil
would broaden an established method's type and error contract.

## 3. Nested scalar field paths

bstore can persist named nested structs but queries address top-level schema
fields. Supporting paths such as `Recipe.Garnish` would let native equality,
comparison, prefix, and string predicates replace callbacks.

Stage this work:

1. Resolve and validate a nested scalar path and evaluate it after parsing a
   record.
2. Decide whether path components use Go field names or persisted bstore names,
   and define interactions with `bstore:"name ..."`, anonymous fields,
   pointers, removed fields, collisions, and historical schema versions.
3. Only then design nested index declarations and encoded keys.

Scan-only nested paths are useful on their own. Nested indices affect schema
metadata, migration, constraint validation, and packing, so they should not be
smuggled into the field-resolution change.

Nested existential paths through slices of structs, such as "any recipe
ingredient has this ID", are a separate and larger feature. They need explicit
`AnyElement` semantics and should not be inferred from dotted strings.

## 4. Slice membership contract

`FilterIn` currently evaluates a value against elements of a top-level slice,
and multikey indices accelerate supported scalar element types. Its comment
describes a string slice even though the implementation and storage type system
are broader.

Make the supported contract precise with tests and documentation for:

- every basic supported scalar element kind;
- named types with supported underlying kinds;
- `time.Time` and binary-marshaled values where equality is well-defined;
- struct elements as a scan-only operation;
- nil versus empty slices;
- duplicate elements and the guarantee that a row is returned once;
- explicit rejection of pointer elements.

If composite or binary-marshaled elements cannot be indexed, the scan behavior
can still be first class. Expanding multikey index encodings for those elements
should be evaluated independently.

All scan equality should reuse `fieldType.equal`, including `time.Time.Equal`,
binary-marshaled byte equality, and stored-field equality for structs. It
should not introduce a second equality definition based on `reflect.DeepEqual`
or Go comparability.

## 5. Scan-only string predicates

After `FilterPrefix`, completeness methods could include:

```go
q.FilterContains("Name", text)
q.FilterSuffix("Name", suffix)
q.FilterRegexp("Name", re)
```

These make intent visible and avoid repeated callbacks, but ordinary B-tree
indices cannot accelerate substring, suffix, or regular-expression matching.
They should be documented as scans unless a separate specialized index is ever
introduced. Accepting a compiled `*regexp.Regexp` avoids repeated compilation
and makes invalid-pattern handling a caller concern.

These convenience predicates are lower priority than grouped Boolean queries,
nil checks, and nested scalar paths because `FilterFn` already provides the
same asymptotic behavior.
