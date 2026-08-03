# Cross-row and specialized-index boundaries

Some application predicates cannot be made into a bstore field filter without
substantially expanding the database's scope. They should remain explicit
application planning concerns unless bstore intentionally adopts the broader
feature.

## Normalized associations and tags

An application may store tags or other many-to-many metadata in a separate row
type keyed by target type, target ID, and association key. A predicate such as
"drink has tag `featured`" is then a semijoin, not slice membership on the
drink row.

Available application strategies are:

- query the association type for matching target IDs, then use `FilterIDs` or
  multi-value equality on the owning row type;
- hydrate associations in one type-scoped batch inside the same read
  transaction and evaluate a residual predicate;
- deliberately denormalize a slice onto the owning row when that ownership and
  consistency tradeoff is acceptable.

Prefer `FilterIDs` when those target IDs are the owning type's primary keys and
the application can construct the correctly typed slice. Multi-value equality
planning remains important when the target is a secondary field or one
component of a composite index.

Grouped predicates within one stored type do not solve an `OR` between an owner
field and a separate association query. That requires a cross-type subquery or
an application-side union. bstore currently declares joins out of scope, so a
tag-specific shortcut would be an awkward partial relational feature.

## Derived application projections

Applications often expose one logical field assembled from several stored
columns, such as a typed entity UID formed from `ResourceType` and `ResourceID`.
bstore can efficiently filter the underlying columns, especially with a
composite index, but it should not parse an application-specific external
representation.

The application planner should recognize literal derived values, validate and
split them, then emit native column predicates. The complete application
predicate remains the semantic check.

## Nested collections

A row may contain slices of structs and nested slices. Queries such as "any
ingredient or substitute equals this entity" require existential traversal,
not simple top-level `FilterIn`.

A future bstore feature would need explicit rules for:

- `any` versus `all` element semantics;
- correlation when several conditions must match the same element;
- nil and empty collections;
- duplicate matching elements and row deduplication;
- nested multikey index encoding and index-size growth.

Until that design exists, `FilterFn` is the honest interface. Nested scalar
field paths can proceed independently without promising collection queries.

## Substring, suffix, and regular-expression search

Native scan predicates improve API completeness but do not make these searches
index-backed. Trigram, suffix, or full-text indices would add tokenization,
normalization, schema, migration, and storage policies well beyond a normal
B-tree field index.

Do not describe scan-only methods as performance pushdown. They avoid opaque
callbacks and may combine with other indexed conjuncts, but each remaining
candidate record still requires string evaluation.

## Aggregates and field-to-field expressions

Aggregate predicates and cross-row calculations need execution machinery that
bstore does not currently provide. Field-to-field comparisons within one row
could be represented as residual native predicates, but they have little index
value and should follow the higher-frequency completeness items.

## Decision rule

A proposed first-class predicate is a good fit when it:

1. can be validated against one registered Go type;
2. has unambiguous zero, equality, and ordering semantics;
3. can run as a residual check even without an index;
4. can later inform candidate selection without changing its meaning; and
5. composes with sorting, limits, mutation, and transactions on that same type.

Features that fail the first or fifth condition should remain in application
planning or begin as a separate, explicit expansion of bstore's scope.
