// Copyright 2023 Molecula Corp (DBA FeatureBase). All rights reserved.
package keys

/*
Package keys provides data types and structures that represent the logical
structure of a featurebase database. This package is separate from the
main package so it can be imported by other things which are also imported
by the main package. That may some day become unnecessary.

Featurebase's design sorts data into indexes (comparable to SQL tables),
fields (comparable to SQL columns), views (no real comparison in SQL),
and shards. The data denoted by {index, field, view, shard} is called a
"fragment", and is itself a bitmap of 2^64 logical bits.

Individual backend data storage may not correspond to this layout. The
keys package provides data types representing how high-level Featurebase
models and uses these data types, and does not address the question of how
to make that make sense for a given storage backend.
*/
