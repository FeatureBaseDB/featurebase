// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.

/*
Package querycontext provides a semi-transactional layer wrapping access
to multiple underlying transactional databases.

# Background

Within a single transactional database, a transaction provides the
ability to make multiple changes atomically (either all or none are
visible, you can never observe only some of them), and manages the
lifetime of data access; for instance, a database could return objects
which point into database-managed storage, but which will be invalidated
after the transaction completes. A transaction also provides a stable
view of the data; even if you aren't writing, a transaction you open
will be able to see the same data across multiple queries, even if
changes are being made to the database.

Featurebase uses multiple transactional databases in parallel, and we
want to preserve transactional semantics across these databases as well
as we can. We want to be able to see a consistent view of each database,
to be able to make multiple changes in sequence which are committed
atomically, and to ensure that data we're still using isn't invalidated.

We can approximate this by having an object which tracks transactions
for each individual database, and then closes them all at once, or commits
them all at once. However, if we do this naively, it becomes very likely
for us to end up deadlocked -- we can have two queries running each of
which is holding a lock, and will hold it until it finishes running,
and each of which is waiting for access to a lock the other is holding.

The QueryContext offers a resolution to this by tracking the scope of
each query's prospective writes. Queries register their prospective writes
when they're created, and cannot begin running (potentially acquiring
locks) until there are no existing queries that could contest any locks
with them.

# Data Organization

The overall data set maintained by Featurebase is divided into Indexes
(which correspond roughly to SQL tables), Fields, Views, and Shards. Shards
divide the set of records into blocks of adjacent records, while the
Index/Field/View hierarchy corresponds more to tables and columns. The
intersection of {index, field, view, shard} is called a fragment. The
QueryContext/TxStore design abstracts away the question of which fragments
are stored in which backend database files; you specify your scope in
terms of indexes, fields, and shards, and you request access to fragments.

# Usage

The major exported types from this package are [KeySplitter], [QueryContext],
[QueryScope], and [TxStore]. In general, the usage of the package is that
you create a TxStore representing your underlying collection of databases,
and using a KeySplitter to determine how the overall data stored is split
into individual database files. When you wish to operate on data, you
request a new QueryContext from the TxStore. If you want to write, you
use a QueryScope to identify the scope of which parts of the data you
want to write to. (The request for a QueryContext will block until it
can be satisfied.) The QueryContext then provides read and write access
to individual fragments, and handles any necessary multiplexing between
fragment access and database access.
*/
package querycontext
