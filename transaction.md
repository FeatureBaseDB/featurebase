# Initial Transaction Support

This is not full-featured transaction support with commit and rollback
for now; this is a placeholder intended to allow us to solve shorter-term
problems.

The primary purpose of this is to allow an exclusive transaction to
block new ingest activity from starting, while permitting existing ingest
operations to complete, even if a single ingest requires multiple operations.
This allows users with cooperating ingest operations to ensure a stable state
for the data on disk before triggering snapshots or other writes.

## Overview: What transactions are

A transaction reflects an ongoing set of related operations that may be
occurring in multiple or distinct messages. There is no support for
rolling back a failed transaction. Transactions can coexist, and there's
nothing controlling simultaneous access to fields.

However, a transaction can be exclusive. An exclusive transaction cannot
start until other transactions complete, but no non-exclusive transaction
can start while an exclusive transaction is waiting.

Transactions are holder-wide, not index-specific. Transactions are also
presumably cluster-wide.

### API Details

The base transaction endpoints are `/transactions`, for listing or creating
transactions, and `/transaction/[id]`, for listing, creating, finishing, or
cancelling a transaction.

A POST to `/transactions` attempts to create a transaction, assigning it an
arbitrary ID that is not the ID of any existing transaction. A `GET` from
`/transactions` lists existing transactions.

A POST to `/transaction/[id]` tries to create a transaction with the given
ID, failing if it can't for any reason, including the reason "this ID is
already in use". A GET from `/transaction/[id]` retrieves information about
the transaction.

When creating a transaction, you may specify an options object:

	```
	{
		"exclusive": true, // default is false
		"timeout": 300     // in seconds, default is 300
	}
	```

For an exclusive transaction, you may also specify the optional parameter
"pause-snapshots" as a boolean. A `true` value indicates that the snapshot
queue should be paused once this transaction becomes active. *Note that pausing
the snapshot queue can cause some write operations to block indefinitely.*
If a transaction requests that the snapshot queue be paused, it will not
report itself "active" until the snapshot queue has completed any outstanding
snapshots and paused itself. The full sequence of events, then, is:

* Stop allowing new transactions to start.
* Wait for transactions to complete.
* Pause snapshot queue.
* Wait for snapshot queue to report that it's successfully paused.
* Transition to active state.

Exclusive transactions which pause the snapshot queue should not write to
the database; this is used as a way to block activity so backups can be made.

When requesting information about a transaction, you get back an object:

	```
	{
		"active": true,
		"timeout": 300,       // timeout time in seconds
		"stats": {
			"idle": 0,    // time in seconds since last activity
			"queries": 3, // queries submitted in this transaction
			"errors": 0   // errors produced by queries
		}
	}
	```

To mark a transaction as complete, you POST to `/transaction/[id]/finish`, and
get back the same information you'd have gotten from a GET for that transaction.
The finish request may block if any existing queries are running as part of
that transaction, but immediately prevents any new queries from starting for
that transaction.

Queries can be associated with a transaction by including
`X-Pilosa-Transaction: [id]` in their request headers. A transaction's idle
timer is reset by any query against it, even a query which doesn't write
anything.

When an exclusive transaction is created, it does not necessarily start out
in the `active` state. It immediately blocks the starting of new non-exclusive
transactions, but does not transiction to an `active` state until existing
transactions complete. During this time, a GET to it should return:

	```
	{
		"active": false,
		"blocked-by": [ "id" ]
	}
	```

where blocked-by is a list of the IDs of any transactions blocking the
transition.

If multiple exclusive transactions are requested, they become active
sequentially in the order the requests came in, and the snapshot queue and
other transactions are not permitted to resume until the exclusive transactions
all complete.



### Implementation Notes

All requests go through coordinator.

When creating a new transaction, we'll create it on every node in the
cluster and persist it to disk.

Only the coordinator will accept requests to start a transaction.

Timeouts only expire when there has been *no activity* on a transaction for the timeout duration. 
Any activity on the transaction may extend the deadline (unimplemented).

When finishing a transaction, we'll finish it on the coordinator and
then broadcast the finish to the cluster before returning to the
client.

When getting an exclusive transaction, if the transaction is active,
we'll make sure that all nodes agree before returning it.


Coordinator forwards all requests to every other node so they can stay
in sync. If the coordinator doesn't hear back from a node, the request
fails. The coordinator only reaches out to active nodes, so if the
cluster is in DEGRADED, things can still continue.

If an node is down and comes back up it needs to synchronize its state
with the coordinator (unimplemented).

There is a separate TransactionManager and TransactionStore

The store is just responsible for persisting info about
transactions. The manager handles all the logic (at the node level).
Logic related to cluster and remote vs local node is handled by the
Server. The Holder contains the TransactionManager, and the Server
contains the logic for how to handle external vs intra cluster
requests (remote==true).

There is intra-cluster messaging for transactions which is handled
with the new TransactionMessage and goes through the usual
SendMessage/Broadcaster stuff.

There is also external API which is handled by the HTTP handler and
goes through API (and is passed directly to Server). (unimplemented)


#### TODO

- [x] implement api layer and cluster logic, startup, etc.
- [ ] add new cluster state to explicitly reject certain requests during exclusive transaction?
- [x] implement HTTP layer
- [ ] implement transaction id in header
- [x] propagate context
- [ ] implement and use persistent transaction store rather than inmem.
- [ ] update go-pilosa/gpexp to actually USE transactions
  - [ ] update IDK to use updated go-pilosa
- [ ] external testing with e.g. curl
  
- ID validation. No slashes, no non-URL safe chars

#### Testing TransactionManager
- there should never be more than one Exclusive transaction
- if the Exclusive transaction is active, there should be no other transactions


### Documentation

Before performing a backup, you must request an exclusive "transaction" with the cluster. Do this via and HTTP POST to the coordinator node at path:

`/transaction` OR `/transaction/{id}` if you wish to specify a custom ID (any alphanum+dash). Otherwise a UUID will be generated and returned in the response.

Use headers:

```
Accept: application/json
Content-Type: application/json
```

And body like:

```
{
  "timeout": "10m",
  "exclusive": true
}
```

You may choose any timeout you like, though it's better to err on the
longer side of how long you expect the backup to take. You explicitly
finish the transaction once you're done, so the timeout exists solely
for cleanup in the case of failures.

This will return a JSON "transaction response" object.
```
{
"transaction": {
  "id":"5e572d95-4204-40cd-804c-92976b68dc9b",
  "active":true,
  "exclusive":false,
  "timeout":"1m0s",
  "deadline":"2020-04-17T21:54:18.69359-05:00"
  },
"error":"some message"
}
```

The `error` field MAY not be present if there is no error.

You MUST check whether `active` is true. If not, you must poll the transaction endpoint with a GET request and your ID until it is true. This looks like:

GET `/transaction/5e572d95-4204-40cd-804c-92976b68dc9b`

with headers:

```
Accept: application/json
```

and also returns a "transaction response" object.

Once an "active", "exclusive" transaction is returned, proceed with your backup.

Once the backup is complete, finish the transaction with

POST `/transaction/{id}/finish`

with headers:

```
Accept: application/json
```

