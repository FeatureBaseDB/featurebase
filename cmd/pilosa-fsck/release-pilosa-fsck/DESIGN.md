Design for pilosa-fsck
======================

Problem Background
------------------

Molecula Pilosa provides replication for fault-tolerance within a Pilosa cluster.

Three kinds of data are replicated: Roaring bitmap data, Column-Key translation data,
and Row-Key translations are replicated. Only the first two, Roaring data and Column-Key
translation are relevant here. Broadly, the Roaring bitmap data
forms the central features -- the bits -- of a large, sparse bitmap matrix.
The Column-Keys are the labels for the columns at the top margin of this matrix.

For speed, the Roaring bitmap data is stored separately from the
Key-translation data. The Roaring data is stored in sharded files
within a directory heirarchy under PILOSA-DATA-DIR/index_name/field_name/...
The Key translation data is stored in sharded BoltDB databases with
in the PILOSA-DATA-DIR/index_name/_key directory.

The current approach to Roaring file replication involves an
eventually consistent mechanism that uses an Anti-Entropy agent to
fix partial or incomplete replication from the primary shard to all
replica shards.

Unfortunately, the Anti-Entropy agent approach has proved inadequate on two
fronts. First, it does not provide for immediately consistent reads in the
event that the primary is lost. Second, the Anti-Entropy agent itself experienced
out-of-memory issues that have not yet been resolved.

Therefore, work is now underway to replace this replication
approach with a more consistent design. 

However, in the meantime, for our customers in production with Molecula
Pilosa, we wish to provide a means to re-establish correct replication.

The pilosa-fsck tool can therefore been seen as a temporary, stop-gap
measure to address immediate issues while the cluster replication
mechanism is replaced.

The second factor motivating the creation of pilosa-fsck was the discovery
of a bug in the Key-translation process. Unfortunately this was a hard
to reproduce bug, since it happened only on the customer's premises.
It happened only after running the system for a long time, with a
large amount of data, and with various eccentric node failures
and recoveries.

However, we were able to reproduce a plausible explanation.
Non-primary replicas were creating keys when they should have been
forwarding the request to the primary. Correcting this bug is impetus
for the release of the v2.1.4 version of Molecula Pilosa.

A fine point here: since we were not able to precisely reproduce the customer's
issue in the development environment, we cannot guarantee with 100%
certainty that we have actually addressed the bug that the customer
was seeing.

Therefore we also desired an additional insurance
policy. We wished to be able to empower customers to pro-actively discover any
future Key-translation issues that happen in their on-premise system.

To do this, we proposed providing select customers with the pilosa-fsck
tool which can analyze their offline backups for issues.

Optionally, these issues can also be repaired in-place in the
offline backup on which pilosa-fsck is run.
The -fix flag repairs both kinds of replication issues.

Solution Approach: mechanism of action
--------------------------------------

The pilosa-fsck is run offline on a full set of backups taken from
all nodes in a Pilosa cluster. It runs on a single computer that
must be separate from the production or staging Pilosa environments.

When run, pilosa-fsck analyzes the differences between the
primary and its replicas. Both the Roaring
files and the Key translation databases are analyzed.
The computer running pilosa-fsck must have the same or more
memory as the Pilosa nodes in the cluster, as it will
"pretend" to be each Pilosa node in turn. However, as each
node's backup is closed before the next node's backup is
read, we do not require substantially more memory than a single
production node. Short Blake3 cryptographic checksums are
computed for each Roaring fragment and each Key translation
database. These are held in memory (and printed to the log)
for comparing nodes. This comparison forms the heart of
the consistency checks, and is the basis for any subsequent
repair.

We recommend capturing both stdout and stderr to a log.
Use `&> log` or  `2>&1 > log` at the end of the
pilosa-fsck invocation to save a log of the run to disk.

In a typical cluster, the Replication factor R may be less
than the number nodes N in the cluster. For example, while
N may be 4, the R may be only 3. In this example, within
each replicated shard, one node will be the primary for
that shard, two nodes will be "regular" non-primary replicas, and one
node will be a non-replica. Note that the designation
of primary changes for different Roaring shard within an index,
even on a single node.

The essence of the the -fix repair operation that pilosa-fsck
can do is this: it will copy from the primary to the
the non-primary replicas. Further, it will remove data from
any non-replica node if it was mistakenly present.

The pilosa-fsck output log will contain
a sequence of command line 'cp' and 'rm' commands.
These commands are merely a record (with
accompanying justifcation in the comment following the
command) of what actions would be performed to repair
the Roaring file data.

Only with -fix will the repair actions actually happen
during the pilosa-fsck run.

The Key translation repairs must be made to the BoltDB
databases directly, and so cannot be represented by
simple cp/rm commands. Therefore if any key-translation
repairs are indicated, and if the user wishes to
make those repairs to the backups, then the -fix
flag must be used.

If only the Roaring files needs repair, then it suffices
to chmod +x and run the output log, which is in the
form of a bash shell script with comments.

Details: running pilosa-fsck
----------------------------

Errors in invocation are reported on stderr and the program will exit with a non-zero
error code if invocation errors are present. A non-zero error code
is returned if a repair is needed.

The log of the run is printed to stdout.

The -h flag to pilosa-fsck prints a summary of its operation
and a guide to laying out the backup directories in preparation.

The help is reproduced below.

~~~
jaten@twg-pilosa0 ~/pilosa/cmd/pilosa-fsck (fsck-design) $ pilosa-fsck -h
pilosa-fsck version: Molecula Pilosa v2.2.1-43-g61047fde (Oct  5 2020 11:51AM, 61047fde)

Use: pilosa-fsck -replicas R {-fix} {-q} /backup/1/.pilosa /backup/2/.pilosa ... /backup/N/.pilosa

  -fix
    	(warning: alters the backed-up node images on disk) copy primary data to replicas to create a consistent cluster.

  -replicas R 
        (required) R is a positive integer, giving the replicaN or replicator factor for the cluster. This is
        the number of replicas maintained in the cluster. Must be the same as the 
        [cluster] 'replicas = R' entry shared across all the pilosa.conf files on each node.

  -q
        be very quiet during analysis and repair


Welcome to pilosa-fsck. This is a scan and repair 
tool that is modeled after the classic unix file 
system utility fsck. 

WARNING: DO NOT RUN ON A LIVE SYSTEM.

The most important point to remember is that analysis
and repair must be done *offline*.

Just as fsck must be run on an unmounted disk, 
pilosa-fsck must be run on a backup. It must 
not be run on the directories where a live Pilosa system
is serving queries. Instead, take a backup first.
A backup is a set of N cluster-node directories that have been
copied from your live system. They must all 
be visible and mounted on one filesystem together.

pilosa-fsck can be run in scan-mode (without -fix),
or in repair-mode with -fix. The console output
supplies a log documenting the analysis 
and showing what data changes would have been made.

REQUIRED COMMAND LINE ARGUMENTS

The paths to all the top-level pilosa 
directories in a cluster must be given on the command
line. The -replicas R flag is also always required. It
must be correct for your cluser. Here R is the same as
the [cluster] stanza "replicas = R" line from your
pilosa.conf.

Example:

Suppose you are ready to run pilosa-fsck: 
you have taken a backup of your four node Pilosa 
cluster and stored it all on one filesystem with 
all nodes visible and uncompressed. This
is a pre-requisite to running pilosa-fsck. 
Let's suppose we have replication R = 3 set.
In this example, have stored our backed-up directories in 

/backup/molecula

and the four node backups are in 
subdirectories node1/ node2/ node3/ node4/ under this:

/backup/molecula/node1/
/backup/molecula/node1/.pilosa/.id
/backup/molecula/node1/.pilosa/.topology
/backup/molecula/node1/.pilosa/myindex

/backup/molecula/node2/
/backup/molecula/node2/.pilosa/.id
/backup/molecula/node2/.pilosa/.topology
/backup/molecula/node2/.pilosa/myindex

/backup/molecula/node3/
/backup/molecula/node3/.pilosa/.id
/backup/molecula/node3/.pilosa/.topology
/backup/molecula/node3/.pilosa/myindex

/backup/molecula/node4/
/backup/molecula/node4/.pilosa/.id
/backup/molecula/node4/.pilosa/.topology
/backup/molecula/node4/.pilosa/myindex

NOTE: your .pilosa directories need not be named .pilosa. They can
be something else, such as when the -d flag to pilosa server was used.
The .id and .topology and index directories must be found directly underneath.

Then a typical invocation to scan a cluster backup for issues:

$ cd /backup/molecula/
$ pilosa-fsck -replicas 3 node1/.pilosa node2/.pilosa node3/.pilosa node4/.pilosa

A typical invocation to repair the replication in the same backup:

$ pilosa-fsck -replicas 3 -fix node1/.pilosa node2/.pilosa node3/.pilosa node4/.pilosa

In both cases, the .id and .topology files must 
be present in the backups.

Without -fix, no modifications will be made to the backups. Only
by running with -fix will repairs be made. The user can safely
always run with -fix to repair only if needed.

A zero error code will be returned to the shell if no repairs were needed.

A zero error code will be also be returned to the shell if 
repairs were needed and they were accomplished under -fix.

A non-zero error code indicates that repairs were needed but
were not made.

~~~
