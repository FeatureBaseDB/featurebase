package controller

import (
	"context"
	"time"

	"github.com/molecula/featurebase/v3/dax"
)

func (c *Controller) snappingTurtleRoutine(period time.Duration, control chan struct{}) {
	if period == 0 {
		return // disable automatic snapshotting
	}
	ticker := time.NewTicker(period)
	for {
		select {
		case <-c.stopping:
			ticker.Stop()
			c.logger.Debugf("TURTLE: Stopping Snapping Turtle")
			return
		case <-ticker.C:
			c.snapAll()
		case <-control:
			c.snapAll()
		}
	}

}

func (c *Controller) snapAll() {
	c.logger.Debugf("TURTLE: snapAll")
	ctx := context.Background()

	tx, err := c.boltDB.BeginTx(ctx, false)
	if err != nil {
		c.logger.Printf("Error getting transaction for snapping turtle: %v", err)
		return
	}
	defer tx.Rollback()

	qdbs, err := c.Schemar.Databases(tx, "")
	if err != nil {
		c.logger.Printf("couldn't get databases: %v", err)
	}

	for _, qdb := range qdbs {
		c.snapAllForDatabase(tx, qdb.QualifiedID())
	}

	c.logger.Debugf("TURTLE: snapAll complete")
}

func (c *Controller) snapAllForDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID) {
	c.logger.Debugf("TURTLE: snapAllForDatabase: %s", qdbid)
	computeNodes, err := c.Balancer.CurrentState(tx, dax.RoleTypeCompute, qdbid)
	if err != nil {
		c.logger.Printf("Error getting compute balancer state for snapping turtle: %v", err)
	}

	// Weird nested loop for snapshotting shard data. The reason for
	// this is to avoid hotspotting each node in turn and spread the
	// snapshotting load across all nodes rather than snapshotting all
	// jobs on one node and then moving onto the next one.
	i := 0
	stillWorking := true
	for stillWorking {
		stillWorking = false
		for _, workerInfo := range computeNodes {
			if len(workerInfo.Jobs) <= i {
				continue
			}
			stillWorking = true
			j, err := decodeShard(workerInfo.Jobs[i])
			if err != nil {
				c.logger.Printf("couldn't decode a shard out of the job: '%s', err: %v", workerInfo.Jobs[i], err)
			}
			if err := c.snapshotShardData(tx, j.t.QualifiedTableID(), j.shardNum()); err != nil {
				c.logger.Printf("Couldn't snapshot table: %s, shard: %d, error: %v", j.t, j.shardNum(), err)
			}
		}
		i++
	}

	// Get all tables across all orgs/dbs so we can snapshot all keyed
	// fields and look up whether a table is keyed to snapshot its
	// partitions.
	tables, err := c.Schemar.Tables(tx, dax.QualifiedDatabaseID{})
	if err != nil {
		c.logger.Printf("Couldn't get schema for snapshotting keys: %v", err)
		return
	}
	// snapshot keyed fields
	tableMap := make(map[dax.TableKey]*dax.QualifiedTable)
	for _, table := range tables {
		tableMap[table.Key()] = table
		for _, f := range table.Fields {
			if f.StringKeys() && !f.IsPrimaryKey() {
				if err := c.snapshotFieldKeys(tx, table.QualifiedID(), f.Name); err != nil {
					c.logger.Printf("Couldn't snapshot table: %s, field: %s, error: %v", table, f.Name, err)
				}
			}
		}
	}

	// Get all partition jobs from balancer and snapshot table keys
	// for any partition that goes with a keyed table. Doing the same
	// weird nested loop thing to avoid doing all jobs on one node
	// back to back.
	translateNodes, err := c.Balancer.CurrentState(tx, dax.RoleTypeTranslate, qdbid)
	if err != nil {
		c.logger.Printf("Error getting translate balancer state for snapping turtle: %v", err)
	}

	i = 0
	stillWorking = true
	for stillWorking {
		stillWorking = false
		for _, workerInfo := range translateNodes {
			if len(workerInfo.Jobs) <= i {
				continue
			}
			stillWorking = true
			j, err := decodePartition(workerInfo.Jobs[i])
			if err != nil {
				table := tableMap[j.table()]
				if table.StringKeys() {
					if err := c.snapshotTableKeys(tx, table.QualifiedID(), j.partitionNum()); err != nil {
						c.logger.Printf("Couldn't snapshot table: %s, partition: %d, error: %v", table, j.partitionNum(), err)
					}
				}
				c.logger.Printf("couldn't decode a partition out of the job: '%s', err: %v", workerInfo.Jobs[i], err)
			}
		}
		i++
	}
	c.logger.Debugf("TURTLE: snapAllForDatabase complete: %s", qdbid)
}
