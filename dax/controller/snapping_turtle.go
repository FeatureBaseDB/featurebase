package controller

import (
	"context"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/logger"
)

func (c *Controller) snappingTurtleRoutine(period time.Duration, control chan struct{}, log logger.Logger) error {
	if period == 0 {
		return nil
	}
	ticker := time.NewTicker(period)
	for {
		select {
		case <-c.stopping:
			ticker.Stop()
			log.Debugf("Stopping Snapping Turtle")
			return nil
		case <-ticker.C:
			c.snapAll(log)
		case <-control:
			c.snapAll(log)
		}
	}
}

func (c *Controller) snapAll(log logger.Logger) {
	start := time.Now()
	defer func() {
		log.Printf("full snapshot took: %v", time.Since(start))
	}()
	ctx := context.Background()

	tx, err := c.Transactor.BeginTx(ctx, false)
	if err != nil {
		log.Printf("Error getting transaction for snapping turtle: %v", err)
		return
	}
	defer tx.Rollback()

	qdbs, err := c.Schemar.Databases(tx, "")
	if err != nil {
		log.Printf("couldn't get databases: %v", err)
	}

	for _, qdb := range qdbs {
		c.snapAllForDatabase(tx, qdb.QualifiedID(), log)
	}
}

func (c *Controller) snapAllForDatabase(tx dax.Transaction, qdbid dax.QualifiedDatabaseID, log logger.Logger) {
	log.Debugf("snapAllForDatabase: %s", qdbid)
	computeNodes, err := c.Balancer.CurrentState(tx, dax.RoleTypeCompute, qdbid)
	if err != nil {
		log.Printf("Error getting compute balancer state for snapping turtle: %v", err)
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
				log.Printf("couldn't decode a shard out of the job: '%s', err: %v", workerInfo.Jobs[i], err)
			}
			if err := c.snapshotShardData(tx, j.t.QualifiedTableID(), j.shardNum()); err != nil {
				log.Printf("Couldn't snapshot table: %s, shard: %d, error: %v", j.t, j.shardNum(), err)
			}
		}
		i++
	}

	// Get all tables across all orgs/dbs so we can snapshot all keyed
	// fields and look up whether a table is keyed to snapshot its
	// partitions.
	tables, err := c.Schemar.Tables(tx, dax.QualifiedDatabaseID{})
	if err != nil {
		log.Printf("Couldn't get schema for snapshotting keys: %v", err)
		return
	}
	// snapshot keyed fields
	tableMap := make(map[dax.TableKey]*dax.QualifiedTable)
	for _, table := range tables {
		tableMap[table.Key()] = table
		for _, f := range table.Fields {
			if f.StringKeys() && !f.IsPrimaryKey() {
				if err := c.snapshotFieldKeys(tx, table.QualifiedID(), f.Name); err != nil {
					log.Printf("Couldn't snapshot table: %s, field: %s, error: %v", table, f.Name, err)
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
		log.Printf("Error getting translate balancer state for snapping turtle: %v", err)
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
						log.Printf("Couldn't snapshot table: %s, partition: %d, error: %v", table, j.partitionNum(), err)
					}
				}
				log.Printf("couldn't decode a partition out of the job: '%s', err: %v", workerInfo.Jobs[i], err)
			}
		}
		i++
	}
	log.Debugf("snapAllForDatabase complete: %s", qdbid)
}
