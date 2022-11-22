package dax

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	fb "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/controller"
	mdshttp "github.com/molecula/featurebase/v3/dax/mds/http"
	queryerhttp "github.com/molecula/featurebase/v3/dax/queryer/http"
	"github.com/molecula/featurebase/v3/dax/test"
	"github.com/molecula/featurebase/v3/dax/test/datagen"
	"github.com/molecula/featurebase/v3/dax/test/docker"
	"github.com/molecula/featurebase/v3/dax/test/featurebase"
	"github.com/molecula/featurebase/v3/dax/test/inspector"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/sql3/test/defs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDAXIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	mdsStub := "mds"
	computerStub := "computer"
	queryerStub := "queryer"
	writeloggerStub := "writelogger"
	snapshotterStub := "snapshotter"
	datagenStub := "datagen"
	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getting CWD: %v", err)
	}
	coverVolume := docker.Volume{
		Type:   "bind",
		Source: filepath.Join(mydir, "../../../coverage-from-docker"),
		Target: "/results",
	}

	mdsNetworkName := "mds-network"

	addressFn := func(name string) dax.Address {
		return dax.Address(name + ":8080")
	}

	qual := dax.NewTableQualifier("acme", "db1")

	require := require.New(t)

	t.Run("SQL", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		fcName := uniqueContainerName(t, computerStub)
		fc := featurebase.NewContainer(fcName, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "0s",
			"FEATUREBASE_COMPUTER_RUN":                          "true",
			"FEATUREBASE_QUERYER_RUN":                           "true",
		})

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc).
			WithService(featurebase.ImageName, fc).
			WithNetwork(uniqueNetworkName(t, featurebase.NetworkName))

		defer dc.Down()

		// start featurebase.featurebase with no errors
		require.NoError(dc.Up())
		waitForHealthy(t, fc, "", 10, time.Second)
		waitForHealthy(t, fc, dax.ServicePrefixMDS, 10, time.Second)
		waitForHealthy(t, fc, dax.ServicePrefixComputer, 10, time.Second)
		waitForHealthy(t, fc, dax.ServicePrefixQueryer, 10, time.Second)
		waitForStatus(t, fc, "NORMAL", 10, time.Second)

		tableTests := defs.TableTests

		// skips is a list of tests which are currently not passing in dax. We
		// need to get these passing before alpha.
		skips := []string{
			"testinsert/test-5", // error messages differ
			"table-82/test-3",
			"table-82/test-8",
			"table-83/test-3",
			"table-83/test-8",
			"cast_int/test-2",
			"cast_int/test-7",
			"cast_id/test-2",
			"cast_string/test-12",
			"cast_ts/test-7",
			"sum_test/test-5",
			"percentile_test/test-6",
			"minmax_test/test-7",
			"minmax_test/test-8",
			"groupby_test/test-5",
			"groupby_test/test-6",
			"innerjointest/innerjoin-aggregate-groupby",
		}

		doSkip := func(name string) bool {
			for i := range skips {
				if skips[i] == name {
					return true
				}
			}
			return false
		}

		for i, test := range tableTests {
			t.Run(test.Name(i), func(t *testing.T) {

				// Create a table with all field types.
				if test.HasTable() {
					sqlCheck(t,
						fc,
						addressFn(fcName),
						qual,
						test.CreateTable(),
						`{"schema":{"fields":[]},"data":[],"error":"","warnings":null}`,
					)
				}

				// Populate fields with data.
				if test.HasTable() && test.HasData() {
					sqlCheck(t,
						fc,
						addressFn(fcName),
						qual,
						test.InsertInto(t),
						`{"schema":{"fields":[]},"data":[],"error":"","warnings":null}`,
					)
				}

				for j, sqltest := range test.SQLTests {
					t.Run(sqltest.Name(j), func(t *testing.T) {
						if doSkip(test.Name(i) + "/" + sqltest.Name(j)) {
							t.Skip("in dax skips list")
						}
						for _, sql := range sqltest.SQLs {
							t.Run(fmt.Sprintf("sql-%s", sql), func(t *testing.T) {
								log.Printf("SQL: %s", sql)
								rows, headers, err := mustQueryRows(t, fc, addressFn(fcName), qual, sql, "")

								// Check expected error instead of results.
								if sqltest.ExpErr != "" {
									if assert.Error(t, err) {
										assert.Contains(t, err.Error(), sqltest.ExpErr)
									}
									return
								}

								require.NoError(err)

								// Check headers.
								assert.ElementsMatch(t, sqltest.ExpHdrs, headers)

								// make a map of column name to header index
								m := make(map[dax.FieldName]int)
								for i := range headers {
									m[dax.FieldName(headers[i].Name)] = i
								}

								// Put the expRows in the same column order as the headers returned
								// by the query.
								exp := make([][]interface{}, len(sqltest.ExpRows))
								for i := range sqltest.ExpRows {
									exp[i] = make([]interface{}, len(headers))
									for j := range sqltest.ExpHdrs {
										targetIdx := m[sqltest.ExpHdrs[j].Name]
										assert.GreaterOrEqual(t, len(sqltest.ExpRows[i]), len(headers),
											"expected row set has fewer columns than returned headers")
										exp[i][targetIdx] = sqltest.ExpRows[i][j]
									}
								}

								if sqltest.SortStringKeys {
									sortStringKeys(rows)
								}

								switch sqltest.Compare {
								case defs.CompareExactOrdered:
									assert.Equal(t, len(sqltest.ExpRows), len(rows))
									assert.EqualValues(t, exp, rows)
								case defs.CompareExactUnordered:
									assert.Equal(t, len(sqltest.ExpRows), len(rows))
									assert.ElementsMatch(t, exp, rows)
								case defs.CompareIncludedIn:
									assert.Equal(t, sqltest.ExpRowCount, len(rows))
									for _, row := range rows {
										assert.Contains(t, exp, row)
									}
								}
							})
						}
					})
				}
				for j, pqltest := range test.PQLTests {
					t.Run(pqltest.Name(j), func(t *testing.T) {
						if doSkip(test.Name(i) + "/" + pqltest.Name(j)) {
							t.Skip("in dax skips list")
						}
						for _, pql := range pqltest.PQLs {
							t.Run(fmt.Sprintf("pql-%s", pql), func(t *testing.T) {
								log.Printf("PQL: %s", pql)
								rows, headers, err := mustQueryRows(t, fc, addressFn(fcName), qual, pql, pqltest.Table)

								// Check expected error instead of results.
								if pqltest.ExpErr != "" {
									if assert.Error(t, err) {
										assert.Contains(t, err.Error(), pqltest.ExpErr)
									}
									return
								}

								require.NoError(err)

								// Check headers.
								assert.ElementsMatch(t, pqltest.ExpHdrs, headers)

								// make a map of column name to header index
								m := make(map[dax.FieldName]int)
								for i := range headers {
									m[dax.FieldName(headers[i].Name)] = i
								}

								// Put the expRows in the same column order as the headers returned
								// by the query.
								exp := make([][]interface{}, len(pqltest.ExpRows))
								for i := range pqltest.ExpRows {
									exp[i] = make([]interface{}, len(headers))
									for j := range pqltest.ExpHdrs {
										targetIdx := m[pqltest.ExpHdrs[j].Name]
										assert.GreaterOrEqual(t, len(pqltest.ExpRows[i]), len(headers),
											"expected row set has fewer columns than returned headers")
										exp[i][targetIdx] = pqltest.ExpRows[i][j]
									}
								}

								assert.Equal(t, len(pqltest.ExpRows), len(rows))
								assert.EqualValues(t, exp, rows)

							})
						}
					})
				}
			})
		}

		// time.Sleep(30000 * time.Second)
	})

	t.Run("Datagen", func(t *testing.T) {
		imagePull(t, datagen.ImageName)

		// datagen
		gc := datagen.NewContainer(uniqueContainerName(t, datagenStub), addressFn(""))

		dc := new(docker.Composer).
			WithService(datagen.ImageName, gc).
			WithNetwork(uniqueNetworkName(t, datagen.NetworkName))

		defer dc.Down()

		t.Run("start datagen with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
		})
	})

	t.Run("FeatureBase", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		fc := featurebase.NewContainer("base", nil)

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc).
			WithService(featurebase.ImageName, fc).
			WithNetwork(uniqueNetworkName(t, featurebase.NetworkName))

		defer dc.Down()

		t.Run("start featurebase with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, fc, "", 10, time.Second)
		})
	})

	t.Run("FeatureBase_MDS", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		fc := featurebase.NewContainer(uniqueContainerName(t, mdsStub), map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
		})

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc).
			WithService(featurebase.ImageName, fc).
			WithNetwork(uniqueNetworkName(t, featurebase.NetworkName))

		defer dc.Down()

		t.Run("start featurebase.mds with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, fc, "", 10, time.Second)
			waitForHealthy(t, fc, dax.ServicePrefixMDS, 10, time.Second)
		})
	})

	// Queryer requires MDS to run.
	t.Run("FeatureBase_Queryer_MDS", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		fc := featurebase.NewContainer(uniqueContainerName(t, queryerStub), map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
			"FEATUREBASE_QUERYER_RUN":                           "true",
		})

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc).
			WithService(featurebase.ImageName, fc).
			WithNetwork(uniqueNetworkName(t, featurebase.NetworkName))

		defer dc.Down()

		t.Run("start featurebase.queryer with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, fc, "", 10, time.Second)
			waitForHealthy(t, fc, dax.ServicePrefixQueryer, 10, time.Second)
		})
	})

	t.Run("FeatureBase_WriteLogger", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		fc := featurebase.NewContainer(uniqueContainerName(t, writeloggerStub), map[string]string{
			"FEATUREBASE_WRITELOGGER_RUN": "true",
		})

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc).
			WithService(featurebase.ImageName, fc).
			WithNetwork(uniqueNetworkName(t, featurebase.NetworkName))

		defer dc.Down()

		t.Run("start featurebase.writelogger with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, fc, "", 10, time.Second)
			waitForHealthy(t, fc, dax.ServicePrefixWriteLogger, 10, time.Second)
		})
	})

	t.Run("FeatureBase_Snapshotter", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		fc := featurebase.NewContainer(uniqueContainerName(t, snapshotterStub), map[string]string{
			"FEATUREBASE_SNAPSHOTTER_RUN": "true",
		})

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc).
			WithService(featurebase.ImageName, fc).
			WithNetwork(uniqueNetworkName(t, featurebase.NetworkName))

		defer dc.Down()

		t.Run("start featurebase.snapshotter with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, fc, "", 10, time.Second)
			waitForHealthy(t, fc, dax.ServicePrefixSnapshotter, 10, time.Second)
		})
	})

	// Computer requires MDS to run.
	t.Run("FeatureBase_Computer", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		fcName := uniqueContainerName(t, computerStub)
		fc := featurebase.NewContainer(fcName, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
			"FEATUREBASE_COMPUTER_RUN":                          "true",
		})

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc).
			WithService(featurebase.ImageName, fc).
			WithNetwork(uniqueNetworkName(t, featurebase.NetworkName))

		defer dc.Down()

		t.Run("start featurebase.featurebase with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, fc, "", 10, time.Second)
			waitForHealthy(t, fc, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc, "NORMAL", 10, time.Second)
		})

		t.Run("check the schema endpoint", func(t *testing.T) {
			inspect(t,
				fc,
				addressFn(fcName),
				"computer/schema",
				nil,
				getExpFEqual(`{"indexes":[]}`),
			)
		})
	})

	t.Run("MDS_FeatureBase", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		mcName := uniqueContainerName(t, mdsStub)
		mc := featurebase.NewContainer(mcName, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
		})

		fcName := uniqueContainerName(t, computerStub)
		fc := featurebase.NewContainer(fcName, map[string]string{
			"FEATUREBASE_COMPUTER_RUN":                "true",
			"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS": string(addressFn(mcName)),
		})

		tableName := dax.TableName("tbl")
		keyedTbl := test.TestQualifiedTableWithID(t, qual, "", tableName, 12, true)
		expTbl := test.TestQualifiedTableWithID(t, qual, "someid", tableName, 12, true)

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc).
			WithService(featurebase.ImageName, mc).
			WithService(featurebase.ImageName, fc).
			WithNetwork(uniqueNetworkName(t, mdsNetworkName))

		defer dc.Down()

		t.Run("start the containers with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForStatus(t, fc, "NORMAL", 10, time.Second)
		})

		t.Run("add a keyed table", func(t *testing.T) {
			inspect(t,
				mc,
				addressFn(mcName),
				"mds/create-table",
				keyedTbl,
				getExpFTable(expTbl),
			)
		})

		t.Run("check the translate-nodes endpoint", func(t *testing.T) {
			data := mdshttp.TranslateNodesRequest{
				Table:      keyedTbl.QualifiedID(),
				Partitions: dax.PartitionNums{3, 5, 8},
			}

			inspect(t,
				mc,
				addressFn(mcName),
				"mds/translate-nodes",
				data,
				getExpFTranslateResponse(mdshttp.TranslateNodesResponse{
					TranslateNodes: []controller.TranslateNode{
						{
							Address:    addressFn(fcName),
							Partitions: dax.PartitionNums{3, 5, 8},
						},
					}}),
			)
		})

		// The steps above result in featurebase receiving a directive which
		// creates the table.
		t.Run("check the schema endpoint", func(t *testing.T) {
			inspect(t,
				fc,
				addressFn(fcName),
				"computer/schema",
				nil,
				getExpFContains(`{"indexes":[{"name":"`, `"options":{"keys":true,"trackExistence":true,"partitionN":0,"description":""},"fields":[],"shardWidth":1048576}`),
			)
		})
	})

	t.Run("MDS_FeatureBase_Datagen_Queryer", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)
		imagePull(t, datagen.ImageName)

		tableName := dax.TableName("tbl")
		partitionN := 256
		tbl := dax.NewTable(tableName)
		tbl.PartitionN = partitionN
		tbl.Fields = []*dax.Field{
			{Name: "_id", Type: "id"},
			{Name: "an_int", Type: "int",
				Options: dax.FieldOptions{
					Min: pql.NewDecimal(0, 0),
					Max: pql.NewDecimal(500, 0),
				},
			},
			{Name: "a_random_string", Type: "string"},
			{Name: "an_id_set", Type: "idset"},
			{Name: "a_string_set", Type: "stringset"},
		}
		qtbl := dax.NewQualifiedTable(qual, tbl)

		mcName := uniqueContainerName(t, mdsStub)
		mc := featurebase.NewContainer(mcName, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
		})

		fc := featurebase.NewContainer(uniqueContainerName(t, computerStub), map[string]string{
			"FEATUREBASE_COMPUTER_RUN":                "true",
			"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS": string(addressFn(mcName)),
		})

		qcName := uniqueContainerName(t, queryerStub)
		qc := featurebase.NewContainer(qcName, map[string]string{
			"FEATUREBASE_QUERYER_RUN":                "true",
			"FEATUREBASE_QUERYER_CONFIG_MDS_ADDRESS": string(addressFn(mcName)),
		})

		networkName := uniqueNetworkName(t, mdsNetworkName)

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc, mc, qc).
			WithService(featurebase.ImageName, mc).
			WithService(featurebase.ImageName, fc).
			WithService(featurebase.ImageName, qc).
			WithNetwork(networkName)

		t.Run("start the containers with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, mc, dax.ServicePrefixMDS, 10, time.Second)
			waitForHealthy(t, qc, dax.ServicePrefixQueryer, 10, time.Second)
			waitForHealthy(t, fc, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc, "NORMAL", 10, time.Second)
		})
		defer dc.Down()

		t.Run("add a table", func(t *testing.T) {
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qtbl.Qualifier(),
				qtbl.CreateSQL(),
				`{"schema":{"fields":[]},"data":[],"error":"","warnings":null}`,
			)
		})

		t.Run("ingest some data", func(t *testing.T) {
			// datagen
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_ORG_ID", string(qual.OrganizationID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_DB_ID", string(qual.DatabaseID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_TABLE_NAME", string(qtbl.Name)))
			assert.NoError(t, docker.Setenv("GEN_CUSTOM_CONFIG", "/testdata/basic.yaml"))
			assert.NoError(t, docker.Setenv("GEN_USE_SHARD_TRANSACTIONAL_ENDPOINT", "true"))
			gcName := uniqueContainerName(t, datagenStub)
			gc := datagen.NewContainer(gcName, addressFn(mcName))
			_, err := docker.ContainerCreate(datagen.ImageName, gc, []docker.Volume{})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, gcName))
			assert.NoError(t, docker.ContainerStartWithLogging(gcName))
			defer func() {
				docker.ContainerStop(gcName)
				docker.ContainerRemove(gcName)
			}()

			// Wait for datagen to finish ingest.
			require.NoError(docker.ContainerWait(gc.Hostname()))
		})

		t.Run("query some data", func(t *testing.T) {
			// 	PQL: `Count(Row(a_string_set="A90B"))`,
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qual,
				`select count(*) as cnt from tbl where setcontains(a_string_set, 'A90B')`,
				`{"schema":{"fields":[{"name":"cnt","type":"int","base-type":"int"}]},"data":[[1]],"error":"","warnings":null}`,
			)
		})

		// time.Sleep(30000 * time.Second)
	})

	t.Run("MDS_Poller_FeatureBase", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		computerName1 := uniqueContainerName(t, computerStub+"1")
		computerName2 := uniqueContainerName(t, computerStub+"2")

		tableName := dax.TableName("tbl")
		partitionN := 12
		qtbl := test.TestQualifiedTableWithID(t, qual, "", tableName, partitionN, true)
		expTbl := test.TestQualifiedTableWithID(t, qual, "someID", tableName, partitionN, true)

		mcName := uniqueContainerName(t, mdsStub)
		mc := featurebase.NewContainer(mcName, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
		})

		env := map[string]string{
			"FEATUREBASE_COMPUTER_RUN":                "true",
			"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS": string(addressFn(mcName)),
		}
		fc1 := featurebase.NewContainer(computerName1, env)
		fc2 := featurebase.NewContainer(computerName2, env)

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc1, fc2, mc).
			WithService(featurebase.ImageName, mc).
			WithService(featurebase.ImageName, fc1).
			WithService(featurebase.ImageName, fc2).
			WithNetwork(uniqueNetworkName(t, mdsNetworkName))

		t.Run("start the containers with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, mc, dax.ServicePrefixMDS, 10, time.Second)
			waitForHealthy(t, fc1, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc1, "NORMAL", 10, time.Second)
			waitForHealthy(t, fc2, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc2, "NORMAL", 10, time.Second)
		})
		defer dc.Down()

		t.Run("add a keyed table", func(t *testing.T) {
			inspect(t,
				mc,
				addressFn(mcName),
				"mds/create-table",
				qtbl,
				getExpFTable(expTbl),
			)
		})

		partitions := dax.PartitionNums{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

		// ensure partitions are covered
		t.Run("check the translate-nodes endpoint", func(t *testing.T) {
			data := mdshttp.TranslateNodesRequest{
				Table:      qtbl.QualifiedID(),
				Partitions: partitions,
				IsWrite:    true,
			}

			inspect(t,
				mc,
				addressFn(mcName),
				"mds/translate-nodes",
				data,
				getExpFTranslateResponse(mdshttp.TranslateNodesResponse{
					TranslateNodes: []controller.TranslateNode{
						{
							Address:    addressFn(computerName1),
							Partitions: dax.PartitionNums{0, 2, 4, 6, 8, 10},
						},
						{
							Address:    addressFn(computerName2),
							Partitions: dax.PartitionNums{1, 3, 5, 7, 9, 11},
						},
					}}),
			)
		})

		// stop featurebase 1 (may need to sleep)
		t.Run("stop a container", func(t *testing.T) {
			docker.ContainerStop(computerName1)
		})

		// Give the poller time to recognize the node is gone.
		// TODO: implement this without a sleep.
		time.Sleep(20 * time.Second)

		// ensure paritions are still covered
		t.Run("check the translate-nodes endpoint again", func(t *testing.T) {
			data := mdshttp.TranslateNodesRequest{
				Table:      qtbl.QualifiedID(),
				Partitions: partitions,
			}

			inspect(t,
				mc,
				addressFn(mcName),
				"mds/translate-nodes",
				data,
				getExpFTranslateResponse(mdshttp.TranslateNodesResponse{
					TranslateNodes: []controller.TranslateNode{
						{
							Address:    addressFn(computerName2),
							Partitions: dax.PartitionNums{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
						},
					}}),
			)
		})

		//time.Sleep(3000 * time.Second)
	})

	t.Run("Node_Recovery", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		computerName1 := uniqueContainerName(t, computerStub+"1")
		computerName2 := uniqueContainerName(t, computerStub+"2")

		tableName := dax.TableName("tbl")
		partitionN := 12

		tbl := dax.NewTable(tableName)
		tbl.PartitionN = partitionN
		tbl.Fields = []*dax.Field{
			{Name: "_id", Type: "id"},
			{Name: "an_int", Type: "int",
				Options: dax.FieldOptions{
					Min: pql.NewDecimal(0, 0),
					Max: pql.NewDecimal(500, 0),
				},
			},
			{Name: "a_random_string", Type: "string"},
			{Name: "an_id_set", Type: "idset"},
			{Name: "a_string_set", Type: "stringset"},
		}
		qtbl := dax.NewQualifiedTable(qual, tbl)

		mcName := uniqueContainerName(t, mdsStub)
		mc := featurebase.NewContainer(mcName, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
		})

		wcName := uniqueContainerName(t, writeloggerStub)
		wc := featurebase.NewContainer(wcName, map[string]string{
			"FEATUREBASE_WRITELOGGER_RUN":             "true",
			"FEATUREBASE_WRITELOGGER_CONFIG_DATA_DIR": "/tmp",
		})

		fc1 := featurebase.NewContainer(computerName1, map[string]string{
			"FEATUREBASE_COMPUTER_RUN":                 "true",
			"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS":  string(addressFn(mcName)),
			"FEATUREBASE_COMPUTER_CONFIG_WRITE_LOGGER": string(addressFn(wcName)),
		})

		qcName := uniqueContainerName(t, queryerStub)
		qc := featurebase.NewContainer(qcName, map[string]string{
			"FEATUREBASE_QUERYER_RUN":                "true",
			"FEATUREBASE_QUERYER_CONFIG_MDS_ADDRESS": string(addressFn(mcName)),
		})

		networkName := uniqueNetworkName(t, mdsNetworkName)

		dc := new(docker.Composer).
			WithVolume(coverVolume, mc, wc, qc, fc1).
			WithService(featurebase.ImageName, mc).
			WithService(featurebase.ImageName, wc).
			WithService(featurebase.ImageName, qc).
			WithService(featurebase.ImageName, fc1).
			WithNetwork(networkName)

		t.Run("start the containers with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, mc, dax.ServicePrefixMDS, 10, time.Second)
			waitForHealthy(t, wc, dax.ServicePrefixWriteLogger, 10, time.Second)
			waitForHealthy(t, qc, dax.ServicePrefixQueryer, 10, time.Second)
			waitForHealthy(t, fc1, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc1, "NORMAL", 10, time.Second)
		})
		defer dc.Down()

		t.Run("add a table", func(t *testing.T) {
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qtbl.Qualifier(),
				qtbl.CreateSQL(),
				`{"schema":{"fields":[]},"data":[],"error":"","warnings":null}`,
			)
		})

		t.Run("ingest some data", func(t *testing.T) {
			// datagen
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_ORG_ID", string(qual.OrganizationID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_DB_ID", string(qual.DatabaseID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_TABLE_NAME", string(qtbl.Name)))
			assert.NoError(t, docker.Setenv("GEN_CUSTOM_CONFIG", "/testdata/basic.yaml"))
			assert.NoError(t, docker.Setenv("GEN_USE_SHARD_TRANSACTIONAL_ENDPOINT", "true"))
			gcName := uniqueContainerName(t, datagenStub)
			gc := datagen.NewContainer(gcName, addressFn(mcName))
			_, err := docker.ContainerCreate(datagen.ImageName, gc, []docker.Volume{})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, gcName))
			assert.NoError(t, docker.ContainerStartWithLogging(gcName))
			defer func() {
				docker.ContainerStop(gcName)
				docker.ContainerRemove(gcName)
			}()

			// Wait for datagen to finish ingest.
			require.NoError(docker.ContainerWait(gc.Hostname()))
		})

		t.Run("query some data", func(t *testing.T) {
			// 	PQL: `Count(Row(a_string_set="A90B"))`,
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qual,
				`select count(*) as cnt from tbl where setcontains(a_string_set, 'A90B')`,
				`{"schema":{"fields":[{"name":"cnt","type":"int","base-type":"int"}]},"data":[[1]],"error":"","warnings":null}`,
			)
		})

		// stop featurebase 1 (may need to sleep)
		t.Run("stop a container", func(t *testing.T) {
			docker.ContainerStop(computerName1)
		})

		// Give the poller time to recognize the node is gone.
		// TODO: implement this without a sleep.
		time.Sleep(25 * time.Second)

		t.Run("start a new compute node", func(t *testing.T) {
			fc2 := featurebase.NewContainer(computerName2, map[string]string{
				"FEATUREBASE_COMPUTER_RUN":                 "true",
				"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS":  string(addressFn(mcName)),
				"FEATUREBASE_COMPUTER_CONFIG_WRITE_LOGGER": string(addressFn(wcName)),
			})
			_, err := docker.ContainerCreate(featurebase.ImageName, fc2, []docker.Volume{coverVolume})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, computerName2))
			assert.NoError(t, docker.ContainerStart(computerName2))

			waitForHealthy(t, fc2, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc2, "NORMAL", 10, time.Second)
		})
		// This closes the container created in "start a new compute node", but
		// this should really only be called if that sub-test runs successfully.
		defer func() {
			docker.ContainerStop(computerName2)
			docker.ContainerRemove(computerName2)
		}()

		t.Run("query the same data", func(t *testing.T) {
			// 	PQL: `Count(Row(a_string_set="A90B"))`,
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qual,
				`select count(*) as cnt from tbl where setcontains(a_string_set, 'A90B')`,
				`{"schema":{"fields":[{"name":"cnt","type":"int","base-type":"int"}]},"data":[[1]],"error":"","warnings":null}`,
			)
		})

		//time.Sleep(3000 * time.Second)
	})

	t.Run("Node_Recovery_Snapshot", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		computerName1 := uniqueContainerName(t, computerStub+"1")
		computerName2 := uniqueContainerName(t, computerStub+"2")

		tableName := dax.TableName("tbl")
		partitionN := 12

		tbl := dax.NewTable(tableName)
		tbl.PartitionN = partitionN
		tbl.Fields = []*dax.Field{
			{Name: "_id", Type: "id"},
			{Name: "an_int", Type: "int",
				Options: dax.FieldOptions{
					Min: pql.NewDecimal(0, 0),
					Max: pql.NewDecimal(500, 0),
				},
			},
			{Name: "a_random_string", Type: "string"},
			{Name: "an_id_set", Type: "idset"},
			{Name: "a_string_set", Type: "stringset"},
		}
		qtbl := dax.NewQualifiedTable(qual, tbl)

		mcName := uniqueContainerName(t, mdsStub)
		mc := featurebase.NewContainer(mcName, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
		})

		wcName := uniqueContainerName(t, writeloggerStub)
		wc := featurebase.NewContainer(wcName, map[string]string{
			"FEATUREBASE_WRITELOGGER_RUN":             "true",
			"FEATUREBASE_WRITELOGGER_CONFIG_DATA_DIR": "/tmp",
		})

		scName := uniqueContainerName(t, snapshotterStub)
		sc := featurebase.NewContainer(scName, map[string]string{
			"FEATUREBASE_SNAPSHOTTER_RUN":             "true",
			"FEATUREBASE_SNAPSHOTTER_CONFIG_DATA_DIR": "/tmp",
		})

		fc1 := featurebase.NewContainer(computerName1, map[string]string{
			"FEATUREBASE_COMPUTER_RUN":                 "true",
			"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS":  string(addressFn(mcName)),
			"FEATUREBASE_COMPUTER_CONFIG_WRITE_LOGGER": string(addressFn(wcName)),
			"FEATUREBASE_COMPUTER_CONFIG_SNAPSHOTTER":  string(addressFn(scName)),
		})

		qcName := uniqueContainerName(t, queryerStub)
		qc := featurebase.NewContainer(qcName, map[string]string{
			"FEATUREBASE_QUERYER_RUN":                "true",
			"FEATUREBASE_QUERYER_CONFIG_MDS_ADDRESS": string(addressFn(mcName)),
		})

		networkName := uniqueNetworkName(t, mdsNetworkName)

		dc := new(docker.Composer).
			WithVolume(coverVolume, mc, wc, sc, qc, fc1).
			WithService(featurebase.ImageName, mc).
			WithService(featurebase.ImageName, wc).
			WithService(featurebase.ImageName, sc).
			WithService(featurebase.ImageName, qc).
			WithService(featurebase.ImageName, fc1).
			WithNetwork(networkName)

		t.Run("start the containers with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, mc, dax.ServicePrefixMDS, 10, time.Second)
			waitForHealthy(t, wc, dax.ServicePrefixWriteLogger, 10, time.Second)
			waitForHealthy(t, sc, dax.ServicePrefixSnapshotter, 10, time.Second)
			waitForHealthy(t, qc, dax.ServicePrefixQueryer, 10, time.Second)
			waitForHealthy(t, fc1, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc1, "NORMAL", 10, time.Second)
		})
		defer dc.Down()

		// TODO(tlt): using SQL to create the table doesn't work in tests
		// because we don't know the TableID that gets auto-generated. And
		// therefore we can't use that TableID later to, for example, trigger a
		// snapshot. For now we'll go directly to MDS to create the table (where
		// we can specify the TableID here in the test), but we probably need a
		// way to lookup a TableID given qual/table-name.
		//
		// t.Run("add a non-keyed table", func(t *testing.T) {
		// 	sqlCheck(t,
		// 		mc,
		// 		queryerAddress,
		// 		qtbl.Qualifier(),
		// 		qtbl.CreateSQL(),
		// 		`{"schema":{"fields":[]},"data":[],"error":"","warnings":null}`,
		// 	)
		// })
		t.Run("add a non-keyed table", func(t *testing.T) {
			inspect(t,
				mc,
				addressFn(mcName),
				"mds/create-table",
				qtbl,
				getExpFContains(
					`"name":"tbl","fields":[`,
					`{"name":"_id","type":"id","options":{"min":0,"max":0,"epoch":"0001-01-01T00:00:00Z"}}`,
					`{"name":"an_int","type":"int","options":{"min":0,"max":500,"epoch":"0001-01-01T00:00:00Z"}}`,
					`{"name":"a_random_string","type":"string","options":{"min":0,"max":0,"epoch":"0001-01-01T00:00:00Z"}}`,
					`{"name":"an_id_set","type":"idset","options":{"min":0,"max":0,"epoch":"0001-01-01T00:00:00Z"}}`,
					`{"name":"a_string_set","type":"stringset","options":{"min":0,"max":0,"epoch":"0001-01-01T00:00:00Z"}}`,
					`"partitionN":12,"org-id":"acme","db-id":"db1"}`,
				),
			)
		})

		t.Run("ingest some data", func(t *testing.T) {
			// datagen
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_ORG_ID", string(qual.OrganizationID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_DB_ID", string(qual.DatabaseID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_TABLE_NAME", string(qtbl.Name)))
			//assert.NoError(t, docker.Setenv("GEN_SEED", "1"))
			assert.NoError(t, docker.Setenv("GEN_CUSTOM_CONFIG", "/testdata/basic.yaml"))
			assert.NoError(t, docker.Setenv("GEN_USE_SHARD_TRANSACTIONAL_ENDPOINT", "true"))
			gcName := uniqueContainerName(t, datagenStub)
			gc := datagen.NewContainer(gcName, addressFn(mcName))
			_, err := docker.ContainerCreate(datagen.ImageName, gc, []docker.Volume{})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, gcName))
			assert.NoError(t, docker.ContainerStartWithLogging(gcName))
			defer func() {
				docker.ContainerStop(gcName)
				docker.ContainerRemove(gcName)
			}()

			// Wait for datagen to finish ingest.
			require.NoError(docker.ContainerWait(gc.Hostname()))
		})

		t.Run("query some data", func(t *testing.T) {
			// 	PQL: `Count(Row(a_string_set="B25A"))`,
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qual,
				`select count(*) as cnt from tbl where setcontains(a_string_set, 'B25A')`,
				`{"schema":{"fields":[{"name":"cnt","type":"int","base-type":"int"}]},"data":[[1]],"error":"","warnings":null}`,
			)
		})

		// snapshot shards and partitions
		t.Run("snapshot shards", func(t *testing.T) {
			data := mdshttp.SnapshotShardRequest{
				Table: qtbl.QualifiedID(),
				Shard: 0,
			}
			inspect(t,
				mc,
				addressFn(mcName),
				"mds/snapshot/shard-data",
				data,
				getExpFEqual(``),
			)
		})
		t.Run("snapshot partitions", func(t *testing.T) {
			data := mdshttp.SnapshotFieldKeysRequest{
				Table: qtbl.QualifiedID(),
				Field: "a_string_set",
			}
			inspect(t,
				mc,
				addressFn(mcName),
				"mds/snapshot/field-keys",
				data,
				getExpFEqual(``),
			)
		})

		// ingest some more data (seed 1)
		t.Run("ingest some more data", func(t *testing.T) {
			// datagen
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_ORG_ID", string(qual.OrganizationID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_DB_ID", string(qual.DatabaseID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_TABLE_NAME", string(qtbl.Name)))
			assert.NoError(t, docker.Setenv("GEN_SEED", "1"))
			assert.NoError(t, docker.Setenv("GEN_CUSTOM_CONFIG", "/testdata/basic.yaml"))
			assert.NoError(t, docker.Setenv("GEN_USE_SHARD_TRANSACTIONAL_ENDPOINT", "true"))
			gcName := uniqueContainerName(t, datagenStub)
			gc := datagen.NewContainer(gcName, addressFn(mcName))
			_, err := docker.ContainerCreate(datagen.ImageName, gc, []docker.Volume{})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, gcName))
			assert.NoError(t, docker.ContainerStartWithLogging(gcName))
			defer func() {
				docker.ContainerStop(gcName)
				docker.ContainerRemove(gcName)
			}()

			// Wait for datagen to finish ingest.
			require.NoError(docker.ContainerWait(gc.Hostname()))
		})

		t.Run("query the new data", func(t *testing.T) {
			// 	PQL: `Count(Row(a_string_set="B25A"))`,
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qual,
				`select count(*) as cnt from tbl where setcontains(a_string_set, 'B25A')`,
				`{"schema":{"fields":[{"name":"cnt","type":"int","base-type":"int"}]},"data":[[2]],"error":"","warnings":null}`,
			)
		})

		// stop featurebase 1 (may need to sleep)
		t.Run("stop a container", func(t *testing.T) {
			docker.ContainerStop(computerName1)
		})

		// Give the poller time to recognize the node is gone.
		// TODO: implement this without a sleep.
		time.Sleep(25 * time.Second)

		t.Run("start a new compute node", func(t *testing.T) {
			fc2 := featurebase.NewContainer(computerName2, map[string]string{
				"FEATUREBASE_COMPUTER_RUN":                 "true",
				"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS":  string(addressFn(mcName)),
				"FEATUREBASE_COMPUTER_CONFIG_WRITE_LOGGER": string(addressFn(wcName)),
				"FEATUREBASE_COMPUTER_CONFIG_SNAPSHOTTER":  string(addressFn(scName)),
			})
			_, err := docker.ContainerCreate(featurebase.ImageName, fc2, []docker.Volume{coverVolume})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, computerName2))
			assert.NoError(t, docker.ContainerStart(computerName2))

			waitForHealthy(t, fc2, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc2, "NORMAL", 10, time.Second)
		})
		// This closes the container created in "start a new compute node", but
		// this should really only be called if that sub-test runs successfully.
		defer func() {
			docker.ContainerStop(computerName2)
			docker.ContainerRemove(computerName2)
		}()

		t.Run("query the same data", func(t *testing.T) {
			// 	PQL: `Count(Row(a_string_set="B25A"))`,
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qual,
				`select count(*) as cnt from tbl where setcontains(a_string_set, 'B25A')`,
				`{"schema":{"fields":[{"name":"cnt","type":"int","base-type":"int"}]},"data":[[2]],"error":"","warnings":null}`,
			)
		})

		// TODO(tlt): it doesn't really make sense to have this tacked on to the
		// end of this test. There should be a separate test run which calls
		// this instead of the individual snapshot calls (in this test, above).
		// What we really need to do is create a test which can
		// create/snapshot/restore different table types (keyed/non-keyed), and
		// test different snapshot methods (individual shards, whole table,
		// etc.).
		t.Run("snapshot table", func(t *testing.T) {
			data := qtbl.QualifiedID()
			inspect(t,
				mc,
				addressFn(mcName),
				"mds/snapshot",
				data,
				getExpFEqual(``),
			)
		})

	})

	t.Run("MDS_Persistence", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)

		computerName1 := uniqueContainerName(t, computerStub+"1")
		computerName2 := uniqueContainerName(t, computerStub+"2")

		// Note: this test is able to bring up a new mds container with a
		// different name because it doesn't rely on any other container
		// maintaining a link to a reliable, constant "mds" address (i.e. this
		// test doesn't use queryer, or rely on a compute node registering with
		// mdsAddress2). We could probably share the same name for both
		// containers, and therefore not prevent other nodes from communicating
		// with the new mds container, but the purpose of this test is to
		// explicity show that the data is persistent across two, distinct mds
		// containers.
		mdsName1 := uniqueContainerName(t, mdsStub+"1")
		mdsName2 := uniqueContainerName(t, mdsStub+"2")

		mdsAddress1 := addressFn(mdsName1)
		mdsAddress2 := addressFn(mdsName2)

		tableName := dax.TableName("tbl")
		partitionN := 12
		qtbl := test.TestQualifiedTableWithID(t, qual, "", tableName, partitionN, true)

		mdsVolumeSource := "mds-storage"
		mdsVolumeTarget := "/storage"
		dsn := "file:" + mdsVolumeTarget + "/mds.boltdb"

		mc1 := featurebase.NewContainer(mdsName1, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
			"FEATUREBASE_STORAGE_METHOD":                        "boltdb",
			"FEATUREBASE_STORAGE_DSN":                           dsn,
		})
		var mc2 *featurebase.Container

		env := map[string]string{
			"FEATUREBASE_COMPUTER_RUN":                "true",
			"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS": string(mdsAddress1),
		}
		fc1 := featurebase.NewContainer(computerName1, env)

		mdsVolume := docker.Volume{
			Source: mdsVolumeSource,
			Target: mdsVolumeTarget,
		}

		networkName := uniqueNetworkName(t, mdsNetworkName)

		dc := new(docker.Composer).
			WithVolume(mdsVolume, mc1).
			WithVolume(coverVolume, mc1, fc1).
			WithService(featurebase.ImageName, mc1).
			WithService(featurebase.ImageName, fc1).
			WithNetwork(networkName)

		t.Run("start the containers with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, mc1, dax.ServicePrefixMDS, 10, time.Second)
			waitForHealthy(t, fc1, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc1, "NORMAL", 10, time.Second)
		})
		defer dc.Down()

		t.Run("add a keyed table", func(t *testing.T) {
			inspect(t,
				mc1,
				mdsAddress1,
				"mds/create-table",
				qtbl,
				getExpFContains(`"name":"tbl","fields":[{"name":"_id","type":"string","options":{"min":0,"max":0,"epoch":"0001-01-01T00:00:00Z"}}],"partitionN":12,"org-id":"acme","db-id":"db1"}`),
			)
		})

		partitions := dax.PartitionNums{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

		// ensure partitions are covered
		t.Run("check the translate-nodes endpoint", func(t *testing.T) {
			data := mdshttp.TranslateNodesRequest{
				Table:      qtbl.QualifiedID(),
				Partitions: partitions,
				IsWrite:    true,
			}

			inspect(t,
				mc1,
				mdsAddress1,
				"mds/translate-nodes",
				data,
				getExpFTranslateResponse(mdshttp.TranslateNodesResponse{
					TranslateNodes: []controller.TranslateNode{
						{
							Address:    addressFn(computerName1),
							Partitions: dax.PartitionNums{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
						},
					}}),
			)
		})

		// stop mds (may need to sleep)
		t.Run("stop mds container", func(t *testing.T) {
			docker.ContainerStop(mdsName1)
		})

		t.Run("start a new mds node", func(t *testing.T) {
			mc2 = featurebase.NewContainer(mdsName2, map[string]string{
				"FEATUREBASE_MDS_RUN":                               "true",
				"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
				"FEATUREBASE_STORAGE_METHOD":                        "boltdb",
				"FEATUREBASE_STORAGE_DSN":                           dsn,
			})
			_, err := docker.ContainerCreate(featurebase.ImageName, mc2, []docker.Volume{mdsVolume, coverVolume})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, mdsName2))
			assert.NoError(t, docker.ContainerStart(mdsName2))

			waitForHealthy(t, mc2, dax.ServicePrefixMDS, 10, time.Second)
		})
		// This closes the container created in "start a new compute node", but
		// this should really only be called if that sub-test runs successfully.
		defer func() {
			docker.ContainerStop(mdsName2)
			docker.ContainerRemove(mdsName2)
		}()

		// ensure partitions are covered by the new mds node.
		t.Run("check the translate-nodes endpoint again", func(t *testing.T) {
			data := mdshttp.TranslateNodesRequest{
				Table:      qtbl.QualifiedID(),
				Partitions: partitions,
				IsWrite:    true,
			}

			inspect(t,
				mc2,
				mdsAddress2,
				"mds/translate-nodes",
				data,
				getExpFTranslateResponse(mdshttp.TranslateNodesResponse{
					TranslateNodes: []controller.TranslateNode{
						{
							Address:    addressFn(computerName1),
							Partitions: dax.PartitionNums{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
						},
					}}),
			)
		})

		// ensure the poller is polling the compute node(s) after the MDS restart.

		// stop featurebase 1 (may need to sleep)
		t.Run("stop a container", func(t *testing.T) {
			docker.ContainerStop(computerName1)
		})

		// Give the poller time to recognize the node is gone.
		// TODO: implement this without a sleep.
		time.Sleep(20 * time.Second)

		t.Run("start a new compute node", func(t *testing.T) {
			fc2 := featurebase.NewContainer(computerName2, map[string]string{
				"FEATUREBASE_COMPUTER_RUN":                "true",
				"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS": string(mdsAddress2),
			})
			_, err := docker.ContainerCreate(featurebase.ImageName, fc2, []docker.Volume{coverVolume})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, computerName2))
			assert.NoError(t, docker.ContainerStart(computerName2))

			waitForHealthy(t, fc2, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc2, "NORMAL", 10, time.Second)
		})
		// This closes the container created in "start a new compute node", but
		// this should really only be called if that sub-test runs successfully.
		defer func() {
			docker.ContainerStop(computerName2)
			docker.ContainerRemove(computerName2)
		}()

		// ensure partitions are covered by the new compute node.
		t.Run("check the translate-nodes endpoint after compute node shutdown", func(t *testing.T) {
			data := mdshttp.TranslateNodesRequest{
				Table:      qtbl.QualifiedID(),
				Partitions: partitions,
				IsWrite:    true,
			}

			inspect(t,
				mc2,
				mdsAddress2,
				"mds/translate-nodes",
				data,
				getExpFTranslateResponse(mdshttp.TranslateNodesResponse{
					TranslateNodes: []controller.TranslateNode{
						{
							Address:    addressFn(computerName2),
							Partitions: dax.PartitionNums{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
						},
					}}),
			)
		})
	})

	// The idea for this test is to see if a node which has been paused (and
	// therefore removed from the MDS's node list by the poller) will be
	// re-registered via a CheckIn once it resumes following the pause. If this
	// happens, querying the data should result in 0 records because we have not
	// implemented a writelogger. This means the node re-joined, but it did not
	// recover data from the writelogger, and what data it did have locally was
	// removed due to a Directive.Method=reset, which is what we expect.
	t.Run("Node_CheckIn", func(t *testing.T) {
		imagePull(t, featurebase.ImageName)
		imagePull(t, datagen.ImageName)

		tableName := dax.TableName("tbl")
		partitionN := 256
		tbl := dax.NewTable(tableName)
		tbl.PartitionN = partitionN
		tbl.Fields = []*dax.Field{
			{Name: "_id", Type: "id"},
			{Name: "an_int", Type: "int",
				Options: dax.FieldOptions{
					Min: pql.NewDecimal(0, 0),
					Max: pql.NewDecimal(500, 0),
				},
			},
			{Name: "a_random_string", Type: "string"},
			{Name: "an_id_set", Type: "idset"},
			{Name: "a_string_set", Type: "stringset"},
		}
		qtbl := dax.NewQualifiedTable(qual, tbl)

		mcName := uniqueContainerName(t, mdsStub)
		mc := featurebase.NewContainer(mcName, map[string]string{
			"FEATUREBASE_MDS_RUN":                               "true",
			"FEATUREBASE_MDS_CONFIG_REGISTRATION_BATCH_TIMEOUT": "1ms",
		})

		fcName := uniqueContainerName(t, computerStub)
		fc := featurebase.NewContainer(fcName, map[string]string{
			"FEATUREBASE_COMPUTER_RUN":                      "true",
			"FEATUREBASE_COMPUTER_CONFIG_MDS_ADDRESS":       string(addressFn(mcName)),
			"FEATUREBASE_COMPUTER_CONFIG_CHECK_IN_INTERVAL": "2s",
		})

		qcName := uniqueContainerName(t, queryerStub)
		qc := featurebase.NewContainer(qcName, map[string]string{
			"FEATUREBASE_QUERYER_RUN":                "true",
			"FEATUREBASE_QUERYER_CONFIG_MDS_ADDRESS": string(addressFn(mcName)),
		})

		networkName := uniqueNetworkName(t, mdsNetworkName)

		dc := new(docker.Composer).
			WithVolume(coverVolume, fc, mc, qc).
			WithService(featurebase.ImageName, mc).
			WithService(featurebase.ImageName, fc).
			WithService(featurebase.ImageName, qc).
			WithNetwork(networkName)

		t.Run("start the containers with no errors", func(t *testing.T) {
			require.NoError(dc.Up())
			waitForHealthy(t, mc, dax.ServicePrefixMDS, 10, time.Second)
			waitForHealthy(t, qc, dax.ServicePrefixQueryer, 10, time.Second)
			waitForHealthy(t, fc, dax.ServicePrefixComputer, 10, time.Second)
			waitForStatus(t, fc, "NORMAL", 10, time.Second)
		})
		defer dc.Down()

		t.Run("add a table", func(t *testing.T) {
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qtbl.Qualifier(),
				qtbl.CreateSQL(),
				`{"schema":{"fields":[]},"data":[],"error":"","warnings":null}`,
			)
		})

		t.Run("ingest some data", func(t *testing.T) {
			// datagen
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_ORG_ID", string(qual.OrganizationID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_DB_ID", string(qual.DatabaseID)))
			assert.NoError(t, docker.Setenv("GEN_FEATUREBASE_TABLE_NAME", string(qtbl.Name)))
			assert.NoError(t, docker.Setenv("GEN_CUSTOM_CONFIG", "/testdata/basic.yaml"))
			assert.NoError(t, docker.Setenv("GEN_USE_SHARD_TRANSACTIONAL_ENDPOINT", "true"))
			gcName := uniqueContainerName(t, datagenStub)
			gc := datagen.NewContainer(gcName, addressFn(mcName))
			_, err := docker.ContainerCreate(datagen.ImageName, gc, []docker.Volume{})
			assert.NoError(t, err)
			assert.NoError(t, docker.NetworkConnect(networkName, gcName))
			assert.NoError(t, docker.ContainerStartWithLogging(gcName))
			defer func() {
				docker.ContainerStop(gcName)
				docker.ContainerRemove(gcName)
			}()

			// Wait for datagen to finish ingest.
			require.NoError(docker.ContainerWait(gc.Hostname()))
		})

		t.Run("query some data", func(t *testing.T) {
			// 	PQL: `Count(Row(a_string_set="A90B"))`,
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qual,
				`select count(*) as cnt from tbl`,
				`{"schema":{"fields":[{"name":"cnt","type":"int","base-type":"int"}]},"data":[[100]],"error":"","warnings":null}`,
			)
		})

		// stop featurebase 1 (may need to sleep)
		t.Run("pause a container", func(t *testing.T) {
			docker.ContainerPauseAndResume(fcName, 25*time.Second)
		})

		// Wait the 25 seconds for the pause/resume, plus another 25s.
		// TODO: implement this without a sleep.
		time.Sleep(50 * time.Second)

		t.Run("query some data again", func(t *testing.T) {
			// 	PQL: `Count(Row(a_string_set="A90B"))`,
			sqlCheck(t,
				mc,
				addressFn(qcName),
				qual,
				`select count(*) as cnt from tbl`,
				`{"schema":{"fields":[{"name":"cnt","type":"int","base-type":"int"}]},"data":[[0]],"error":"","warnings":null}`,
			)
		})
	})
}

type expFunc func(t *testing.T, actual string)

// checks if the response contains all passed strings.
func getExpFContains(exps ...string) expFunc {
	return func(t *testing.T, actual string) {
		for _, exp := range exps {
			assert.Contains(t, actual, exp)
		}
	}
}

// compares if strings are qual
func getExpFEqual(exp string) expFunc {
	return func(t *testing.T, actual string) {
		assert.Equal(t, exp, actual)
	}
}

// compares tables, but only checks for presence of absence of ID, not
// actual value.
func getExpFTable(tabl *dax.QualifiedTable) expFunc {
	return func(t *testing.T, actual string) {
		actTabl := dax.Table{}
		err := json.Unmarshal([]byte(actual), &actTabl)
		assert.NoError(t, err)
		assert.Equal(t, tabl.Name, actTabl.Name)
		assert.Equal(t, tabl.Fields, actTabl.Fields)
		assert.Equal(t, tabl.PartitionN, actTabl.PartitionN)
		if len(tabl.ID) == 0 && len(actTabl.ID) != 0 {
			t.Errorf("expected no ID, but got '%s'", actTabl.ID)
		}
		if len(tabl.ID) != 0 && len(actTabl.ID) == 0 {
			t.Error("expected ID, but got none")
		}

		for i, fld := range actTabl.Fields {
			assert.Equal(t, tabl.Fields[i], fld)
		}
	}
}

// compares translate nodes response, but doesn't compare node names.
func getExpFTranslateResponse(exp mdshttp.TranslateNodesResponse) expFunc {
	return func(t *testing.T, actual string) {
		actResp := mdshttp.TranslateNodesResponse{}
		err := json.Unmarshal([]byte(actual), &actResp)
		assert.NoError(t, err)
		assert.Equal(t, len(exp.TranslateNodes), len(actResp.TranslateNodes))
		for i, actNode := range actResp.TranslateNodes {
			expNode := exp.TranslateNodes[i]
			assert.Equal(t, expNode.Address, actNode.Address)
			assert.Equal(t, expNode.Partitions, actNode.Partitions)
		}
	}
}

// inspect is a test helper for issuing common curl commands to a container and
// verifying the results are expected.
//
// c         - the container on which the curl command should be run
// address   - the target of the curl command
// data      - if non-nil, will be marshaled to json as the POST arguments
//   - if nil, the curl command will run as GET
//
// expF       - A function called on the result which should return true if the result is as expected.
func inspect(t *testing.T, c docker.Container, address dax.Address, path string, data interface{}, expF expFunc) {
	t.Helper()

	insp, err := inspector.NewInspector()
	assert.NoError(t, err)
	defer insp.Close()

	ctx := context.Background()

	req := inspector.NewExecRequest(address, path, data)

	cmd := req.Cmd()
	log.Printf("cmd: %s", cmd)

	//resp, err := insp.ExecResp(ctx, c, req.Cmd())
	resp, err := insp.ExecResp(ctx, c, cmd)
	assert.NoError(t, err)
	expF(t, resp.Out())
	//assert.JSONEq(t, exp, resp.Out())
}

// sqlCheck is a test helper for issuing a sql command to a queryer and
// verifying the results are expected.
//
// c         - the container on which the curl command should be run
// address   - the queryer address
// qual      - table qualifier
// sql       - sql string
//
// exp       - the expected result marshaled to a string
func sqlCheck(t *testing.T, c docker.Container, address dax.Address, qual dax.TableQualifier, sql string, exp string) {
	t.Helper()

	out := sqlRun(t, c, address, qual, sql)

	got := &fb.WireQueryResponse{}
	assert.NoError(t, json.Unmarshal([]byte(out), got))

	want := &fb.WireQueryResponse{}
	assert.NoError(t, json.Unmarshal([]byte(exp), want))

	assert.Equal(t, want.Schema, got.Schema)
	assert.Equal(t, want.Data, got.Data)
	assert.Equal(t, want.Error, got.Error)
	assert.Equal(t, want.Warnings, got.Warnings)
}

// sqlRun is a test helper for issuing sql to a queryer's sql endpoint.
//
// c         - the container on which the curl command should be run
// address   - the queryer address
// qual      - table qualifier
// sql       - sql string
func sqlRun(tb testing.TB, c docker.Container, address dax.Address, qual dax.TableQualifier, sql string) string {
	tb.Helper()

	path := "queryer/sql"

	insp, err := inspector.NewInspector()
	assert.NoError(tb, err)
	defer insp.Close()

	ctx := context.Background()

	sqlReq := queryerhttp.SQLRequest{
		OrganizationID: qual.OrganizationID,
		DatabaseID:     qual.DatabaseID,
		SQL:            sql,
	}

	req := inspector.NewExecRequest(address, path, sqlReq)

	cmd := req.Cmd()
	log.Printf("cmd: %s", cmd)

	//resp, err := insp.ExecResp(ctx, c, req.Cmd())
	resp, err := insp.ExecResp(ctx, c, cmd)
	assert.NoError(tb, err)

	out := resp.Out()
	log.Printf("RESPONSE: cmd: %s; out: %s", cmd, out)

	return out
}

// pqlRun is a test helper for issuing pql to a queryer's sql endpoint.
//
// c         - the container on which the curl command should be run
// address   - the queryer address
// qual      - table qualifier
// pql       - pql string
func pqlRun(tb testing.TB, c docker.Container, address dax.Address, qual dax.TableQualifier, table, pql string) string {
	tb.Helper()

	path := "queryer/query"

	insp, err := inspector.NewInspector()
	assert.NoError(tb, err)
	defer insp.Close()

	ctx := context.Background()

	pqlReq := queryerhttp.QueryRequest{
		OrganizationID: qual.OrganizationID,
		DatabaseID:     qual.DatabaseID,
		Table:          dax.TableName(table),
		PQL:            pql,
	}

	req := inspector.NewExecRequest(address, path, pqlReq)

	cmd := req.Cmd()
	log.Printf("cmd: %s", cmd)

	resp, err := insp.ExecResp(ctx, c, cmd)
	assert.NoError(tb, err)

	out := resp.Out()
	log.Printf("RESPONSE: cmd: %s; out: %s", cmd, out)

	return out
}

// statusResponse mirrors the basic elements of getStatusResponse, a private
// type in the featurebase package. In the future, we should have shared, public
// API return types for featurebase.
type statusResponse struct {
	State       string `json:"state"`
	LocalID     string `json:"localID"`
	ClusterName string `json:"clusterName"`
}

// waitForStatus is currently specific to featurebase. In the future, we could
// generalize this by taking a docker.Container instead, and having every
// container support something like host:80/status (where we standardize on the
// port, /status, and the return payload)
func waitForStatus(t *testing.T, fc *featurebase.Container, status string, n int, sleep time.Duration) {
	t.Helper()

	insp, err := inspector.NewInspector()
	assert.NoError(t, err)
	defer insp.Close()

	ctx := context.Background()
	address := dax.Address(fc.Hostname() + ":8080")
	req := inspector.NewExecRequest(address, fmt.Sprintf("%s/status", dax.ServicePrefixComputer), nil)

	for i := 0; i < n; i++ {
		resp, err := insp.ExecResp(ctx, fc, req.Cmd())
		assert.NoError(t, err)

		var statusResp statusResponse

		if err := json.Unmarshal([]byte(resp.Out()), &statusResp); err != nil {
			// treat json unmarshal error the same as not getting the expected
			// status.
		} else {
			s := statusResp.State
			t.Logf("Status (%d/%d): %s (sleep: %s)\n", i, n, s, sleep.String())

			if s == status {
				return
			}
		}

		if i < n-1 {
			time.Sleep(sleep)
		}
	}

	// Getting to here means status was never found, so we need to stop the
	// test.
	panic("waitForStatus timed out")
}

// waitForHealthy currently requires the service to be listening on port 8080.
// It polls the /health endpoint expecting `HTTP/1.1 200 OK`.
func waitForHealthy(t *testing.T, container docker.Container, path string, n int, sleep time.Duration) {
	t.Helper()

	insp, err := inspector.NewInspector()
	assert.NoError(t, err)
	defer insp.Close()

	ctx := context.Background()
	address := container.Hostname() + ":8080"
	fullPath := "health"
	if path != "" {
		fullPath = path + "/" + fullPath
	}
	uri := fmt.Sprintf("%s/%s", address, fullPath)
	cmd := []string{"curl", "-I", "-XGET", uri}
	healthOk := "HTTP/1.1 200 OK"

	for i := 0; i < n; i++ {
		resp, err := insp.ExecResp(ctx, container, cmd)
		assert.NoError(t, err)

		out := resp.Out()
		t.Logf("Health (%d/%d): uri: %s '%s' (sleep: %s)\n", i, n, uri, out, sleep.String())

		if strings.HasPrefix(out, healthOk) {
			return
		}

		if i < n-1 {
			time.Sleep(sleep)
		}
	}

	// Getting to here means the health endpoint never returned successfully, so
	// we need to stop the test.
	panic("waitForHealthy timed out")
}

func uniqueNetworkName(t *testing.T, stub string) string {
	rn := make([]byte, 8)
	if _, err := rand.Read(rn); err != nil {
		t.Fatalf("getting random data: %v", err)
	}
	return fmt.Sprintf("%s_%x", stub, rn)
}

func uniqueContainerName(t *testing.T, stub string) string {
	rn := make([]byte, 8)
	if _, err := rand.Read(rn); err != nil {
		t.Fatalf("getting random data: %v", err)
	}
	return fmt.Sprintf("%s%x", stub, rn)
}

// sortStringKeys goes through an entire set of rows, and for any []string it
// finds, it orders the elements. This is obviously only useful in tests, and
// only in cases where we expect the elements to match, but we don't care what
// order they're in. It's basically the equivalent of assert.ElementsMatch(),
// but the way we use that on rows doesn't recurse down into the field values
// within each row.
// TODO(tlt): put this in sql test?
func sortStringKeys(in [][]interface{}) {
	for i := range in {
		for j := range in[i] {
			switch v := in[i][j].(type) {
			case []string:
				sort.Strings(v)
			}
		}
	}
}

// mustQueryRows returns the row results as a slice of []interface{}, along with the columns.
func mustQueryRows(tb testing.TB, c docker.Container, address dax.Address, qual dax.TableQualifier, query string, table string) ([][]interface{}, []*fb.WireQueryField, error) {
	tb.Helper()
	var sql string
	var out string
	if table == "" {
		sql = query
		out = sqlRun(tb, c, address, qual, sql)
	} else {
		out = pqlRun(tb, c, address, qual, table, query)
	}

	sqlResp := &fb.WireQueryResponse{}

	if err := json.Unmarshal([]byte(out), sqlResp); err != nil {
		tb.Fatalf("error unmarshaling response: %v, raw resp: '%s'", err, out)
	}

	var errOut error
	if sqlResp.Error != "" {
		errOut = errors.New(sqlResp.Error)
	}

	return sqlResp.Data, sqlResp.Schema.Fields, errOut
}
