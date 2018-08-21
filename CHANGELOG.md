# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [v1.1.0] - 2018-08-21

This version contains 32 contributions from 5 contributors. There are 89 files changed; 2,752 insertions; and 1,013 deletions.

### Added

- Add CircleCI ([#1610](https://github.com/pilosa/pilosa/pull/1610))
- Add key translation to exports ([#1608](https://github.com/pilosa/pilosa/pull/1608))
- Support importing key values ([#1599](https://github.com/pilosa/pilosa/pull/1599), [#1601](https://github.com/pilosa/pilosa/pull/1601))
- Treat coordinator as primary translate store ([#1582](https://github.com/pilosa/pilosa/pull/1582))
- Add DEGRADED cluster state and handle gossip NodeLeave events correctly ([#1584](https://github.com/pilosa/pilosa/pull/1584))
- Add linters to gometalinter and fix related issues ([#1544](https://github.com/pilosa/pilosa/pull/1544), [#1543](https://github.com/pilosa/pilosa/pull/1543), [#1540](https://github.com/pilosa/pilosa/pull/1540), [#1539](https://github.com/pilosa/pilosa/pull/1539), [#1537](https://github.com/pilosa/pilosa/pull/1537), [#1536](https://github.com/pilosa/pilosa/pull/1536), [#1535](https://github.com/pilosa/pilosa/pull/1535), [#1534](https://github.com/pilosa/pilosa/pull/1534), [#1530](https://github.com/pilosa/pilosa/pull/1530), [#1529](https://github.com/pilosa/pilosa/pull/1529), [#1528](https://github.com/pilosa/pilosa/pull/1528), [#1526](https://github.com/pilosa/pilosa/pull/1526), [#1527](https://github.com/pilosa/pilosa/pull/1527))
- Add mutex field type ([#1524](https://github.com/pilosa/pilosa/pull/1524))
- Fragment rows() and rowsForColumn() ([#1532](https://github.com/pilosa/pilosa/pull/1532))

### Fixed

- Fix race on replicationClosing channel ([#1607](https://github.com/pilosa/pilosa/pull/1607))
- Prevent anti-entropy and cluster resize from running simultaneously ([#1586](https://github.com/pilosa/pilosa/pull/1586))
- Require a valid port that isn't greater than 65,535 ([#1603](https://github.com/pilosa/pilosa/pull/1603))
- Add view parameter to sync logic for syncing time fields ([#1602](https://github.com/pilosa/pilosa/pull/1602))
- Fix translator in cluster environment ([#1552](https://github.com/pilosa/pilosa/pull/1552))
- Use string prefix instead of equality so json error message will pass on all Go versions ([#1558](https://github.com/pilosa/pilosa/pull/1558))

## [v1.0.2] - 2018-08-01

This version contains 11 contributions from 3 contributors. There are 30 files changed; 1,569 insertions; and 1,215 deletions.

### Fixed

- Fix documentation ([#1503](https://github.com/pilosa/pilosa/pull/1503), [#1495](https://github.com/pilosa/pilosa/pull/1495), [#1551](https://github.com/pilosa/pilosa/pull/1551))
- Fix places where empty IndexOptions were being used ([#1547](https://github.com/pilosa/pilosa/pull/1547))
- Fix translator syncing bug in cluster environments ([#1552](https://github.com/pilosa/pilosa/pull/1552))
- Fix race condition in translate_test ([#1541](https://github.com/pilosa/pilosa/pull/1541))
- Add IndexOptions to IndexInfo json response ([#1542](https://github.com/pilosa/pilosa/pull/1542))
- Add proper locking to cluster code to prevent races ([#1533](https://github.com/pilosa/pilosa/pull/1533))
- Re-export erroneously unexported func Row.Intersect ([#1502](https://github.com/pilosa/pilosa/pull/1502))
- Update parser to handle row keys on SetRowAttrs() ([#1555](https://github.com/pilosa/pilosa/pull/1555))

## [v1.0.1] - 2018-07-11

This version contains 12 contributions from 4 contributors. There are 11 files changed; 133 insertions; and 39 deletions.

### Fixed

- Use `dep ensure -vendor-only` for build repeatability ([#1491](https://github.com/pilosa/pilosa/pull/1491))
- Make sure time range views are calculated correctly across months ([#1485](https://github.com/pilosa/pilosa/pull/1485))
- Fix up error handling, add a configurable timeout to http handler closing ([#1486](https://github.com/pilosa/pilosa/pull/1486))
- Add gossip Closer ([#1483](https://github.com/pilosa/pilosa/pull/1483))
- Update docs references to WebUI naming (console) and installation ([#1493](https://github.com/pilosa/pilosa/pull/1493))

## [v1.0.0] - 2018-07-09

This version contains 218 contributions from 7 contributors. There are 184 files changed; 21,769 insertions; and 20,275 deletions.

### Added

- ID-Key Translation ([#1337](https://github.com/pilosa/pilosa/pull/1337))
- Add CORS support to handler ([#1327](https://github.com/pilosa/pilosa/pull/1327))

### Changed

- HTTP handler updates ([#1408](https://github.com/pilosa/pilosa/pull/1408), [#1399](https://github.com/pilosa/pilosa/pull/1399), [#1441](https://github.com/pilosa/pilosa/pull/1441), [#1375](https://github.com/pilosa/pilosa/pull/1375), [#1433](https://github.com/pilosa/pilosa/pull/1433), [#1444](https://github.com/pilosa/pilosa/pull/1444), [#1388](https://github.com/pilosa/pilosa/pull/1388), [#1309](https://github.com/pilosa/pilosa/pull/1309), [#1302](https://github.com/pilosa/pilosa/pull/1302), [#1304](https://github.com/pilosa/pilosa/pull/1304), [#1465](https://github.com/pilosa/pilosa/pull/1465), [#1466](https://github.com/pilosa/pilosa/pull/1466))
- Refactor/improve tests ([#1437](https://github.com/pilosa/pilosa/pull/1437), [#1434](https://github.com/pilosa/pilosa/pull/1434), [#1435](https://github.com/pilosa/pilosa/pull/1435), [#1425](https://github.com/pilosa/pilosa/pull/1425), [#1418](https://github.com/pilosa/pilosa/pull/1418), [#1419](https://github.com/pilosa/pilosa/pull/1419), [#1413](https://github.com/pilosa/pilosa/pull/1413), [#1394](https://github.com/pilosa/pilosa/pull/1394), [#1387](https://github.com/pilosa/pilosa/pull/1387), [#1386](https://github.com/pilosa/pilosa/pull/1386), [#1378](https://github.com/pilosa/pilosa/pull/1378), [#1364](https://github.com/pilosa/pilosa/pull/1364), [#1348](https://github.com/pilosa/pilosa/pull/1348), [#1340](https://github.com/pilosa/pilosa/pull/1340), [#1297](https://github.com/pilosa/pilosa/pull/1297))
- Simplify inter-node communication ([#1428](https://github.com/pilosa/pilosa/pull/1428), [#1427](https://github.com/pilosa/pilosa/pull/1427), [#1412](https://github.com/pilosa/pilosa/pull/1412), [#1398](https://github.com/pilosa/pilosa/pull/1398), [#1391](https://github.com/pilosa/pilosa/pull/1391), [#1389](https://github.com/pilosa/pilosa/pull/1389))
- Make gossip's interface to Pilosa the API struct ([#1452](https://github.com/pilosa/pilosa/pull/1452))
- Rename slice to shard ([#1426](https://github.com/pilosa/pilosa/pull/1426))
- Clearbit for time fields ([#1424](https://github.com/pilosa/pilosa/pull/1424))
- Update docs ([#1390](https://github.com/pilosa/pilosa/pull/1390), [#1329](https://github.com/pilosa/pilosa/pull/1329), [#1305](https://github.com/pilosa/pilosa/pull/1305), [#1296](https://github.com/pilosa/pilosa/pull/1296), [#1461](https://github.com/pilosa/pilosa/pull/1461))
- Simplify server setup ([#1417](https://github.com/pilosa/pilosa/pull/1417), [#1393](https://github.com/pilosa/pilosa/pull/1393),[#1451](https://github.com/pilosa/pilosa/pull/1451))
- Refactor API ([#1407](https://github.com/pilosa/pilosa/pull/1407))
- Rewrite PQL parser and add various improvements/simplifications ([#1382](https://github.com/pilosa/pilosa/pull/1382), [#1402](https://github.com/pilosa/pilosa/pull/1402), [#1354](https://github.com/pilosa/pilosa/pull/1354), [#1463](https://github.com/pilosa/pilosa/pull/1463))
- Rename "frame" to "field" ([#1395](https://github.com/pilosa/pilosa/pull/1395), [#1362](https://github.com/pilosa/pilosa/pull/1362), [#1360](https://github.com/pilosa/pilosa/pull/1360), [#1358](https://github.com/pilosa/pilosa/pull/1358), [#1357](https://github.com/pilosa/pilosa/pull/1357), [#1355](https://github.com/pilosa/pilosa/pull/1355))
- Optimize count ([#1365](https://github.com/pilosa/pilosa/pull/1365))
- Simplify bitmap max function ([#1333](https://github.com/pilosa/pilosa/pull/1333))
- Rename "bit" to "column" for clarity ([#1326](https://github.com/pilosa/pilosa/pull/1326))
- Rename pilosa.Bitmap to Row ([#1311](https://github.com/pilosa/pilosa/pull/1311))
- Invert encoding/decoding and remove internal references ([#1454](https://github.com/pilosa/pilosa/pull/1454))

### Removed

- Rename (unexport) many items to reduce public API footprint prior to 1.0 release ([#1470](https://github.com/pilosa/pilosa/pull/1470), [#1458](https://github.com/pilosa/pilosa/pull/1458), [#1450](https://github.com/pilosa/pilosa/pull/1450), [#1449](https://github.com/pilosa/pilosa/pull/1449), [#1448](https://github.com/pilosa/pilosa/pull/1448), [#1447](https://github.com/pilosa/pilosa/pull/1447), [#1446](https://github.com/pilosa/pilosa/pull/1446), [#1438](https://github.com/pilosa/pilosa/pull/1438), [#1443](https://github.com/pilosa/pilosa/pull/1443), [#1440](https://github.com/pilosa/pilosa/pull/1440), [#1439](https://github.com/pilosa/pilosa/pull/1439), [#1409](https://github.com/pilosa/pilosa/pull/1409), [#1392](https://github.com/pilosa/pilosa/pull/1392), [#1374](https://github.com/pilosa/pilosa/pull/1374), [#1372](https://github.com/pilosa/pilosa/pull/1372), [#1369](https://github.com/pilosa/pilosa/pull/1369), [#1367](https://github.com/pilosa/pilosa/pull/1367), [#1366](https://github.com/pilosa/pilosa/pull/1366), [#1351](https://github.com/pilosa/pilosa/pull/1351), [#1420](https://github.com/pilosa/pilosa/pull/1420), [#1416](https://github.com/pilosa/pilosa/pull/1416), [#1397](https://github.com/pilosa/pilosa/pull/1397))
- Remove dead code ([#1432](https://github.com/pilosa/pilosa/pull/1432), [#1457](https://github.com/pilosa/pilosa/pull/1457), [#1421](https://github.com/pilosa/pilosa/pull/1421), [#1411](https://github.com/pilosa/pilosa/pull/1411), [#1377](https://github.com/pilosa/pilosa/pull/1377), [#1393](https://github.com/pilosa/pilosa/pull/1393), [#1462](https://github.com/pilosa/pilosa/pull/1462))
- Remove view argument from Field.SetBit and Field.ClearBit ([#1396](https://github.com/pilosa/pilosa/pull/1396))
- Remove WebUI (now contained in a separate package) ([#1363](https://github.com/pilosa/pilosa/pull/1363))
- Remove bench command ([#1347](https://github.com/pilosa/pilosa/pull/1347))
- Remove "view" from API, handler, docs ([#1346](https://github.com/pilosa/pilosa/pull/1346))
- Remove backup/restore stuff ([#1339](https://github.com/pilosa/pilosa/pull/1339), [#1341](https://github.com/pilosa/pilosa/pull/1341))
- Remove inverse frame functionality ([#1335](https://github.com/pilosa/pilosa/pull/1335))
- Remove rangeEnabled option ([#1332](https://github.com/pilosa/pilosa/pull/1332))
- Remove index and field MarshalJSON ([#1468](https://github.com/pilosa/pilosa/pull/1468))

### Fixed

- Fix a few data races ([#1423](https://github.com/pilosa/pilosa/pull/1423))
- Fix for crash while removing containers ([#1401](https://github.com/pilosa/pilosa/pull/1401))
- Allow dashes in frame names ([#1415](https://github.com/pilosa/pilosa/pull/1415))
- Fix generate-config command, use single toml lib ([#1350](https://github.com/pilosa/pilosa/pull/1350))

## [v0.10.0] - 2018-05-15

This version contains 93 contributions from 8 contributors. There are 93 files changed; 4,495 insertions; and 5,392 deletions.

### Added

- Add B+Tree containers (Enterprise Edition) ([#1285](https://github.com/pilosa/pilosa/pull/1285))
- Add /info endpoint ([#1236](https://github.com/pilosa/pilosa/pull/1236))

### Changed

- Wrap errors ([#1271](https://github.com/pilosa/pilosa/pull/1271), [#1258](https://github.com/pilosa/pilosa/pull/1258), [#1274](https://github.com/pilosa/pilosa/pull/1274), [#1270](https://github.com/pilosa/pilosa/pull/1270), [#1273](https://github.com/pilosa/pilosa/pull/1273), [#1272](https://github.com/pilosa/pilosa/pull/1272), [#1260](https://github.com/pilosa/pilosa/pull/1260), [#1259](https://github.com/pilosa/pilosa/pull/1259), [#1256](https://github.com/pilosa/pilosa/pull/1256), [#1257](https://github.com/pilosa/pilosa/pull/1257), [#1261](https://github.com/pilosa/pilosa/pull/1261), [#1262](https://github.com/pilosa/pilosa/pull/1262), [#1263](https://github.com/pilosa/pilosa/pull/1263), [#1265](https://github.com/pilosa/pilosa/pull/1265))

### Removed

- Remove unused code ([#1286](https://github.com/pilosa/pilosa/pull/1286))
- Remove input definition, add install-stringer to Makefile ([#1284](https://github.com/pilosa/pilosa/pull/1284))
- Remove /id and /hosts endpoints. Add local ID to /status ([#1238](https://github.com/pilosa/pilosa/pull/1238))
- Remove API.URI ([#1255](https://github.com/pilosa/pilosa/pull/1255))

### Fixed

- Assorted docs fixes ([#1281](https://github.com/pilosa/pilosa/pull/1281), [#1269](https://github.com/pilosa/pilosa/pull/1269))
- Update PQL syntax in bench subcommand ([#1279](https://github.com/pilosa/pilosa/pull/1279))
- Update help menu in WebUI ([#1278](https://github.com/pilosa/pilosa/pull/1278))
- Fix dead lock ([#1268](https://github.com/pilosa/pilosa/pull/1268))
- Make sure gossipMemberSet.Logger is set during server setup ([#1266](https://github.com/pilosa/pilosa/pull/1266))
- Make sure ~ is expanded in NewServer; BroadcastReceiver uses temp path ([#1242](https://github.com/pilosa/pilosa/pull/1242))
- Avoid creating a slice of nil timestamps on Import() ([#1234](https://github.com/pilosa/pilosa/pull/1234))
- Fixup internal client ([#1253](https://github.com/pilosa/pilosa/pull/1253))

## [v0.9.0] - 2018-05-04

This version contains 188 contributions from 12 contributors. There are 141 files changed; 17,832 insertions; and 7,503 deletions.

*Please see special [upgrading instructions](https://www.pilosa.com/docs/latest/administration/#version-0-9) for this release.*

### Added

- Add ability to dynamically resize clusters ([#982](https://github.com/pilosa/pilosa/pull/982), [#946](https://github.com/pilosa/pilosa/pull/946), [#929](https://github.com/pilosa/pilosa/pull/929), [#927](https://github.com/pilosa/pilosa/pull/927), [#917](https://github.com/pilosa/pilosa/pull/917), [#913](https://github.com/pilosa/pilosa/pull/913), [#912](https://github.com/pilosa/pilosa/pull/912), [#908](https://github.com/pilosa/pilosa/pull/908))
- Update docs to include cluster-resize config and instructions ([#1088](https://github.com/pilosa/pilosa/pull/1088))
- Add support for lists of gossip seeds for redundancy ([#1133](https://github.com/pilosa/pilosa/pull/1133))
- Add HTTP Handler validation ([#1140](https://github.com/pilosa/pilosa/pull/1140), [#1121](https://github.com/pilosa/pilosa/pull/1121))
- Add validation around node-remove conditions ([#1138](https://github.com/pilosa/pilosa/pull/1138))
- broadcast.SendSync field creation and deletion to all nodes ([#1132](https://github.com/pilosa/pilosa/pull/1132))
- Spread recalculate caches to all nodes. Fixes #1069 ([#1109](https://github.com/pilosa/pilosa/pull/1109))
- Add QueryResult.Type to protobuf message to distiguish results at the client ([#1064](https://github.com/pilosa/pilosa/pull/1064))
- Modify `pilosa import` to support string rows/columns ([#1063](https://github.com/pilosa/pilosa/pull/1063))
- Add some statsd calls to HolderSyncer ([#1048](https://github.com/pilosa/pilosa/pull/1048))
- Add support for memberlist gossip configuration via pilosa.Config ([#1014](https://github.com/pilosa/pilosa/pull/1014))
- Add local and cluster IDs ([#1013](https://github.com/pilosa/pilosa/pull/1013), [#1245](https://github.com/pilosa/pilosa/pull/1245))
- Add HolderCleaner and view.DeleteFragment ([#985](https://github.com/pilosa/pilosa/pull/985))
- Add set-coordinator endpoint ([#963](https://github.com/pilosa/pilosa/pull/963))
- Implement Min/Max BSI queries ([#1191](https://github.com/pilosa/pilosa/pull/1191))
- Log time/version to startup log ([#1246](https://github.com/pilosa/pilosa/pull/1246))
- Documentation improvements ([#1135](https://github.com/pilosa/pilosa/pull/1135), [#1154](https://github.com/pilosa/pilosa/pull/1154), [#1091](https://github.com/pilosa/pilosa/pull/1091), [#1108](https://github.com/pilosa/pilosa/pull/1108), [#1087](https://github.com/pilosa/pilosa/pull/1087), [#1086](https://github.com/pilosa/pilosa/pull/1086), [#1026](https://github.com/pilosa/pilosa/pull/1026), [#1022](https://github.com/pilosa/pilosa/pull/1022), [#1007](https://github.com/pilosa/pilosa/pull/1007), [#981](https://github.com/pilosa/pilosa/pull/981), [#901](https://github.com/pilosa/pilosa/pull/901), [#972](https://github.com/pilosa/pilosa/pull/972), [#1215](https://github.com/pilosa/pilosa/pull/1215), [#1213](https://github.com/pilosa/pilosa/pull/1213), [#1224](https://github.com/pilosa/pilosa/pull/1224), [#1250](https://github.com/pilosa/pilosa/pull/1250))

### Changed

- Put Statik behind an interface ([#1163](https://github.com/pilosa/pilosa/pull/1163))
- Refactor diagnostics, inject gopsutil dependency ([#1166](https://github.com/pilosa/pilosa/pull/1166))
- Use boolean instead of address to configure coordinator ([#1158](https://github.com/pilosa/pilosa/pull/1158))
- Put GCNotify behind an interface ([#1148](https://github.com/pilosa/pilosa/pull/1148))
- Replace custom assembly bit functions with standard go ([#797](https://github.com/pilosa/pilosa/pull/797))
- Improve roaring tests ([#1115](https://github.com/pilosa/pilosa/pull/1115))
- Change configuration cluster.type (string) to cluster.disabled (bool) ([#1099](https://github.com/pilosa/pilosa/pull/1099))
- Use NodeID instead of URI for node identification ([#1077](https://github.com/pilosa/pilosa/pull/1077))
- Change gossip config from DefaultLocalConfig to DefaultWANConfig ([#1032](https://github.com/pilosa/pilosa/pull/1032))
- Use binary search in runAdd ([#1027](https://github.com/pilosa/pilosa/pull/1027))
- Use HTTP handler for gossip SendSync ([#1001](https://github.com/pilosa/pilosa/pull/1001))
- Group the write operations in syncBlock by MaxWritesPerRequest ([#950](https://github.com/pilosa/pilosa/pull/950))
- Refactor HTTPClient handling ([#991](https://github.com/pilosa/pilosa/pull/991))
- Remove FrameSchema. Move Fields to the Frame struct ([#907](https://github.com/pilosa/pilosa/pull/907))
- Refactor pilosa/server ([#1220](https://github.com/pilosa/pilosa/pull/1220))
- Clean up flipBitmap and add tests ([#1223](https://github.com/pilosa/pilosa/pull/1223))
- Move pilosa.Config to pilosa/server.Config ([#1216](https://github.com/pilosa/pilosa/pull/1216))
- Vendor github.com/golang/groupcache/lru ([#1221](https://github.com/pilosa/pilosa/pull/1221))

### Removed

- Remove the Gossip stutter from memberlist-related config options ([#1171](https://github.com/pilosa/pilosa/pull/1171))
- Remove old GossipPort and GossipSeed config options ([#1142](https://github.com/pilosa/pilosa/pull/1142))
- Remove cluster type `http` from docs ([#1130](https://github.com/pilosa/pilosa/pull/1130))
- Remove holder.Peek, combine with HasData, move server logic ([#1226](https://github.com/pilosa/pilosa/pull/1226))
- Remove PATCH frame endpoint ([#1222](https://github.com/pilosa/pilosa/pull/1222))
- Remove Index.MergeSchemas() method ([#1219](https://github.com/pilosa/pilosa/pull/1219))
- Remove references to Input Definition from the docs ([#1212](https://github.com/pilosa/pilosa/pull/1212))
- Remove Index.TimeQuantum ([#1209](https://github.com/pilosa/pilosa/pull/1209))
- Remove SecurityManager. Implement api restrictions in api package. ([#1207](https://github.com/pilosa/pilosa/pull/1207))

### Fixed

- Handle the scheme correctly in config.Bind ([#1143](https://github.com/pilosa/pilosa/pull/1143))
- Prevent excessive sendSync (createView) messages. ([#1139](https://github.com/pilosa/pilosa/pull/1139))
- Fix a shift logic bug in bitmapZeroRange ([#1110](https://github.com/pilosa/pilosa/pull/1110))
- Fix node id validation on set-coordinator ([#1102](https://github.com/pilosa/pilosa/pull/1102))
- Avoid overflow bug in differenceRunArray ([#1105](https://github.com/pilosa/pilosa/pull/1105))
- Fix bug in NewServerCluster where each host was its own coordinator ([#1101](https://github.com/pilosa/pilosa/pull/1101))
- Fix count/bitmap mismatch bug ([#1084](https://github.com/pilosa/pilosa/pull/1084))
- Fix edge case with Range() calls outside field Min/Max. Fixes #876. ([#979](https://github.com/pilosa/pilosa/pull/979))
- Bind the handler to all interfaces (0.0.0.0) in Dockerfile. Fixes #977. ([#980](https://github.com/pilosa/pilosa/pull/980))
- Fix nil client bug in monitorAntiEntropy (and test) ([#1233](https://github.com/pilosa/pilosa/pull/1233))
- Fix crash due to server.diagnostics.server not set ([#1229](https://github.com/pilosa/pilosa/pull/1229))
- Fix some cluster race conditions ([#1228](https://github.com/pilosa/pilosa/pull/1228))

### Deprecated

- Deprecate RangeEnabled option ([#1205](https://github.com/pilosa/pilosa/pull/1205))

### Performance

- Add benchmark for various container usage patterns ([#1017](https://github.com/pilosa/pilosa/pull/1017))

## [0.8.8] - 2018-02-19

This version contains 1 contribution from 2 contributors. There are 4 files changed; 1,153 insertions; and 618 deletions.

### Fixed

- Bug fixes and improved test coverage in roaring ([#1118](https://github.com/pilosa/pilosa/pull/1118))

## [0.8.7] - 2018-02-12

This version contains 1 contribution from 1 contributors. There are 2 files changed; 84 insertions; and 4 deletions.

### Fixed

- Fix a shift logic bug in bitmapZeroRange ([#1111](https://github.com/pilosa/pilosa/pull/1111))

## [0.8.6] - 2018-02-09

This version contains 2 contributions from 2 contributors. There are 3 files changed; 171 insertions; and 6 deletions.

### Fixed

- Fix overflow bug in differenceRunArray [#1106](https://github.com/pilosa/pilosa/pull/1106)
- Fix bug where count and bitmap queries could return different numbers [#1083](https://github.com/pilosa/pilosa/pull/1083)

## [0.8.5] - 2018-01-18

This version contains 1 contribution from 1 contributor. There is 1 file changed; 1 insertion, and 0 deletions.

### Fixed

- Bind Docker container on all interfaces ([#1061](https://github.com/pilosa/pilosa/pull/1061))

## [0.8.4] - 2018-01-10

This version contains 4 contributions from 3 contributors. There are 17 files changed; 974 insertions; and 221 deletions.

### Fixed

- Group the write operations in syncBlock by MaxWritesPerRequest ([#1038](https://github.com/pilosa/pilosa/pull/1038))
- Change gossip config from memberlist.DefaultLocalConfig to memberlist.DefaultWANConfig ([#1033](https://github.com/pilosa/pilosa/pull/1033))

### Performance

- Change AttrBlock handler calls to support protobuf instead of json ([#1046](https://github.com/pilosa/pilosa/pull/1046))
- Use RLock instead of Lock in a few places ([#1042](https://github.com/pilosa/pilosa/pull/1042))

## [0.8.3] - 2017-12-12

This version contains 1 contribution from 1 contributor. There are 2 files changed; 59 insertions; and 42 deletions.

### Fixed

- Protect against accessing pointers to memory which was unmapped ([#1000](https://github.com/pilosa/pilosa/pull/1000))

## [0.8.2] - 2017-12-05

This version contains 1 contribution from 1 contributor. There are 15 files changed; 127 insertions; and 98 deletions.

### Fixed

- Modify initialization of HTTP client so only one instance is created ([#994](https://github.com/pilosa/pilosa/pull/994))

## [0.8.1] - 2017-11-15

This version contains 2 contributions from 2 contributors. There are 4 files changed; 27 insertions; and 14 deletions.

### Fixed

- Fix CountOpenFiles() fatal crash ([#969](https://github.com/pilosa/pilosa/pull/969))
- Fix version check when local is greater than pilosa.com ([#968](https://github.com/pilosa/pilosa/pull/968))

## [0.8.0] - 2017-11-15

This version contains 31 contributions from 8 contributors. There are 84 files changed; 3,732 insertions; and 1,428 deletions.

### Added

- Diagnostics ([#895](https://github.com/pilosa/pilosa/pull/895))
- Add docker-build make target for repeatable Docker-based builds ([#933](https://github.com/pilosa/pilosa/pull/933))
- Add documentation on importing field values; fixes #924 ([#938](https://github.com/pilosa/pilosa/pull/938))
- Add flag documentation and tests, remove "plugins.path" ([#942](https://github.com/pilosa/pilosa/pull/942))
- Add TLS support ([#867](https://github.com/pilosa/pilosa/pull/867))
- Add TLS cluster how to ([#898](https://github.com/pilosa/pilosa/pull/898))
- Add support for gossip encryption ([#889](https://github.com/pilosa/pilosa/pull/889))
- Add Recalculate Caches endpoint ([#881](https://github.com/pilosa/pilosa/pull/881))
- Add search-friendly documentation for BSI range query syntax ([#955](https://github.com/pilosa/pilosa/pull/955))

### Changed

- Remove unneeded Gopkg.toml constraints and update all dependencies ([#943](https://github.com/pilosa/pilosa/pull/943))
- Remove row and column labels in webUI ([#884](https://github.com/pilosa/pilosa/pull/884))
- Internal Client refactoring ([#892](https://github.com/pilosa/pilosa/pull/892))
- Remove column/row labels for input definition ([#945](https://github.com/pilosa/pilosa/pull/945))
- Update dependencies and Go version ([#878](https://github.com/pilosa/pilosa/pull/878))

### Fixed

- Skip permissions test when run as root. Fixes #940 ([#941](https://github.com/pilosa/pilosa/pull/941))
- Address "connection reset" issues in client ([#934](https://github.com/pilosa/pilosa/pull/934))
- Fix field value import: Use signed int and respect field minimum ([#919](https://github.com/pilosa/pilosa/pull/919))
- Constrain BoltDB to version rather than specific revision ([#887](https://github.com/pilosa/pilosa/pull/887))
- Fix bug in environment variable format ([#882](https://github.com/pilosa/pilosa/pull/882))
- Fix overflow in differenceRunBitmap ([#949](https://github.com/pilosa/pilosa/pull/949))

### Performance

- Use FieldNotNull to improve efficiency of BETWEEN queries ([#874](https://github.com/pilosa/pilosa/pull/874))

## [0.7.2] - 2017-11-15

This version contains 1 contribution from 1 contributor. There is 1 file changed; 16 insertions; and 1 deletion.

### Changed

- Bump HTTP client's MaxIdleConns and MaxIdleConnsPerHost ([#920](https://github.com/pilosa/pilosa/pull/920))

## [0.7.1] - 2017-10-09

This version contains 3 contributions from 3 contributors. There are 14 files changed; 221 insertions; and 52 deletions.

### Changed

- Update dependencies and Go version ([#878](https://github.com/pilosa/pilosa/pull/878))

### Performance

- Leverage not-null field to make BETWEEN queries more efficient ([#874](https://github.com/pilosa/pilosa/pull/874))

## [0.7.0] - 2017-10-03

This version contains 59 contributions from 9 contributors. There are 61 files changed; 5207 insertions; and 1054 deletions.

### Added

- Add HTTP API for fields ([#811](https://github.com/pilosa/pilosa/pull/811), [#856](https://github.com/pilosa/pilosa/pull/856))
- Add HTTP API for delete views ([#785](https://github.com/pilosa/pilosa/pull/785))
- Modify import endpoint to handle BSI field values ([#840](https://github.com/pilosa/pilosa/pull/840))
- Add field Range() support to Executor ([#791](https://github.com/pilosa/pilosa/pull/791))
- Support PQL Range() queries for fields ([#755](https://github.com/pilosa/pilosa/pull/755))
- Add Sum() field query ([#778](https://github.com/pilosa/pilosa/pull/778))
- Add documentation for BSI ([#861](https://github.com/pilosa/pilosa/pull/861))
- Add BETWEEN for Range queries ([#847](https://github.com/pilosa/pilosa/pull/847))
- Add Xor support for PQL ([#789](https://github.com/pilosa/pilosa/pull/789))
- Enable auto-creating the schema on imports ([#837](https://github.com/pilosa/pilosa/pull/837))
- Update client library docs ([#831](https://github.com/pilosa/pilosa/pull/831))
- Handle SIGTERM signal ([#830](https://github.com/pilosa/pilosa/pull/830))
- Add cluster config example to docs ([#806](https://github.com/pilosa/pilosa/pull/806))
- Add ability to exclude attributes and bits in Bitmap queries ([#783](https://github.com/pilosa/pilosa/pull/783))

### Fixed

- Fix panic when iterating over an empty run container ([#860](https://github.com/pilosa/pilosa/pull/860))
- Fix row id zero bug ([#814](https://github.com/pilosa/pilosa/pull/814))
- Fix cache invalidation bug ([#795](https://github.com/pilosa/pilosa/pull/795))
- Set container.n in differenceRunRun ([#794](https://github.com/pilosa/pilosa/pull/794))
- Fix infinite loop in bitmap-to-array conversion ([#779](https://github.com/pilosa/pilosa/pull/779))
- Fix CountRange bug ([#773](https://github.com/pilosa/pilosa/pull/773))

### Deprecated

- Remove support for row/column labels ([#839](https://github.com/pilosa/pilosa/pull/839))

### Performance

- Refactor differenceRunArray ([#859](https://github.com/pilosa/pilosa/pull/859))
- Update fragment.FieldSum to use roaring IntersectionCount() ([#841](https://github.com/pilosa/pilosa/pull/841))
- Add roaring optimizations ([#842](https://github.com/pilosa/pilosa/pull/842))
- Convert lock to read lock ([#848](https://github.com/pilosa/pilosa/pull/848))
- Reduce Lock calls in executor ([#846](https://github.com/pilosa/pilosa/pull/846))
- Implement container.flipBitmap() to improve differenceRunBitmap() ([#849](https://github.com/pilosa/pilosa/pull/849))
- Reuse container storage on UnmarshalBinary to improve memory utilization ([#820](https://github.com/pilosa/pilosa/pull/820))
- Improve WriteTo performance ([#812](https://github.com/pilosa/pilosa/pull/812))

## [0.6.0] - 2017-08-11

This version contains 14 contributions from 5 contributors. There are 28 files changed; 4,936 insertions; and 692 deletions.

### Added

- Add Run-length Encoding ([#758](https://github.com/pilosa/pilosa/pull/758))

### Changed

- Make gossip the default broadcast type ([#750](https://github.com/pilosa/pilosa/pull/750))

### Fixed

- Fix CountRange ([#759](https://github.com/pilosa/pilosa/pull/759))
- Fix `differenceArrayRun` logic ([#674](https://github.com/pilosa/pilosa/pull/674))

## [0.5.0] - 2017-08-02

This version contains 65 contributions from 8 contributors (including 1 volunteer contributor). There are 79 files changed; 7,972 insertions; and 2,800 deletions.

### Added

- Set open file limit during Pilosa startup ([#748](https://github.com/pilosa/pilosa/pull/748))
- Add Input Definition ([#646](https://github.com/pilosa/pilosa/pull/646))
- Add cache type: None ([#745](https://github.com/pilosa/pilosa/pull/745))
- Add panic recovery in top level HTTP handler ([#741](https://github.com/pilosa/pilosa/pull/741))
- Count open file handles as a StatsD metric ([#636](https://github.com/pilosa/pilosa/pull/636))
- Add coverage tools to Makefile ([#635](https://github.com/pilosa/pilosa/pull/635))
- Add Holder test coverage ([#629](https://github.com/pilosa/pilosa/pull/629))
- Add runtime memory metrics ([#600](https://github.com/pilosa/pilosa/pull/600))
- Add sorting flag to import command ([#606](https://github.com/pilosa/pilosa/pull/606))
- Add PQL support for field values (WIP) ([#721](https://github.com/pilosa/pilosa/pull/721))
- Set and retrieve field values (WIP) ([#702](https://github.com/pilosa/pilosa/pull/702))
- Add BSI range-encoding schema support (WIP) ([#670](https://github.com/pilosa/pilosa/pull/670))

### Changed

- Move InternalPort config option to top-level ([#747](https://github.com/pilosa/pilosa/pull/747))
- Switch from glide to dep for dependency management ([#744](https://github.com/pilosa/pilosa/pull/744))
- Remove QueryRequest.Quantum since it is no longer used ([#699](https://github.com/pilosa/pilosa/pull/699))
- Refactor test utilities into importable package ([#675](https://github.com/pilosa/pilosa/pull/675))

### Fixed

- Add mutex for attribute cache ([#729](https://github.com/pilosa/pilosa/pull/729))
- Use log-path flag to specify log file ([#678](https://github.com/pilosa/pilosa/pull/678))

## [0.4.0] - 2017-06-08

This version contains 53 contributions from 13 contributors (including 4 volunteer contributors). There are 96 files changed; 6373 insertions; and 770 deletions.

*Note that data files created in Pilosa < 0.4.0 are not compatible with Pilosa 0.4.0 as a result of [#520](https://github.com/pilosa/pilosa/pull/520).*

### Added
- Support metric reporting through StatsD protocol ([#468](https://github.com/pilosa/pilosa/pull/468), [#568](https://github.com/pilosa/pilosa/pull/568), [#580](https://github.com/pilosa/pilosa/pull/580))
- Improve test coverage for ctl package ([#586](https://github.com/pilosa/pilosa/pull/586))
- Add support for bit flip (negate) in roaring ([#592](https://github.com/pilosa/pilosa/pull/592))
- Add xor support to roaring ([#571](https://github.com/pilosa/pilosa/pull/571))
- Improve WebUI autocomplete ([#560](https://github.com/pilosa/pilosa/pull/560))
- Add syntax hints tooltip to WebUI ([#537](https://github.com/pilosa/pilosa/pull/537))
- Implement 'config' CLI command ([#541](https://github.com/pilosa/pilosa/pull/541))
- Move docs into repo ([#563](https://github.com/pilosa/pilosa/pull/563))
- Add inverse TopN() support ([#551](https://github.com/pilosa/pilosa/pull/551))
- Add various Makefile updates ([#540](https://github.com/pilosa/pilosa/pull/540))
- Provide details on Glide checksum mismatch ([#546](https://github.com/pilosa/pilosa/pull/546))
- Add Docker multi-stage build ([#535](https://github.com/pilosa/pilosa/pull/535))
- Support inverse Range() queries ([#533](https://github.com/pilosa/pilosa/pull/533))
- Support colon commands in WebUI ([#529](https://github.com/pilosa/pilosa/pull/529), [#510](https://github.com/pilosa/pilosa/pull/510))

### Changed
- Increase default partition count from 16 to 256 (BREAKING CHANGE) ([#520](https://github.com/pilosa/pilosa/pull/520))
- Validate unknown query params ([#578](https://github.com/pilosa/pilosa/pull/578))
- Validate configuration file ([#573](https://github.com/pilosa/pilosa/pull/573))
- Change default cache type to ranked ([#524](https://github.com/pilosa/pilosa/pull/524))
- Add max-writes-per-requests limit ([#525](https://github.com/pilosa/pilosa/pull/525))

### Fixed
- Add "make test" to PHONY section of Makefile ([#605](https://github.com/pilosa/pilosa/pull/605))
- Fix failing tests when IPv6 is disabled ([#594](https://github.com/pilosa/pilosa/pull/594))
- Add minor docs fix, indent in JSON ([#599](https://github.com/pilosa/pilosa/pull/599))
- Fix BroadcastHandler handle missing index error ([#597](https://github.com/pilosa/pilosa/pull/597))
- Add WebUI fixes ([#589](https://github.com/pilosa/pilosa/pull/589))
- Fix support for 32-bit Linux ([#549](https://github.com/pilosa/pilosa/pull/549), [#565](https://github.com/pilosa/pilosa/pull/565))
- Fix 3 separate bugs in bitmapCountRange ([#559](https://github.com/pilosa/pilosa/pull/559))
- Add client support for MaxInverseSliceByIndex ([#555](https://github.com/pilosa/pilosa/pull/555))
- Fix bug in `handleGetSliceMax` ([#554](https://github.com/pilosa/pilosa/pull/554))
- Default to `standard` view in export command ([#548](https://github.com/pilosa/pilosa/pull/548))
- Fix vet issues with the assembly code in Roaring ([#528](https://github.com/pilosa/pilosa/pull/528))
- Prevent row labels that match the column label ([#503](https://github.com/pilosa/pilosa/pull/503))
- Fix roaring test: TestBitmap_Quick_Array1 ([#507](https://github.com/pilosa/pilosa/pull/507))
- Don't try to create inverse views on Import() when inverseEnabled is false ([#462](https://github.com/pilosa/pilosa/pull/462))

### Performance
- Set n based on array length instead of incrementing repeatedly ([#590](https://github.com/pilosa/pilosa/pull/590))
- Rewrite intersectCountArrayBitmap for perf test ([#577](https://github.com/pilosa/pilosa/pull/577))
- Check for duplicate attributes under read lock on insert ([#562](https://github.com/pilosa/pilosa/pull/562))

[Unreleased]: https://github.com/pilosa/pilosa/compare/v0.5...HEAD
[0.4.0]: https://github.com/pilosa/pilosa/compare/v0.3...v0.4
[0.5.0]: https://github.com/pilosa/pilosa/compare/v0.4...v0.5
[0.6.0]: https://github.com/pilosa/pilosa/compare/v0.5...v0.6
[0.7.0]: https://github.com/pilosa/pilosa/compare/v0.6...v0.7
[0.8.0]: https://github.com/pilosa/pilosa/compare/v0.7...v0.8
[0.9.0]: https://github.com/pilosa/pilosa/compare/v0.8...v0.9
[0.10.0]: https://github.com/pilosa/pilosa/compare/v0.9...v0.10
[1.0.0]: https://github.com/pilosa/pilosa/compare/v0.10...v1.0
[1.1.0]: https://github.com/pilosa/pilosa/compare/v1.0...v1.1
