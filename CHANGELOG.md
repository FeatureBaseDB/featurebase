# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.8.3] - 2017-12-12

This version contains 1 contribution from 1 contributor. There are 2 files changed, 59 insertions, and 42 deletions.

### Fixed

- Protect against accessing pointers to memory which was unmapped ([#1000](https://github.com/pilosa/pilosa/pull/1000))

## [0.8.2] - 2017-12-05

This version contains 1 contribution from 1 contributor. There are 15 files changed, 127 insertions, and 98 deletions.

### Fixed

- Modify initialization of HTTP client so only one instance is created ([#994](https://github.com/pilosa/pilosa/pull/994))

## [0.8.1] - 2017-11-15

This version contains 2 contributions from 2 contributors. There are 4 files changed, 27 insertions, and 14 deletions.

### Fixed

- Fix CountOpenFiles() fatal crash ([#969](https://github.com/pilosa/pilosa/pull/969))
- Fix version check when local is greater than pilosa.com ([#968](https://github.com/pilosa/pilosa/pull/968))

## [0.8.0] - 2017-11-15

This version contains 31 contributions from 8 contributors. There are 84 files changed, 3,732 insertions, and 1,428 deletions.

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

This version contains 1 contribution from 1 contributor. There is 1 file changed, 16 insertions, and 1 deletion.

### Changed

- Bump HTTP client's MaxIdleConns and MaxIdleConnsPerHost ([#920](https://github.com/pilosa/pilosa/pull/920))

## [0.7.1] - 2017-10-09

This version contains 3 contributions from 3 contributors. There are 14 files changed, 221 insertions, and 52 deletions.

### Changed

- Update dependencies and Go version ([#878](https://github.com/pilosa/pilosa/pull/878))

### Performance

- Leverage not-null field to make BETWEEN queries more efficient ([#874](https://github.com/pilosa/pilosa/pull/874))

## [0.7.0] - 2017-10-03

This version contains 59 contributions from 9 contributors. There are 61 files changed, 5207 insertions, and 1054 deletions.

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

This version contains 14 contributions from 5 contributors. There are 28 files changed, 4,936 insertions, and 692 deletions.

### Added

- Add Run-length Encoding ([#758](https://github.com/pilosa/pilosa/pull/758))

### Changed

- Make gossip the default broadcast type ([#750](https://github.com/pilosa/pilosa/pull/750))

### Fixed

- Fix CountRange ([#759](https://github.com/pilosa/pilosa/pull/759))
- Fix `differenceArrayRun` logic ([#674](https://github.com/pilosa/pilosa/pull/674))

## [0.5.0] - 2017-08-02

This version contains 65 contributions from 8 contributors (including 1 volunteer contributor). There are 79 files changed, 7,972 insertions, and 2,800 deletions.

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

This version contains 53 contributions from 13 contributors (including 4 volunteer contributors). There are 96 files changed, 6373 insertions, and 770 deletions.

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
