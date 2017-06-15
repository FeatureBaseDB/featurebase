# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.4.0] - 2017-06-08

This version contains 53 contributions from 13 contributors (including 4 volunteer contributors). There are 96 files changed, 6373 insertions, and 770 deletions. Test coverage is currently 73%.

*Note that data files created in Pilosa < 0.4.0 are not compatible with Pilosa 0.4.0 as a result of [#520](https://github.com/pilosa/pilosa/pull/520).*

### Added
- Support metric reporting through StatsD protocol ([#468](https://github.com/pilosa/pilosa/pull/468), [#568](https://github.com/pilosa/pilosa/pull/568), [#580](https://github.com/pilosa/pilosa/pull/580))
- Improve test coverage for ctl package ([#586](https://github.com/pilosa/pilosa/pull/586))
- Add support for bit flip (negate) in roaring ([#592](https://github.com/pilosa/pilosa/pull/592))
- Add xor support to roaring ([#571](https://github.com/pilosa/pilosa/pull/571))
- Improve WebUI autocomplete ([#560](https://github.com/pilosa/pilosa/pull/560))
- Add syntax hints tooltip to WebUI ([#537](https://github.com/pilosa/pilosa/pull/537))
- Implement 'config' CLI command. ([#541](https://github.com/pilosa/pilosa/pull/541))
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

[Unreleased]: https://github.com/pilosa/pilosa/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/pilosa/pilosa/compare/v0.3.1...v0.4.0
