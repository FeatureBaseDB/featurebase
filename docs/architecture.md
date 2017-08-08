+++
title = "Architecture"
+++

## Architecture

### Roaring bitmap storage format

Bitmaps are persisted to disk using a file format very similar to the [Roaring Bitmap format spec](https://github.com/RoaringBitmap/RoaringFormatSpec). Pilosa's format uses 64-bit IDs, so it is not binary-compatible with the spec. Some parts of the format are simpler, and an additional section is included. Specific differences include:

* The cookie is always bytes 0-3; the container count is always bytes 4-7, never bytes 2-3.
* The cookie includes file format version in bytes 2-3 (currently equal to zero).
* The offset header section is always included.
* RLE runs are serialized as [start, last], not [start, length].
* After the container storage section is an operation log, of unspecified length.

![roaring file format diagram](/img/docs/pilosa-roaring-storage-diagram.svg)

All values are little-endian. The first two bytes of the cookie is 12346 when the file contains no RLE containers, or 12347 when it does. In the no-RLE case, the runFlagBitset is absent. Otherwise the format is identical in both cases. Container types are determined by their cardinality - a container with 4096 or more values is a bitmap, a container with fewer is an array or RLE container. A high bit in runFlagBitset indicates an RLE container.

Storing the runFlagBitset in a separate section, indicated by the cookie value, keeps this format backward compatible with older storage versions that do not support RLE containers.
