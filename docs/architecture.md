+++
title = "Architecture"
weight = 6
nav = []
+++

## Architecture

### Roaring bitmap storage format

Bitmaps are persisted to disk using a file format very similar to the [Roaring Bitmap format spec](https://github.com/RoaringBitmap/RoaringFormatSpec). Pilosa's format uses 64-bit IDs, so it is not binary-compatible with the spec. Some parts of the format are simpler, and an additional section is included. Specific differences include:

* The cookie is always bytes 0-3; the container count is always bytes 4-7, never bytes 2-3.
* The cookie includes file format version in bytes 2-3 (currently equal to zero).
* The descriptive header includes, for each container, a 64-bit key, a 16-bit cardinality, and a 16-bit container type (which only uses two bits now). This makes the runFlag bitset unnecessary. This is in contrast to the spec, which stores a 16-bit key and a 16-bit cardinality.
* The offset header section is always included.
* RLE runs are serialized as [start, last], not [start, length].
* After the container storage section is an operation log, of unspecified length.

![roaring file format diagram](/img/docs/pilosa-roaring-storage-diagram.png)

All values are little-endian. The first two bytes of the cookie is 12348, to reflect incompatibility with the spec, which uses 12346 or 12347. Container types are NOT inferred from their cardinality as in the spec. Instead, the container type is read directly from the descriptive header.
