+++
title = "PDK"
weight = 11
nav = [
    "Library",
    "Examples",
]
+++

## PDK

The [Pilosa Dev Kit](https://github.com/pilosa/pdk) contains Go libraries to help you use Pilosa effectively. From importing data quickly, to managing the mappings from contiguous integer ids to values of other types, the PDK should help you get off the ground quickly.

The PDK also contains some fully worked examples which make use of its tools. These are available in the `usecase` subdirectory and can be run as subcommands of the `pdk` binary.

### Library

#### Mapping

Importing data into Pilosa is dependent on mapping it to integer IDs. PDK provides some predefined functions for inline mapping to simplify this process, supported by a framework for linking these mappings with the associated fields in a source CSV file. If no custom mapping is necessary, the entire import process can be described by an import definition file. The file is composed of four main parts:

* an enumeration of field names
* a list of parsers that are used to parse strings in the CSV to values
* a list of commonly used, named, mapper functions
* a list of ParserMappers - objects that encapsulate all of the work related to a single frame. 

This definition file can quickly get long, and defining it manually would be quite tedious. That's why we have a tool to generate a definition file by looking at a data set. This will handle most of the legwork, but since it can only guess at the application, it uses the simplest mappings - each column gets mapped to one frame in an appropriate way. This is intended as a starting point, to be updated to suit your use of the PDK.

With this definition available, the PDK tool can run the import, which consists of these steps:

- create the index
- create all frames
- for each CSV file, read all rows
- for each CSV record: 
  - generate a columnID
  - apply all ParserMappers, generating a list of (frame, ID) pairs
  - set the appropriate bit. schematically: SetBit(id=rowID, frame=frame, profileID=columnID)

The process is summarized in this flowchart:

![Bitmapper flowchart](/img/docs/pdk-bitmapper-flowchart.svg)


Some of the simple mapper functions available with PDK include:

* YearMapper: Maps a `time.Time` value to an integer equal to the `Time`'s year. 
* MonthMapper: Maps a `time.Time` value to an integer equal to the `Time`'s month, in [0, 11].
* DayOfWeekMapper: Maps a `time.Time` value to an integer equal to the `Time`'s day of the week, in [0, 6].
* HourMapper: Maps a `time.Time` value to an integer equal to the `Time`'s hour, in [0, 23].
* TimeOfDayMapper: Maps a `time.Time` value to the range [0, N-1], where N is specified by `Res`. This is useful if the resolution used by HourMapper is too small (or large). For example, TimeOfDayMapper with `Res`=48 maps to 48 half-hour bins.
* BoolMapper: Maps a boolean value to the range [0, 1].
* IntMapper: Maps an integer value to the range [Min, Max]. This is suitable for a field with a small- to moderate-sized domain.
* SparseIntMapper: Maps integer values through an arbitrary table, foreign keys for example. This is suitable if the table size is small.
* LinearFloatMapper: Maps floating point values through a linear function. Inputs in the range [`Min`, `Max`] are mapped to row IDs in the range [0, `Res - 1`], where each ID represents one of `Res` evenly spaced buckets.
* FloatMapper: Maps floating point values using arbitrary buckets, in case even spacing is not suitable. These buckets are specified with an array of floats representing the left end of each bucket.
* GridMapper: Maps a pair of floats to a single integer, identifying a cell in a rectangular grid. This can be used, for example, to represent (latitude, longitude) location coarsely, as in the taxi data example.
* CustomMapper: When none of the predefined mappers will work, or when multiple fields determine a row ID value, an arbitrary mapping function can be used. Define a function in Go, with the necessary behavior, and wrap it in a CustomMapper.

### Examples

Run `make install` to build and install the `pdk` binary which contains all the examples. Just running `pdk` will bring up a list of all the examples, with a brief description of each. `pdk help <example>` will bring up a more detailed description of that example along with all arguments that it accepts to configure its functionality.

<!--
#### Net

A detailed discussion of using Pilosa to index network traffic data is available [here - TODO]link blog post). This will discuss the implementation of `pdk net` as it relates to the use of the PDK library tools.
-->
