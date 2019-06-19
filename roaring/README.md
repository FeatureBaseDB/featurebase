# The Fuzzer

For complete documentation on go-fuzz, please see: https://github.com/dvyukov/go-fuzz

The fuzzer in relation to the roaring package checks the `Bitmap.UnmarshalBinary` function found in `roaring.go`. In order to use the fuzzer, you can follow these steps:

`cd $GOPATH/src/github.com/pilosa/pilosa/roaring`

`go-fuzz-build ./`

You must now make the workdir/corpus directory. This is achieved by:

`mkdir workdir/corpus`

The fuzzer needs some input to start the fuzzing with. Copy some sample Pilosa fragments into the workdir/corpus folder. For example:

`cp ~/.pilosa/my-index/my-field/views/standard/fragments/0 workdir/corpus`

Once you have copied your sample inputs, you are ready to run the fuzzer:

`go-fuzz -bin=roaring-fuzz.zip -workdir=workdir -func=FuzzBitmapUnmarshalBinary`

## Understanding the Fuzzer Output

The fuzzer will output something similar to the follwoing:

`2015/04/25 12:39:53 workers: 8, corpus: 124 (12s ago), crashers: 37, restarts: 1/15, execs: 35342 (2941/sec), cover: 403, uptime: 12s`

The most important part of the output is the `crashers` and `cover`. The crashers records how many combinations were discovered that fail and the cover tells you how much code is being accessed.
For a complete explanation of the output, please see: https://github.com/dvyukov/go-fuzz.

The fuzzer will document the crashers in a folder labeled "crashers." It will record the fragment and the error that was produced in two separate files within this folder. This is the final product.

Happy Fuzzing!
