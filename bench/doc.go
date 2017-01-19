// bench contains benchmarks and common utilities useful to benchmarks
//
// In order to write new benchmarks, one must satisfy the Benchmark and Command
// interfaces in bench.go. In order to use the benchmark from pilosactl, it
// needs to be wired in in two places. The first is BagentCommand.ParseFlags,
// where a case statement needs to be added, and the second is just adding the
// benchmark to the BagentCommand.Usage usage string.
//
// When writing a new benchmark, there are a few things to keep in mind other
// than just implementing the interface:
//
// 1. The benchmark should modify its own configuration in its Init method based
// on the agentNum it is given. How it modifies is specific to the benchmark,
// but the idea is that it should make sense to call the benchmark with the same
// configuration, but multiple different agent numbers, and it should do useful
// work each time (i.e. not just setting the same bits, or running the same
// queries).
//
// 2. The Init method should do everything that needs to be done to get the
// benchmark to a runnable state - all code in run should be the stuff that we
// actually want to time.
//
// 3. The Run method does not need to report the total runtime - that is collected
// by calling code.
//
// 4. Usage should follow the format in other benchmarks, and explain how the
// benchmark uses agentNum to modify its behavior
//
//
// Files:
//
// 1. client.go contains pilosa client code which is shared by many benchmarks
//
// 2. errgroup.go contains the ErrGroup implementation copied from golang.org/x/
// so as not to pull in a bunch of useless deps.
//
// 3. stats.go contains useful code for gathering stats about a series of timed
// operations.
package bench
