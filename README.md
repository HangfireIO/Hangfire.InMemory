# Hangfire.Memory

This is an attempt to create an efficient transactional in-memory storage for Hangfire with data structures close to their optimal representation. The result of this attempt should enable production-ready usage of this storage implementation and handle particular properties of in-memory processing like avoiding `OutOfMemoryException` at any cost and moderate load on garbage collection. Of course we can't avoid them entirely, but at least can control them somehow.

Read and write queries are processed by a single thread to avoid additional synchronization between threads and keep everything as simple as possible (keeping future async-based implementation in mind). Monitoring API also uses that dedicated thread, but its future implementation can be changed by using concurrent data structures and immutability, but I expect this will increase load on garbage collection.

Distributed locks (heh, in an in-process storage), queue fetch logic (even from multiple queues) and transactional queries are implemented as blocking operations, so there is no active polling in these cases. Every data returned by storage can be safely changed without causing underlying storage state to be changed with bypassing required transactional processing logic, so everything is safe (but increase load on GC). Every data structure, including indexes and their records, is removed when empty to avoid memory leaks.



### TODO

* Control `OutOfMemoryException` by providing some kind of limits which can be established easily, e.g. total number of jobs.
* Avoid unnecessary object allocations without sacrificing the safety property (as described above).
* Add integration (for public API) and unit tests (for internal API).
* Force expiration when memory pressure is high to avoid `OutOfMemoryException`.
* Add overridden default for expiration time for jobs and batches?
* Can avoid synchronization in some read-only methods in the `MemoryConnection`class.
