# mini-kv

A mini KV server in Rust.

This is a port from the C project [smb374/redis-c](https://github.com/smb374/redis-c) to make certain things easier to maintain while keeping minimal dependencies and add RESP2 support. See that repo for most of the details.

For the list of commands supported see the C code repo or go to [Build Your Own Redis](https://build-your-own.org/redis/#table-of-contents) to check it as the project and the C code implements the list of commands from the book.

## Dependencies

- [bytes](https://crates.io/crates/bytes): Efficient byte buffer.
- [crossbeam](https://crates.io/crates/crossbeam): Mainly 2 use submodules
  - `epoch` for validated EBR instead of the home-baked QSBR in the C code repo.
  - `channel` for faster inter-thread channel implementation than `std::sync::mpsc`.
- [mio](https://crates.io/crates/mio): Low-level system IO polling mechanism wrapper for implementing non-blocking IO event loop.
- [nix](https://crates.io/crates/nix): Only uses `signal` feature to have safe(r) sigaction for graceful shutdown on `SIGINT` & `SIGTERM`.
- [nom](https://crates.io/crates/nom): Parser combinator library for RESP2 parser.
- [slab](https://crates.io/crates/slab): Slab allocator for connection storage.
- [twox-hash](https://crates.io/crates/twox-hash): Provide XXH3-64 used as hasher for the concurrent hashmap.
- [shuttle](https://crates.io/crates/shuttle): Randomized tests for the concurrent hashmap when feature `shuttle` is enabled for test.

## Design update

- Connection & entry expiry are now all handled with a deque, using FIFO-reinsertion like mechanism instead of LRU-like mechanism, reducing complexity and entry expiry now doesn't rely on concurrent skiplist with this.
- ZSet is now implemented by a `HashMap<Arc<str>, f64>` with a `BTreeSet<([u8; 8], Arc<str>)>`. The `[u8; 8]` is obtained by using memcmp friendly encoding on `f64` to use lexicographic order and naturally fits with string comparison.
- Connection is now handled by workers instead of letting main thread does all the IO then dispatch commands to each worker, this has made command pipelining trivial and can be executed in command order. Scheduling is still done by Round-Robin.
- Server now defaults to use 4 instead of 8 workers, resulting in 1+4 configuration.

## Benchmark result

Since the server now uses RESP2 it can be benchmarked with `redis-benchmark`.
Below it describes the parameter, hardware, and comparison with [ValKey](https://github.com/valkey-io/valkey), which is the default Redis implementation in Arch Linux's Package Repo.

### Benchmark Parameter

- `-P 16`: pipeline 16 commands in 1 connection.
- `-d 1024`: 1 KB data size
- `--threads 4`: Use 4 threads
- `-t set,get`: Bench `SET` then `GET` with builtin commands
- `-n 5000000`: Total 5M requests
- `-c 200`: 200 Concurrent Clients
- `-r 1000000000000`: Large key space for less key collision

### Hardware & OS

- CPU: Ryzen 5 9600X
- RAM Size: 32 GB
- `uname -a`: `Linux smb374-arch 6.17.9-zen1-1-zen #1 ZEN SMP PREEMPT_DYNAMIC Mon, 24 Nov 2025 15:21:16 +0000 x86_64 GNU/Linux`
- Network: Traffic go through loopback interface

### Results

This repo:

```csv
# redis-benchmark -P 16 -d 1024 --threads 4 -t set,get -n 5000000 -c 200 -r 1000000000000 --csv
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","3331112.50","0.862","0.008","0.399","3.191","3.807","5.767"
"GET","5000000.00","0.535","0.008","0.287","1.919","2.247","3.487"
```

ValKey:

```csv
# redis-benchmark -P 16 -d 1024 --threads 4 -t set,get -n 5000000 -c 200 -r 1000000000000 --csv
"test","rps","avg_latency_ms","min_latency_ms","p50_latency_ms","p95_latency_ms","p99_latency_ms","max_latency_ms"
"SET","2217294.75","1.305","0.544","1.191","2.151","2.407","4.431"
"GET","2497502.50","1.209","0.192","1.167","1.383","2.343","3.711"
```

## Future Work

- Finish example client in `src/client`.
- Go with full Redis semantic support or at least similar command format
- Add benchmark for the Rust implementation of the concurrent Hopscotch hashmap and compare it with the results in the C code repo by Google Benchmark.
- Other plans mentioned in the C code repo.
