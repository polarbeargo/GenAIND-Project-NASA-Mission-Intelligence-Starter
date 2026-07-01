# Context Compression Benchmark

`benchmarks/benchmark_context_compression.py` measures the naive baseline dedup path against the optimized (blocked/cached/short-circuit) path side-by-side, with correctness assertions that fail fast if both paths diverge.

```bash
# Quick smoke run: 10 rounds × 2 dataset sizes
uv run python benchmarks/benchmark_context_compression.py --runs 10 --sizes 512,1024 --equivalence once
```

**CLI flags:** [Context Compression Benchmark CLI Flags](context-compression-cli-flags.md)

**Sample output (10 runs × 512 and 1024 chunks):**

![Context Compression Benchmark](../images/context_compression_benchmark.png)
> The optimized path (`use_optimized_dedup=True` in `CompressionConfig`) is **gated off by default** in production.
> Enable it only if your dataset shows a consistent speedup above ~1.1x before switching.
