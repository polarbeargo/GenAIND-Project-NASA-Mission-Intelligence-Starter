# Context Compression Benchmark CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--runs` | `30` | Benchmark iterations per dataset size |
| `--sizes` | `512,1024,2048,4096` | Comma-separated input sizes (number of chunks) |
| `--seed` | `42` | Random seed for reproducible datasets |
| `--equivalence` | `once` | Run output parity checks `once`, `per-run`, or turn them `off` outside the timed region |
| `--warmup` | `0` | Untimed warmup iterations per dataset size before samples are collected |

## Recommended Modes

- `--equivalence once`: good default for benchmark runs with low overhead.
- `--equivalence per-run`: strict correctness verification for every sampled dataset.
- `--equivalence off`: pure timing mode when parity has already been validated.
- `--warmup 1` or `--warmup 2`: useful when you want cleaner repeated-run timing.
