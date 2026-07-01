# Judge API Contract

`/chat` responses include a `judge` object. The following fields are part of the contract:

| Field | Type | Description |
|---|---|---|
| `passed` | `bool` | Final gate decision from judge scoring logic. `true` means the answer passed configured thresholds. |
| `low_confidence` | `bool` | Indicates potentially unreliable output quality or uncertain judge outcome. |
| `overall_score` | `float` | Aggregate quality score in the range `[0.0, 1.0]` (weighted across groundedness, safety, and task success). |
| `source` | `string` | Judge result origin. Common values: `llm`, `heuristic`, `async`, `disabled`, `policy`. |
| `rationale` | `string` | Human-readable explanation of why the judge produced the result. |

## Behavior by mode

- `judge_mode=sync`: `judge` contains final scoring and decision fields on the same response.
- `judge_mode=async`: `/chat` returns a pending judge status quickly; finalized results are queryable via `/monitoring/judge` and `/judge/last`.
- `judge_mode=off`: judge execution is skipped and `source` is `disabled`.

## Notes

- `overall_score` is expected only when a completed score is available (typically sync mode or completed async records).
- Blocked preflight responses return a policy-derived judge object with `source=policy`.
- HTTP-level schema stability for `/chat` (including `judge_mode=sync|async|off`) is regression-tested in `test/test_chat_contract_api.py`.

How each mode fits production

1. sync  
Best for high-assurance routes where correctness matters more than latency.

2. async  
Best default for general production traffic. Fast response first, quality verdict shortly after.

3. off  
Useful for incident fallback, load-shedding, or internal non-critical workloads.

Recommended production policy

1. Default to async for most traffic.  
2. Use sync for premium/high-risk endpoints.  
3. Keep off only as controlled fallback with alerting.  
4. Track judge pass rate and low_confidence rate as release gates.  