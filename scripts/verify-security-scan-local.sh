#!/usr/bin/env bash
set -euo pipefail

# Local verifier for the Semgrep portion of .github/workflows/security-scan.yml.
# It extracts the embedded rule file from the workflow, validates it, runs a scan,
# and applies the same CI threshold logic.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKFLOW_FILE="${REPO_ROOT}/.github/workflows/security-scan.yml"

# Match CI behavior:
# - PR/push warning limit: 5
# - schedule warning limit: 10
EVENT_NAME="${GITHUB_EVENT_NAME:-pull_request}"
WARNING_LIMIT=5
if [[ "${EVENT_NAME}" == "schedule" ]]; then
  WARNING_LIMIT=10
fi

if [[ ! -f "${WORKFLOW_FILE}" ]]; then
  echo "ERROR: Workflow file not found: ${WORKFLOW_FILE}" >&2
  exit 2
fi

if ! command -v semgrep >/dev/null 2>&1; then
  echo "ERROR: semgrep is not installed in the current environment." >&2
  echo "Install with: python -m pip install semgrep" >&2
  exit 2
fi

RULES_FILE="$(mktemp /tmp/llm-vuln-rules.XXXXXX.yaml)"
REPORT_FILE="$(mktemp /tmp/semgrep-local-report.XXXXXX.json)"
SCAN_LOG="$(mktemp /tmp/semgrep-local.XXXXXX.log)"

cleanup() {
  rm -f "${RULES_FILE}" "${REPORT_FILE}" "${SCAN_LOG}"
}
trap cleanup EXIT

cd "${REPO_ROOT}"

# Extract only the heredoc body that defines semgrep-rules/llm-vulnerabilities.yaml.
awk '
  /cat > semgrep-rules\/llm-vulnerabilities.yaml << '\''EOF'\''/ {capture=1; next}
  /^[[:space:]]*EOF[[:space:]]*$/ {capture=0}
  capture {print}
' "${WORKFLOW_FILE}" > "${RULES_FILE}"

if [[ ! -s "${RULES_FILE}" ]]; then
  echo "ERROR: Failed to extract Semgrep rules from workflow." >&2
  exit 2
fi

echo "[1/3] Validating Semgrep rule configuration..."
semgrep --validate --config "${RULES_FILE}"

echo "[2/3] Running Semgrep scan..."
semgrep \
  --config "${RULES_FILE}" \
  . \
  --exclude test/ \
  --json \
  -o "${REPORT_FILE}" \
  >"${SCAN_LOG}"

echo "[3/3] Applying CI-equivalent threshold logic..."
python3 - "${REPORT_FILE}" "${WARNING_LIMIT}" << 'PYTHON_SCRIPT'
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
warning_limit = int(sys.argv[2])

report = json.loads(report_path.read_text(encoding="utf-8"))
warning_count = 0
critical = 0
high = 0
for finding in report.get("results", []):
    severity = str(finding.get("extra", {}).get("severity", "WARNING")).upper()
    if severity == "CRITICAL":
        critical += 1
    elif severity in {"HIGH", "ERROR"}:
        high += 1
    elif severity == "WARNING":
        warning_count += 1

print("CI_THRESHOLD_SUMMARY")
print(f"critical={critical} high={high} warning={warning_count} warning_limit={warning_limit}")

if critical > 0:
    print("GATE_STATUS: FAIL (CRITICAL > 0)")
    raise SystemExit(1)
if high > 0:
    print("GATE_STATUS: FAIL (HIGH > 0)")
    raise SystemExit(1)
if warning_count > warning_limit:
    print(f"GATE_STATUS: FAIL (WARNING > {warning_limit})")
    raise SystemExit(1)

print("GATE_STATUS: PASS")
PYTHON_SCRIPT

echo "Done. Local Semgrep gate matches CI logic for event '${EVENT_NAME}'."
