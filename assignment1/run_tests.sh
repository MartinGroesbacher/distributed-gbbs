#!/usr/bin/env bash

set -uo pipefail

# ------------------------------------------------------------
# Assignment 1: Randomized MIS test runner
#
# Builds the student implementation and tests it on every
# graph in assignment1/test_graphs/.
# ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

GRAPH_DIR="$SCRIPT_DIR/test_graphs"

# Maximum wall-clock time for each graph execution.
TIMEOUT_SECONDS=30

# Maximum number of simulated communication rounds.
MAX_ROUNDS=10000

echo "========================================"
echo "Assignment 1: Randomized MIS Test Suite"
echo "========================================"
echo

# ------------------------------------------------------------
# Build executable once
# ------------------------------------------------------------

echo "Building student executable..."
echo

cd "$REPO_ROOT" || exit 1

if ! bazel build //assignment1:mis; then
    echo
    echo "[ERROR] Could not build the student executable."
    exit 1
fi

EXECUTABLE="$REPO_ROOT/bazel-bin/assignment1/mis"

if [[ ! -x "$EXECUTABLE" ]]; then
    echo "[ERROR] Executable not found: $EXECUTABLE"
    exit 1
fi

# ------------------------------------------------------------
# Run tests
# ------------------------------------------------------------

passed=0
failed=0
total=0

shopt -s nullglob

graphs=("$GRAPH_DIR"/*)

if [[ ${#graphs[@]} -eq 0 ]]; then
    echo "[ERROR] No test graphs found in $GRAPH_DIR"
    exit 1
fi

echo
echo "Running tests..."
echo

for graph in "${graphs[@]}"; do

    [[ -f "$graph" ]] || continue

    name="$(basename "$graph")"

    total=$((total + 1))

    # Capture stdout and stderr for this test.
    output_file="$(mktemp)"

    MIS_MAX_ROUNDS="$MAX_ROUNDS" \
        timeout "$TIMEOUT_SECONDS" \
        "$EXECUTABLE" -s "$graph" \
        >"$output_file" 2>&1

    exit_code=$?

    if [[ $exit_code -eq 0 ]] &&
       grep -q "MIS verification passed!" "$output_file"; then

        echo "[PASS] $name"

        passed=$((passed + 1))

    else

        echo "[FAIL] $name"

        if [[ $exit_code -eq 124 ]]; then
            echo "       Reason: Execution timed out."
        else
            echo "       Exit code: $exit_code"
        fi

        echo
        echo "       Program output:"
        sed 's/^/       /' "$output_file"
        echo

        failed=$((failed + 1))
    fi

    rm -f "$output_file"

done

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------

echo
echo "========================================"
echo "Test Summary"
echo "========================================"

echo "Passed: $passed"
echo "Failed: $failed"
echo "Total:  $total"

echo "========================================"

if [[ $failed -eq 0 ]]; then
    echo "All tests passed!"
    exit 0
else
    echo "Some tests failed."
    exit 1
fi
