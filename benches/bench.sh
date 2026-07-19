#!/usr/bin/env bash
# Shared benchmark runner. Source this file and call bench_run.
#
# Usage in a test script:
#   source "${ROOT_DIR}/benches/bench.sh"
#   bench_run "test_name" "command to run" "$@"
#
# Flags forwarded from the caller:
#   --prepare CMD          command hyperfine runs before each timed run (reset state)
#   --save-baseline NAME   save results as a named baseline
#   --baseline NAME        compare against a named baseline (default: previous temp)
#
# Environment:
#   RUNS    hyperfine run count  (default: 3)
#   WARMUP  hyperfine warmup     (default: 1)

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_COMPARE="${BENCH_DIR}/compare.py"

bench_run() {
    local name="$1" cmd="$2"
    shift 2

    # -- Parse flags ---------------------------------------------------------
    local prepare_cmd="" save_baseline="" baseline=""
    while [[ $# -gt 0 ]]; do
        case $1 in
            --prepare)       prepare_cmd="$2";    shift 2 ;;
            --save-baseline) save_baseline="$2"; shift 2 ;;
            --baseline)      baseline="$2";      shift 2 ;;
            *) echo "bench_run: unknown flag '$1'" >&2; return 2 ;;
        esac
    done

    # -- Logging ------------------------------------------------------------
    # Suppress all log output so it does not skew timing measurements.
    export RUST_LOG=error

    # -- Build ---------------------------------------------------------------
    if [ -z "${PGDOG_BIN:-}" ]; then
        local root_dir
        root_dir="$(cd "${BENCH_DIR}/.." && pwd)"
        echo "Building pgdog (release)..."
        cargo build --release --manifest-path "${root_dir}/Cargo.toml"
        export PGDOG_BIN="${root_dir}/target/release/pgdog"
    fi
    echo "Binary: $(${PGDOG_BIN} --version 2>&1 || true)"
    echo ""

    # -- Paths ---------------------------------------------------------------
    local results_dir="${BENCH_DIR}/../target/pgdog_benches"
    mkdir -p "${results_dir}"

    local latest_file="${results_dir}/${name}.json"
    local prev_file prev_label

    if [ -n "${baseline}" ]; then
        # Named baseline: verify it exists before proceeding.
        prev_file="${results_dir}/${name}.${baseline}.json"
        prev_label="${baseline}"
        if [ ! -f "${prev_file}" ]; then
            echo "ERROR: baseline '${baseline}' not found (${prev_file})"
            exit 1
        fi
    else
        # Auto baseline: snapshot current latest before overwriting it.
        # Skip if the file is empty — an interrupted run leaves a zero-byte file.
        prev_file="${results_dir}/${name}.prev.json"
        prev_label="latest"
        [ -s "${latest_file}" ] && cp "${latest_file}" "${prev_file}"
    fi

    # -- Run ----------------------------------------------------------------
    local hyperfine_args=(
        --runs "${RUNS:-3}"
        --warmup "${WARMUP:-1}"
        --show-output
        --export-json "${latest_file}"
        --command-name "${name}"
    )
    if [ -n "${prepare_cmd}" ]; then
        hyperfine_args+=(--prepare "${prepare_cmd}")
    fi
    hyperfine "${hyperfine_args[@]}" "${cmd}"

    # -- Save named baseline ------------------------------------------------
    if [ -n "${save_baseline}" ]; then
        local named_file="${results_dir}/${name}.${save_baseline}.json"
        if [ -s "${latest_file}" ]; then
            cp "${latest_file}" "${named_file}"
            echo "Saved baseline '${save_baseline}' -> ${named_file}"
        else
            echo "WARNING: run did not complete cleanly, baseline '${save_baseline}' not saved."
        fi
    fi

    # -- Compare ------------------------------------------------------------
    echo ""
    if [ -s "${prev_file}" ]; then
        python3 "${BENCH_COMPARE}" "${prev_file}" "${latest_file}" \
            --prev-label "${prev_label}" --curr-label "current"
    else
        echo "No previous run found -- run again to see comparison."
        echo ""
    fi
}
