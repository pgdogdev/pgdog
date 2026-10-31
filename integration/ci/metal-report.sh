#!/bin/bash
# Waits for the metal runners' results in S3 and posts them as commit statuses and PR comments.
# TODO(jkaczman): get rid of this with PAT with necessary perms
set -euo pipefail

readonly X86_WAIT_SECONDS=10800

lane_context() { [ "$1" = x86 ] && echo metal/bench-x86 || echo metal/bench; }
lane_prefix() { [ "$1" = x86 ] && echo results-x86 || echo results; }
lane_marker() { [ "$1" = x86 ] && echo "<!-- metal-bench-x86 -->" || echo "<!-- metal-bench -->"; }

set_status() {
    local lane=$1 state=$2 description=$3

    gh api "repos/$GITHUB_REPOSITORY/statuses/$SHA" \
        -f context="$(lane_context "$lane")" \
        -f state="$state" \
        -f description="$description" \
        -f target_url="https://s3.console.aws.amazon.com/s3/buckets/$BUCKET?prefix=$(lane_prefix "$lane")/$REF/$SHA/" > /dev/null
}

lane_state() {
    local key modified
    key="$(lane_prefix "$1")/$REF/$SHA/state"
    modified=$(aws s3api head-object --bucket "$BUCKET" --key "$key" --query LastModified --output text 2> /dev/null) || return 0
    [ "$(date -d "$modified" +%s)" -ge "$ENQUEUED_AT" ] || return 0
    aws s3 cp "s3://$BUCKET/$key" -
}

wait_for_lane() {
    local lane=$1 deadline=${2:-} state="" last=""

    until [ "$state" = done ] || [ "$state" = skipped ]; do
        if [ -n "$deadline" ] && [ "$(date +%s)" -ge "$deadline" ]; then
            echo timeout
            return
        fi

        sleep 30
        state=$(lane_state "$lane")

        if [ -n "$state" ] && [ "$state" != "$last" ]; then
            set_status "$lane" pending "$state"
            last=$state
        fi
    done

    echo "$state"
}

post_comment() {
    local marker=$1 file=$2 existing
    existing=$(gh api "repos/$GITHUB_REPOSITORY/issues/$PR/comments?per_page=100" \
        --jq "map(select(.body | startswith(\"$marker\")))[0].id")

    if [ -n "$existing" ]; then
        gh api -X PATCH "repos/$GITHUB_REPOSITORY/issues/comments/$existing" -F body=@"$file" > /dev/null
    else
        gh api "repos/$GITHUB_REPOSITORY/issues/$PR/comments" -F body=@"$file" > /dev/null
    fi
}

report_lane() {
    local lane=$1 state=$2 results

    case "$state" in
        skipped)
            set_status "$lane" pending "Skipped, superseded by a newer push"
            return
            ;;
        timeout)
            set_status "$lane" error "No result within $((X86_WAIT_SECONDS / 60)) minutes of arm64"
            return
            ;;
    esac

    results="s3://$BUCKET/$(lane_prefix "$lane")/$REF/$SHA"
    aws s3 cp "$results/verdict.json" "$lane.verdict.json"
    aws s3 cp "$results/comment.md" "$lane.md"

    if [ "$(jq -r .passed "$lane.verdict.json")" = true ]; then
        set_status "$lane" success "$(jq -r .summary "$lane.verdict.json")"
    else
        set_status "$lane" failure "$(jq -r .summary "$lane.verdict.json")"
    fi

    if [ -n "$PR" ]; then
        post_comment "$(lane_marker "$lane")" "$lane.md"
    fi
}

main() {
    report_lane arm64 "$(wait_for_lane arm64)"
    report_lane x86 "$(wait_for_lane x86 $(($(date +%s) + X86_WAIT_SECONDS)))"
}

main
