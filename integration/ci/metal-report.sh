#!/bin/bash
# Waits for the metal runner's result in S3 and posts it as a commit status and a PR comment.
# TODO(jkaczman): get rid of this with PAT with necessary perms
set -euo pipefail

readonly RESULTS="s3://$BUCKET/results/$REF/$SHA"
readonly COMMENT_MARKER="<!-- metal-bench -->"

set_status() {
    local state=$1 description=$2

    gh api "repos/$GITHUB_REPOSITORY/statuses/$SHA" \
        -f context=metal/bench \
        -f state="$state" \
        -f description="$description" \
        -f target_url="https://s3.console.aws.amazon.com/s3/buckets/$BUCKET?prefix=results/$REF/$SHA/" > /dev/null
}

current_state() {
    local modified
    modified=$(aws s3api head-object --bucket "$BUCKET" --key "results/$REF/$SHA/state" --query LastModified --output text 2> /dev/null) || return 0
    [ "$(date -d "$modified" +%s)" -ge "$ENQUEUED_AT" ] || return 0
    aws s3 cp "$RESULTS/state" -
}

wait_for_runner() {
    local state="" last=""

    until [ "$state" = done ] || [ "$state" = skipped ]; do
        sleep 30
        state=$(current_state)

        if [ -n "$state" ] && [ "$state" != "$last" ]; then
            set_status pending "$state"
            last=$state
        fi
    done

    echo "$state"
}

post_comment() {
    local existing
    existing=$(gh api "repos/$GITHUB_REPOSITORY/issues/$PR/comments?per_page=100" \
        --jq "map(select(.body | startswith(\"$COMMENT_MARKER\")))[0].id")

    if [ -n "$existing" ]; then
        gh api -X PATCH "repos/$GITHUB_REPOSITORY/issues/comments/$existing" -F body=@comment.md > /dev/null
    else
        gh api "repos/$GITHUB_REPOSITORY/issues/$PR/comments" -F body=@comment.md > /dev/null
    fi
}

main() {
    if [ "$(wait_for_runner)" = skipped ]; then
        set_status pending "Skipped, superseded by a newer push"
        return
    fi

    aws s3 cp "$RESULTS/verdict.json" verdict.json
    aws s3 cp "$RESULTS/comment.md" comment.md

    if [ "$(jq -r .passed verdict.json)" = true ]; then
        set_status success "$(jq -r .summary verdict.json)"
    else
        set_status failure "$(jq -r .summary verdict.json)"
    fi

    if [ -n "$PR" ]; then
        post_comment
    fi
}

main
