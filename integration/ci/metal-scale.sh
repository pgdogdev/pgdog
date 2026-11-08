#!/bin/bash
set -euo pipefail

queue_url=$1
spot_group=$2
on_demand_group=$spot_group-od
regions=(us-east-1 us-east-2 us-west-2)

groups_in_launch_order=()
for region in "${regions[@]}"; do groups_in_launch_order+=("$region/$spot_group"); done
for region in "${regions[@]}"; do groups_in_launch_order+=("$region/$on_demand_group"); done

desired_capacity() {
    local region=${1%/*} group=${1#*/}
    aws autoscaling describe-auto-scaling-groups --region "$region" --auto-scaling-group-names "$group" \
        --query 'AutoScalingGroups[0].DesiredCapacity' --output text 2> /dev/null || echo unknown
}

set_desired_capacity() {
    local region=${1%/*} group=${1#*/} capacity=$2
    aws autoscaling set-desired-capacity --region "$region" --auto-scaling-group-name "$group" --desired-capacity "$capacity"
}

wait_for_launch() {
    local region=${1%/*} group=${1#*/} started_at=$2 status
    for _ in $(seq 18); do
        status=$(aws autoscaling describe-scaling-activities --region "$region" --auto-scaling-group-name "$group" --max-items 1 \
            --query "Activities[?StartTime>='$started_at'] | [0].StatusCode" --output text 2> /dev/null || echo None)
        case $status in
            Successful | InProgress | PreInService | MidLifecycleAction) return 0 ;;
            Failed | Cancelled) return 1 ;;
        esac
        sleep 10
    done
    return 1
}

jobs_in_queue() {
    aws sqs get-queue-attributes --queue-url "$queue_url" \
        --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
        --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' --output text \
        | awk '{print $1 + $2}'
}

start_the_previous_default_groups() {
    local second_region=us-east-2
    [ "$spot_group" = pgdog-metal-bench ] || second_region=us-west-2
    set_desired_capacity "us-east-1/$spot_group" 1
    set_desired_capacity "$second_region/$spot_group" 1
}

if [ "$(desired_capacity "us-east-1/$spot_group")" = unknown ]; then
    echo "can't read the autoscaling groups, starting the previous default groups"
    start_the_previous_default_groups
    exit 0
fi

jobs=$(jobs_in_queue)
boxes_up=0
for group in "${groups_in_launch_order[@]}"; do
    [ "$(desired_capacity "$group")" = 1 ] && boxes_up=$((boxes_up + 1))
done
echo "$spot_group: $jobs jobs queued or running, $boxes_up boxes up"

for group in "${groups_in_launch_order[@]}"; do
    [ "$boxes_up" -lt "$jobs" ] || break
    [ "$(desired_capacity "$group")" = 0 ] || continue

    started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    set_desired_capacity "$group" 1
    if wait_for_launch "$group" "$started_at"; then
        echo "started $group"
        boxes_up=$((boxes_up + 1))
    else
        echo "no capacity for $group, trying the next group"
        set_desired_capacity "$group" 0
    fi
done
