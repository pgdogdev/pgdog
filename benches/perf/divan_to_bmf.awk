#!/usr/bin/awk -f
# Standardize Divan bench output to Bencher Metric Format

# I wish there was a better way to do this madness...
# There was a rejected PR to Divan for file output: https://github.com/nvzqz/divan/pull/42

function trim(s) {
    sub(/^[ \t]+/, "", s)
    sub(/[ \t]+$/, "", s)
    return s
}

function split_unit(v, out) {
    out[1] = v
    out[2] = v
    sub(/ .*/, "", out[1])
    sub(/^[^ ]* ?/, "", out[2])
}

function to_ns(v,   p) {
    split_unit(v, p)
    if (p[2] == "ps") return p[1] / 1e3
    if (p[2] == "ns") return p[1]
    if (p[2] == "µs") return p[1] * 1e3
    if (p[2] == "ms") return p[1] * 1e6
    if (p[2] == "s")  return p[1] * 1e9
    if (p[2] == "m")  return p[1] * 60e9
    if (p[2] == "h")  return p[1] * 3600e9
    if (p[2] == "d")  return p[1] * 86400e9
    return p[1]
}

function to_bytes(v,   p, i) {
    split_unit(v, p)
    for (i = 1; i <= 6; i++) {
        if (p[2] == DEC[i]) return p[1] * 1000 ^ (i - 1)
        if (p[2] == BIN[i]) return p[1] * 1024 ^ (i - 1)
    }
    return p[1]
}

function emit(name, measure, value, lower, upper) {
    if (!(name in metrics)) {
        order[++count] = name
        metrics[name] = ""
    } else {
        metrics[name] = metrics[name] ","
    }
    metrics[name] = metrics[name] sprintf("\"%s\":{\"value\":%.3f", measure, value)
    if (lower != "") {
        metrics[name] = metrics[name] sprintf(",\"lower_value\":%.3f,\"upper_value\":%.3f", lower, upper)
    }
    metrics[name] = metrics[name] "}"
}

function path(depth,   i, s) {
    s = ""
    for (i = 0; i < depth; i++) s = s parents[i] "::"
    return s
}

BEGIN {
    split("B KB MB GB TB PB", DEC, " ")
    split("B KiB MiB GiB TiB PiB", BIN, " ")
    section = ""
}

{
    gsub("├─", "+-")
    gsub("╰─", "+-")
    gsub("│", "|")

    depth = -1
    branch = index($0, "+-")
    if (branch > 0) depth = (branch - 1) / 3

    line = $0
    sub(/^[ |]+/, "", line)
    n = split(line, col, "|")
    for (i = 1; i <= n; i++) col[i] = trim(col[i])
}

depth >= 0 && col[1] ~ /^\+- [^ ]+ +[0-9.]+ [^ ]+$/ {
    split(substr(col[1], 4), parts, / +/)
    leaf = path(depth) parts[1]
    fastest = parts[2] " " parts[3]
    emit(leaf, "latency", to_ns(col[4]), to_ns(fastest), to_ns(col[2]))
    section = ""
    next
}

depth >= 0 && col[1] ~ /^\+- [^ ]+$/ {
    parents[depth] = substr(col[1], 4)
    section = ""
    next
}

depth >= 0 {
    section = ""
    next
}

col[1] ~ /^(max alloc|alloc|dealloc|grow|shrink):$/ {
    section = substr(col[1], 1, length(col[1]) - 1)
    gsub(/ /, "_", section)
    row = 0
    next
}

section != "" && col[1] ~ /^[0-9]/ {
    row++
    if (row == 1) emit(leaf, section "_count", col[4], "", "")
    if (row == 2) emit(leaf, section "_bytes", to_bytes(col[4]), "", "")
    next
}

END {
    printf "{"
    for (i = 1; i <= count; i++) {
        printf "%s\"%s\":{%s}", (i > 1 ? "," : ""), order[i], metrics[order[i]]
    }
    print "}"
}
