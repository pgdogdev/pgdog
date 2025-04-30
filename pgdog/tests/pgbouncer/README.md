# PgDog vs. PgBouncer

Basic benchmark using Ruby/ActiveRecord. This makes it a bit more like
what will happen in the real world (as opposed to slamming it with pgbench).

## Requirements

1. Datadog agent running locally. Metrics are sent to the agent for easier visualization.
2. Ruby/Bundler
3. PgBouncer

## Running

### Install dependencies

```bash
bundle install
```

### Start PgBouncer

```bash
pgbouncer pgbouncer.ini
```

### Start PgDog

```bash
bash run.sh
```

### Run the benchmark

```bash
bundle exec rspec benchmark_spec.rb
```

And watch metrics flow into Datadog. Metrics will be recorded as:

```
benchmark.pgdog
benchmark.pgbouncer
```

Add a namespace to your agent or in `benchmark_spec.rb` if using it a company/shared Datadog account.

#### Why use Datadog?

It has nice graphs and easy to use.


### Results

On my M1, PgDog is currently 6% slower:

![benchmark_1](benchmark_1.png)

We'll get there.
