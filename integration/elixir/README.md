# Elixir / Postgrex integration suite

Exercises [Postgrex](https://github.com/elixir-ecto/postgrex), the driver
underneath Ecto and Phoenix, against PgDog. Postgrex leans on the extended
protocol harder than most drivers — it prepares statements by name, asks for
binary result formats, and closes statements as soon as it is done with them —
so it reaches code paths the other suites don't.

That last habit is what produced
[#1066](https://github.com/pgdogdev/pgdog/issues/1066): PgDog answered a `SET`
itself, before checking out a backend, and its fake response was missing the
`CloseComplete` Postgrex was waiting for. `test/set_test.exs` pins that down.

## Running

```sh
bash integration/elixir/run.sh    # starts PgDog, runs the suite, stops PgDog
bash integration/elixir/dev.sh    # against an already-running PgDog
```

Both need `elixir` and `mix` on `PATH`. CI installs them with
[erlef/setup-beam](https://github.com/erlef/setup-beam); locally use asdf, mise,
Homebrew, or whatever you already have.

## Known bugs

Tests tagged `@tag :known_bug` reproduce open PgDog bugs and are excluded by
default so CI stays green. Run them with:

```sh
cd integration/elixir && mix test --include known_bug
```

Each carries a comment describing what it pins down. Delete the tag when the
underlying bug is fixed.
