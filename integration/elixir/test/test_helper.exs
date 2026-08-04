# Tests tagged :known_bug reproduce open PgDog bugs. They are excluded so CI
# stays green; run them with `mix test --include known_bug` when working on a
# fix. Each one carries a comment describing the bug it pins down.
ExUnit.start(exclude: [:known_bug])
