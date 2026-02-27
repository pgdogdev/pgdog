# Contributing to pgdog-config

This crate defines all configuration types for PgDog. Every `pub` struct, enum, and field
**must** carry a `///` doc comment.

Doc comments serve two purposes simultaneously:

1. **Rustdoc** — rendered as API documentation via `cargo doc`
2. **JSON Schema** — `schemars` reads `///` comments and emits them as the `description` field in the generated schema, which surfaces in editor autocompletion, schema validators, and any tooling that consumes the schema

Because the same text reaches both audiences, comments must be accurate, self-contained, and kept in sync with the official documentation at [docs.pgdog.dev](https://docs.pgdog.dev).

---

## Doc comment format

### Fields

```rust
/// Short description of what this field controls.
///
/// **Note:** Any important caveat or warning.
///
/// _Default:_ `value`
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/{page}/#{anchor}
pub field_name: Type,
```

- The description must match the intent described at docs.pgdog.dev — keep it in sync when the docs change
- Include a `**Note:**` paragraph for any warnings or caveats (e.g. "requires restart", "enterprise only", "do not use in production")
- Include `_Default:_ \`value\`` only when the field has a meaningful default
- End with the URL of the relevant documentation page and anchor; omit the URL only for internal fields that have no corresponding docs page

### Structs

```rust
/// What this configuration section controls.
///
/// **Note:** Any important caveat, if present.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/{page}/
pub struct Foo { ... }
```

### Enums

```rust
/// Noun phrase describing what the enum represents.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/{page}/#{anchor}
pub enum Bar { ... }
```

### Enum variants

```rust
/// One line: what this variant means or does.
VariantName,
```

No URL on variants — the enum-level URL covers them.

---

## Style rules

- Include inline markdown links where they add context:
  `[two-phase commit](https://docs.pgdog.dev/features/sharding/2pc/)`
- `_Default:_ \`value\`` — italic label, backtick value
- `**Note:**` — bold label, no blockquote `>`
- No trailing punctuation on titles; no leading articles; lowercase noun titles
- Do not restate what the type already expresses (e.g. no "a boolean that enables…")

---

## Keeping docs in sync

When you change a field's behaviour or add a new one:

1. Check the corresponding page on [docs.pgdog.dev](https://docs.pgdog.dev)
2. Update the `///` comment to reflect the current behaviour
3. Update the URL anchor if the heading changed

When the docs site is updated independently, the comment should be updated to match.
