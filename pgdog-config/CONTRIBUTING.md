# Contributing to pgdog-config

## Version compatibility

The changes to the config types should backward compatible in order to provide smooth upgrade mechanism and the integration with control plane services.

### Rules for fields

- Give every new field `#[serde(default)]` or `#[serde(default = "...")]`.
- Do not change the type of an existing field. Add a new field with another name and provide the upgrade window for the existing field.
- Do not rename a field without `#[serde(alias = "old_name")]`.
- You can remove a field. Peers accept unknown keys, and parsing a config file logs them as warnings.
- Do not add a new value to an existing enum without a plan for older peers. An unknown enum value fails the parse.

### Unknown keys

- Do not use `#[serde(deny_unknown_fields)]`. Use `#[schemars(deny_unknown_fields)]`, so the JSON schema stays strict and parsing stays lenient. Except for the untagged enums since they use it to select the correct variant.
- `parse_toml` and `parse_toml_strict` in `core.rs` find unknown keys and logs them.

### Structs with `#[serde(flatten)]`

`parse_toml_strict` cannot see unknown keys in a struct that has a `#[serde(flatten)]` field. For such a struct:

1. Add this field as the last field:

   ```rust
   /// Keys that no field of this struct recognized.
   #[serde(flatten)]
   #[schemars(skip)]
   pub unknown: UnknownFields,
   ```

2. Implement `WithUnknownFields` for the struct and return `self.unknown.unknown_fields()`.
3. In the `WithUnknownFields` implementation of the parent, add the path prefix, for example `databases.{index}.{field}`.
4. Add a test that puts an unknown key in the new section and checks the reported path.

Do not add the catch-all field to a struct that is itself flattened into a parent, for example `ServerTls` or `RoleConfig`. The parent catches those keys.

## Doc comments

This crate defines all configuration types for PgDog. Every `pub` struct, enum, and field
**must** carry a `///` doc comment.

Doc comments serve two purposes simultaneously:

1. **Rustdoc** — rendered as API documentation via `cargo doc`
2. **JSON Schema** — `schemars` reads `///` comments and emits them as the `description` field in the generated schema, which surfaces in editor autocompletion, schema validators, and any tooling that consumes the schema

Because the same text reaches both audiences, comments must be accurate, self-contained, and kept in sync with the official documentation at [docs.pgdog.dev](https://docs.pgdog.dev).

---

### Fields

```rust
/// Short description of what this field controls.
///
/// **Note:** Any important caveat or warning.
///
/// _Default:_ `value`
///
/// <https://docs.pgdog.dev/configuration/pgdog.toml/{page}/#{anchor}>

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
/// <https://docs.pgdog.dev/configuration/pgdog.toml/{page}/>

pub struct Foo { ... }
```

### Enums

```rust
/// Noun phrase describing what the enum represents.
///
/// <https://docs.pgdog.dev/configuration/pgdog.toml/{page}/#{anchor}>

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

