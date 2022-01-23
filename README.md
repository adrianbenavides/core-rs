[![tests](https://github.com/adrianbenavides/core-rs/workflows/tests/badge.svg)](https://github.com/adrianbenavides/core-rs/actions)
[![audit](https://github.com/adrianbenavides/core-rs/workflows/audit/badge.svg)](https://github.com/adrianbenavides/core-rs/actions)

`core-rs` includes some components of gazette's [`core`][core] written in Rust.

[core]: https://github.com/gazette/core/

## How to build

### How to generate proto code

1. Install golang
2. Download the following go modules:

```shell
go install go.gazette.dev/core@latest github.com/gogo/protobuf@latest
```

3. Build the `protocol` crate

```shell
cargo build -p protocol
```
