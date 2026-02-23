# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension named "obsidian", built from the DuckDB extension template. It demonstrates C++ extension development with external dependency integration (OpenSSL via VCPKG). The extension implements vectorized scalar functions registered into DuckDB's function catalog.

## Build Commands

Requires VCPKG installed and `VCPKG_TOOLCHAIN_PATH` set before building.

```bash
make                  # Build release binary
make test             # Run all SQL logic tests
make test_debug       # Run tests against debug build
make format           # Format code
make clean            # Clean build artifacts
```

**Output binaries:**
- `./build/release/duckdb` — DuckDB shell with extension preloaded
- `./build/release/test/unittest` — Test runner binary
- `./build/release/extension/obsidian/obsidian.duckdb_extension` — Loadable extension

## Running a Single Test

```bash
./build/release/test/unittest --test-dir test/sql obsidian.test
```

## Architecture

- **`src/obsidian_extension.cpp`** — Extension entry point. Implements scalar functions using DuckDB's `UnaryExecutor` and registers them via `CreateScalarFunctionInfo`. All new functions go here.
- **`src/include/obsidian_extension.hpp`** — Declares `ObsidianExtension` (inherits `Extension`), exposing `Load()`, `Name()`, `Version()`.
- **`extension_config.cmake`** — Controls which extensions are loaded and which test directories are included.
- **`CMakeLists.txt`** — Configures the extension build, links OpenSSL.
- **`vcpkg.json`** — Declares VCPKG dependencies (currently OpenSSL).
- **`duckdb/`** — DuckDB git submodule (v1.4.4). Do not modify directly.
- **`extension-ci-tools/`** — DuckDB CI tooling submodule. Provides `makefiles/duckdb_extension.Makefile` which the root `Makefile` includes.

## Tests

Tests use DuckDB's SQLLogicTest format in `test/sql/`. Each `.test` file uses `require obsidian` to load the extension before running queries. The `statement ok`, `query`, and `----` directives define expected output.

## LSP / Tooling

`.clangd` points the Clang LSP to `build/release` for the compilation database. Run a build before using IDE features.

## Updating DuckDB Version

See `docs/UPDATING.md` for the process of bumping the DuckDB submodule and CI tool versions together.
