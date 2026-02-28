# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension named "obsidian", built from the DuckDB extension template. It provides a table function for querying an Obsidian vault's Markdown files from DuckDB.

## Build Commands

Requires VCPKG installed. All `make` commands **must** be prefixed with these environment variables:

```bash
export GEN=ninja
export VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake
```

**Important:** `make` and `make test` are separate commands — always run `make` first to build, then `make test` to test.

```bash
GEN=ninja VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake make             # Build release binary
GEN=ninja VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake make test        # Run all SQL logic tests
GEN=ninja VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake make test_debug  # Run tests against debug build
GEN=ninja VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake make format      # Format code
GEN=ninja VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake make clean       # Clean build artifacts
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

- **`src/obsidian_extension.cpp`** — Extension entry point. Registers the `obsidian_notes` table function. DuckDB-specific scan logic: bind, init, scan callbacks.
- **`src/obsidian_frontmatter.cpp`** — YAML frontmatter parsing (`ParseFrontmatter`) and JSON serialization (`FrontmatterToJson`). Owns all ryml dependency.
- **`src/obsidian_wikilinks.cpp`** — Wiki-link extraction (`ExtractWikiLinks`). Pure C++, no DuckDB or third-party deps.
- **`src/obsidian_body.cpp`** — Markdown body parsing via cmark-gfm (`ParseBody`). Extracts headings and wiki-links.
- **`src/include/obsidian_extension.hpp`** — Declares `ObsidianExtension` (inherits `Extension`), exposing `Load()`, `Name()`, `Version()`.
- **`src/include/obsidian_frontmatter.hpp`** — Declares `ParsedFrontmatter`, `ParseFrontmatter`, `FrontmatterToJson`.
- **`src/include/obsidian_wikilinks.hpp`** — Declares `InternalLink`, `ExtractWikiLinks`.
- **`src/include/obsidian_body.hpp`** — Declares `ParsedHeading`, `ParsedBody`, `ParseBody`.
- **`extension_config.cmake`** — Controls which extensions are loaded and which test directories are included.
- **`CMakeLists.txt`** — Configures the extension build.
- **`vcpkg.json`** — Declares VCPKG dependencies. Add new dependencies here.
- **`duckdb/`** — DuckDB git submodule (v1.4.4). Do not modify directly.
- **`extension-ci-tools/`** — DuckDB CI tooling submodule. Provides `makefiles/duckdb_extension.Makefile` which the root `Makefile` includes.

## Tests

Tests use DuckDB's SQLLogicTest format in `test/sql/`. Each `.test` file uses `require obsidian` to load the extension before running queries. The `statement ok`, `query`, and `----` directives define expected output.

## LSP / Tooling

`.clangd` points the Clang LSP to `build/release` for the compilation database. Run a build before using IDE features.

## Updating DuckDB Version

See `docs/UPDATING.md` for the process of bumping the DuckDB submodule and CI tool versions together.
