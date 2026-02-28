# DuckDB Obsidian

A DuckDB extension for querying an [Obsidian](https://obsidian.md) vault's Markdown notes directly from SQL.

```sql
SELECT basename, first_header, properties->'$.tags'
FROM obsidian_notes()
ORDER BY basename;
```

## Loading the Extension

### 1. Install DuckDB

Download DuckDB for your platform from [duckdb.org/docs/installation](https://duckdb.org/docs/installation/).

### 2. Download the extension

Download the `.duckdb_extension` file for your platform from the [GitHub Releases](https://github.com/puzan/duckdb-obsidian/releases) page. Choose the file matching your DuckDB version and OS/architecture.

### 3. Load the extension

Since the extension is not signed by DuckDB, launch DuckDB with unsigned extensions allowed:

```shell
duckdb -unsigned
```

Then load the extension by its local path:

```sql
LOAD '/path/to/obsidian.duckdb_extension';
```

The extension also auto-loads the built-in `json` extension, which is required for `properties` column operations.

### Setting the working directory

`obsidian_notes()` without arguments scans the current working directory. Launch DuckDB from your vault directory, or use the path argument:

```shell
cd /path/to/your/vault
duckdb -unsigned
```

```sql
LOAD '/path/to/obsidian.duckdb_extension';
SELECT basename, first_header FROM obsidian_notes() LIMIT 10;
```

## Table Function

```sql
obsidian_notes()                        -- scan vault at current working directory
obsidian_notes('/path/to/vault')        -- scan vault at the given path
```

The function requires the path to be a valid Obsidian vault (must contain a `.obsidian` directory). Hidden directories (`.obsidian`, `.git`, etc.) are skipped during file discovery. All `.md` files in all non-hidden subdirectories are included.

## Available Columns

| Column | Type | Description |
|--------|------|-------------|
| `filename` | `VARCHAR` | File name including the `.md` extension (e.g. `note.md`) |
| `basename` | `VARCHAR` | File name without the extension (e.g. `note`) |
| `filepath` | `VARCHAR` | Full absolute path to the file |
| `relative_path` | `VARCHAR` | Path relative to the vault root (e.g. `subdir/note.md`) |
| `first_header` | `VARCHAR` | Text of the first H1 heading; `NULL` if the note has no H1 |
| `headers` | `STRUCT(level INT, text VARCHAR)[]` | All headings in document order, each with their level (1–6) and text |
| `properties` | `JSON` | YAML frontmatter serialized as a JSON object; `NULL` if no frontmatter |
| `internal_links` | `STRUCT(target VARCHAR, display_name VARCHAR, header VARCHAR, block_ref VARCHAR)[]` | All `[[wiki-links]]` found in the note body and frontmatter |

### `internal_links` fields

| Field | Description |
|-------|-------------|
| `target` | The linked note name (e.g. `note_a` from `[[note_a]]`) |
| `display_name` | Alias text, or `NULL` (e.g. `My Alias` from `[[note_a\|My Alias]]`) |
| `header` | Linked heading, or `NULL` (e.g. `Section` from `[[note_a#Section]]`) |
| `block_ref` | Linked block ID, or `NULL` (e.g. `abc123` from `[[note_a^abc123]]`) |

## Query Examples

**List all notes with a computed title** (frontmatter title → first H1 → filename):

```sql
SELECT
    basename,
    COALESCE(
        json_extract_string(properties, '$.title'),
        first_header,
        basename
    ) AS title
FROM obsidian_notes()
ORDER BY title;
```

**Find notes by tag** (frontmatter `tags` array):

```sql
SELECT basename
FROM obsidian_notes()
WHERE json_contains(properties->'$.tags', '"projects"')
ORDER BY basename;
```

**Extract a specific frontmatter field**:

```sql
SELECT basename, json_extract_string(properties, '$.author') AS author
FROM obsidian_notes()
WHERE properties IS NOT NULL;
```

**Find notes containing an H2 heading**:

```sql
SELECT basename
FROM obsidian_notes()
WHERE list_contains(list_transform(headers, h -> h.level), 2)
ORDER BY basename;
```

**Find all notes linking to a specific note**:

```sql
SELECT basename
FROM obsidian_notes()
WHERE list_contains(list_transform(internal_links, l -> l.target), 'some_note')
ORDER BY basename;
```

**Build a link graph** (which note links to which):

```sql
SELECT
    basename AS source,
    unnest(list_transform(internal_links, l -> l.target)) AS target
FROM obsidian_notes()
WHERE len(internal_links) > 0
ORDER BY source, target;
```

**Find orphan notes** (not linked to by any other note):

```sql
WITH links AS (
    SELECT unnest(list_transform(internal_links, l -> l.target)) AS target
    FROM obsidian_notes()
)
SELECT basename
FROM obsidian_notes()
WHERE basename NOT IN (SELECT target FROM links)
ORDER BY basename;
```

## Development

### Prerequisites

**Install vcpkg** (used for cmark-gfm, ryml, and other dependencies):

```shell
git clone https://github.com/Microsoft/vcpkg.git ~/.vcpkg
~/.vcpkg/bootstrap-vcpkg.sh
```

**Install Ninja** (recommended for faster builds):

```shell
brew install ninja ccache   # macOS
```

**Set required environment variables** (add to your shell profile or prefix each command):

```shell
export GEN=ninja
export VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Building

```shell
make          # release build
make debug    # debug build
```

Output binaries:

- `./build/release/duckdb` — DuckDB shell with the extension preloaded
- `./build/release/test/unittest` — test runner
- `./build/release/extension/obsidian/obsidian.duckdb_extension` — loadable extension file

### Running Tests

Always run `make` before `make test` — they are separate steps.

```shell
make test        # run all SQL logic tests against release build
make test_debug  # run tests against debug build
```

Run a single test file:

```shell
./build/release/test/unittest --test-dir test/sql obsidian.test
```

### Other Commands

```shell
make format   # format C++ source files
make clean    # remove build artifacts
```
