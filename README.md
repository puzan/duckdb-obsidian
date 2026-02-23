# DuckDB Obsidian

A DuckDB extension for querying an [Obsidian](https://obsidian.md) vault's Markdown notes directly from SQL.

```sql
SELECT title, relative_path FROM obsidian_notes('/path/to/vault') ORDER BY title;
```

## Building

### Prerequisites

#### Install vcpkg

The extension uses vcpkg for dependency management (cmark-gfm, ryml, etc).

```shell
git clone https://github.com/Microsoft/vcpkg.git ~/.vcpkg
~/.vcpkg/bootstrap-vcpkg.sh
```

#### Set required environment variables

```shell
export VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake
export GEN=ninja   # recommended for faster builds
```

> `GEN=ninja` requires Ninja: `brew install ninja ccache`

### Build steps

```shell
make          # release build
make debug    # debug build (for debugging and lldb)
```

Output binaries:
- `./build/release/duckdb` — DuckDB shell with extension preloaded
- `./build/release/test/unittest` — test runner
- `./build/release/extension/obsidian/obsidian.duckdb_extension` — loadable extension

## Running

```shell
./build/release/duckdb
```

```sql
SELECT filename, relative_path, title
FROM obsidian_notes('/path/to/your/vault')
ORDER BY title;
```

## Running the tests

```shell
make test        # run all SQL logic tests (release build)
make test_debug  # run tests against debug build
```

Run a single test:

```shell
./build/release/test/unittest --test-dir test/sql obsidian.test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:

```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```

Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:

```sql
INSTALL obsidian;
LOAD obsidian;
```
