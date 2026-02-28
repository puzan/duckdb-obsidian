#define DUCKDB_EXTENSION_MAIN

#include "obsidian_extension.hpp"
#include "obsidian_body.hpp"
#include "obsidian_frontmatter.hpp"
#include "obsidian_wikilinks.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <algorithm>
#include <mutex>

namespace duckdb {

//===--------------------------------------------------------------------===//
// obsidian_notes table function
//===--------------------------------------------------------------------===//

enum ColIdx : idx_t {
	COL_FILENAME = 0,
	COL_BASENAME,
	COL_FILEPATH,
	COL_RELATIVE_PATH,
	COL_FIRST_HEADER,
	COL_HEADERS,
	COL_PROPERTIES,
	COL_INTERNAL_LINKS,
	COL_COUNT,
};

struct ObsidianNotesScanData : public TableFunctionData {
	string vault_path;
	vector<string> files;
	// Cached once at bind time; avoids re-constructing the LogicalType per row.
	LogicalType heading_struct_type;
	LogicalType link_struct_type;
};

struct ObsidianNotesScanState : public GlobalTableFunctionState {
	std::mutex lock;
	idx_t position = 0;
	idx_t max_threads;
	// Projected column ids supplied by DuckDB when projection_pushdown = true.
	// output.data[i] corresponds to the original column column_ids[i].
	vector<column_t> column_ids;

	explicit ObsidianNotesScanState(idx_t max_threads_p) : max_threads(max_threads_p) {
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

// Per-thread local state. Work is distributed dynamically via the global position counter,
// so no per-thread cursor is needed here — the struct exists only to enable parallel execution.
struct ObsidianNotesLocalState : public LocalTableFunctionState {};

static void CollectMarkdownFiles(FileSystem &fs, const string &dir, vector<string> &files) {
	fs.ListFiles(dir, [&](const string &name, bool is_dir) {
		// Skip hidden directories (e.g. .obsidian, .git)
		if (!name.empty() && name[0] == '.') {
			return;
		}
		string full_path = fs.JoinPath(dir, name);
		if (is_dir) {
			CollectMarkdownFiles(fs, full_path, files);
		} else if (name.size() > 3 && name.compare(name.size() - 3, 3, ".md") == 0) {
			files.push_back(full_path);
		}
	});
}

// Read file contents into a string
static string ReadFileContents(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = fs.GetFileSize(*handle);
	string contents(file_size, '\0');
	fs.Read(*handle, &contents[0], file_size);
	return contents;
}

static LogicalType HeadingStructType() {
	child_list_t<LogicalType> fields;
	fields.emplace_back("level", LogicalType::INTEGER);
	fields.emplace_back("text", LogicalType::VARCHAR);
	return LogicalType::STRUCT(std::move(fields));
}

static Value HeadingToValue(const ParsedHeading &heading) {
	child_list_t<Value> fields;
	fields.emplace_back("level", Value::INTEGER(heading.level));
	fields.emplace_back("text", Value(heading.text));
	return Value::STRUCT(std::move(fields));
}

static LogicalType InternalLinkStructType() {
	child_list_t<LogicalType> fields;
	fields.emplace_back("target", LogicalType::VARCHAR);
	fields.emplace_back("display_name", LogicalType::VARCHAR);
	fields.emplace_back("header", LogicalType::VARCHAR);
	fields.emplace_back("block_ref", LogicalType::VARCHAR);
	return LogicalType::STRUCT(std::move(fields));
}

static Value InternalLinkToValue(const InternalLink &link) {
	child_list_t<Value> fields;
	fields.emplace_back("target", Value(link.target));
	fields.emplace_back("display_name",
	                    link.display_name.empty() ? Value(LogicalType::VARCHAR) : Value(link.display_name));
	fields.emplace_back("header", link.header.empty() ? Value(LogicalType::VARCHAR) : Value(link.header));
	fields.emplace_back("block_ref", link.block_ref.empty() ? Value(LogicalType::VARCHAR) : Value(link.block_ref));
	return Value::STRUCT(std::move(fields));
}

struct NotePathInfo {
	string filepath;
	string filename;
	string basename;
	string relative_path;
};

static NotePathInfo ComputeNotePathInfo(const string &raw_path, const string &vault_path) {
	NotePathInfo info;
	info.filepath = raw_path;
	std::replace(info.filepath.begin(), info.filepath.end(), '\\', '/');

	auto sep = info.filepath.find_last_of("/\\");
	info.filename = (sep == string::npos) ? info.filepath : info.filepath.substr(sep + 1);

	info.basename = info.filename.size() > 3 ? info.filename.substr(0, info.filename.size() - 3) : info.filename;

	info.relative_path = info.filepath;
	if (info.filepath.size() > vault_path.size() && info.filepath.compare(0, vault_path.size(), vault_path) == 0) {
		info.relative_path = info.filepath.substr(vault_path.size());
		if (!info.relative_path.empty() && (info.relative_path[0] == '/' || info.relative_path[0] == '\\')) {
			info.relative_path = info.relative_path.substr(1);
		}
	}
	return info;
}

static unique_ptr<FunctionData> ObsidianNotesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ObsidianNotesScanData>();
	auto &fs = FileSystem::GetFileSystem(context);

	if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
		result->vault_path = input.inputs[0].GetValue<string>();
	} else {
		result->vault_path = fs.GetWorkingDirectory();
	}

	if (!fs.DirectoryExists(result->vault_path)) {
		throw IOException("Vault path does not exist: " + result->vault_path);
	}

	string obsidian_config_dir = fs.JoinPath(result->vault_path, ".obsidian");
	if (!fs.DirectoryExists(obsidian_config_dir)) {
		throw IOException("Path is not an Obsidian vault (missing .obsidian directory): " + result->vault_path);
	}

	CollectMarkdownFiles(fs, result->vault_path, result->files);

	result->heading_struct_type = HeadingStructType();
	result->link_struct_type = InternalLinkStructType();

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("filename");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("basename");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("filepath");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("relative_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("first_header");
	return_types.emplace_back(LogicalType::LIST(result->heading_struct_type));
	names.emplace_back("headers");
	return_types.emplace_back(LogicalType::JSON());
	names.emplace_back("properties");
	return_types.emplace_back(LogicalType::LIST(result->link_struct_type));
	names.emplace_back("internal_links");

	return result;
}

static unique_ptr<GlobalTableFunctionState> ObsidianNotesInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ObsidianNotesScanData>();
	// Allow one thread per file; DuckDB caps this at the actual thread-pool size.
	idx_t max_threads = bind_data.files.empty() ? 1 : bind_data.files.size();
	auto state = make_uniq<ObsidianNotesScanState>(max_threads);
	state->column_ids = input.column_ids;
	return state;
}

static unique_ptr<LocalTableFunctionState> ObsidianNotesInitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
	return make_uniq<ObsidianNotesLocalState>();
}

static void ObsidianNotesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ObsidianNotesScanData>();
	auto &gstate = data_p.global_state->Cast<ObsidianNotesScanState>();
	auto &fs = FileSystem::GetFileSystem(context);

	// Atomically claim the next batch of file indices from the shared counter.
	// The lock is released before any I/O or parsing, so threads overlap on the
	// expensive work (file reads, YAML/cmark parsing) without serialising each other.
	idx_t batch_start, batch_end;
	{
		std::lock_guard<std::mutex> guard(gstate.lock);
		batch_start = gstate.position;
		batch_end = std::min(gstate.position + STANDARD_VECTOR_SIZE, (idx_t)bind_data.files.size());
		gstate.position = batch_end;
	}

	if (batch_start >= batch_end) {
		output.SetCardinality(0);
		return;
	}

	// Build a reverse map: original column index → position in output.data[].
	// output.data[i] corresponds to original column column_ids[i] (projection pushdown).
	idx_t col_to_out[COL_COUNT];
	std::fill(col_to_out, col_to_out + COL_COUNT, idx_t(-1));
	const auto &col_ids = gstate.column_ids;
	for (idx_t i = 0; i < col_ids.size(); i++) {
		if (col_ids[i] < COL_COUNT) {
			col_to_out[col_ids[i]] = i;
		}
	}

	// Determine which expensive operations are required based on projected columns.
	// Reading the file is only needed when at least one content column is projected.
	// Parsing the body (cmark) is only needed for first_header, headers, or internal_links.
	// Emitting properties JSON is only needed when the properties column is projected.
	const bool need_file_read = col_to_out[COL_FIRST_HEADER] != idx_t(-1) || col_to_out[COL_HEADERS] != idx_t(-1) ||
	                            col_to_out[COL_PROPERTIES] != idx_t(-1) || col_to_out[COL_INTERNAL_LINKS] != idx_t(-1);
	const bool need_body = col_to_out[COL_FIRST_HEADER] != idx_t(-1) || col_to_out[COL_HEADERS] != idx_t(-1) ||
	                       col_to_out[COL_INTERNAL_LINKS] != idx_t(-1);
	const bool need_emit_json = col_to_out[COL_PROPERTIES] != idx_t(-1);

	// Pre-fetch output data pointers for cheap VARCHAR columns (nullptr = not projected).
	auto *filename_out = col_to_out[COL_FILENAME] != idx_t(-1)
	                         ? FlatVector::GetData<string_t>(output.data[col_to_out[COL_FILENAME]])
	                         : nullptr;
	auto *basename_out = col_to_out[COL_BASENAME] != idx_t(-1)
	                         ? FlatVector::GetData<string_t>(output.data[col_to_out[COL_BASENAME]])
	                         : nullptr;
	auto *filepath_out = col_to_out[COL_FILEPATH] != idx_t(-1)
	                         ? FlatVector::GetData<string_t>(output.data[col_to_out[COL_FILEPATH]])
	                         : nullptr;
	auto *relpath_out = col_to_out[COL_RELATIVE_PATH] != idx_t(-1)
	                        ? FlatVector::GetData<string_t>(output.data[col_to_out[COL_RELATIVE_PATH]])
	                        : nullptr;
	auto *first_header_out = col_to_out[COL_FIRST_HEADER] != idx_t(-1)
	                             ? FlatVector::GetData<string_t>(output.data[col_to_out[COL_FIRST_HEADER]])
	                             : nullptr;
	auto *props_out = col_to_out[COL_PROPERTIES] != idx_t(-1)
	                      ? FlatVector::GetData<string_t>(output.data[col_to_out[COL_PROPERTIES]])
	                      : nullptr;

	idx_t count = 0;
	for (idx_t i = batch_start; i < batch_end; i++) {
		auto p = ComputeNotePathInfo(bind_data.files[i], bind_data.vault_path);

		// Write cheap VARCHAR columns directly — no file I/O needed.
		if (filename_out) {
			filename_out[count] = StringVector::AddString(output.data[col_to_out[COL_FILENAME]], p.filename);
		}
		if (basename_out) {
			basename_out[count] = StringVector::AddString(output.data[col_to_out[COL_BASENAME]], p.basename);
		}
		if (filepath_out) {
			filepath_out[count] = StringVector::AddString(output.data[col_to_out[COL_FILEPATH]], p.filepath);
		}
		if (relpath_out) {
			relpath_out[count] = StringVector::AddString(output.data[col_to_out[COL_RELATIVE_PATH]], p.relative_path);
		}

		// Expensive: file read + frontmatter/body parsing. Skipped entirely when
		// only path columns (filename, basename, filepath, relative_path) are projected.
		if (need_file_read) {
			string contents = ReadFileContents(fs, p.filepath);
			auto fm = ParseFrontmatter(contents);

			// Body parse (cmark) — skipped when only properties is projected.
			if (need_body) {
				auto body = ParseBody(contents, fm);

				// first_header
				if (first_header_out) {
					if (body.h1_heading.empty()) {
						FlatVector::Validity(output.data[col_to_out[COL_FIRST_HEADER]]).SetInvalid(count);
					} else {
						first_header_out[count] =
						    StringVector::AddString(output.data[col_to_out[COL_FIRST_HEADER]], body.h1_heading);
					}
				}

				// headers
				if (col_to_out[COL_HEADERS] != idx_t(-1)) {
					const LogicalType &hst = bind_data.heading_struct_type;
					vector<Value> hvs;
					hvs.reserve(body.headings.size());
					for (const auto &h : body.headings) {
						hvs.push_back(HeadingToValue(h));
					}
					output.data[col_to_out[COL_HEADERS]].SetValue(count, Value::LIST(hst, std::move(hvs)));
				}

				// internal_links
				if (col_to_out[COL_INTERNAL_LINKS] != idx_t(-1)) {
					const LogicalType &lst = bind_data.link_struct_type;
					vector<Value> lvs;
					lvs.reserve(body.links.size());
					for (const auto &link : body.links) {
						lvs.push_back(InternalLinkToValue(link));
					}
					output.data[col_to_out[COL_INTERNAL_LINKS]].SetValue(count, Value::LIST(lst, std::move(lvs)));
				}
			}

			// properties — only serialize YAML to JSON when projected.
			if (need_emit_json) {
				string props_json = FrontmatterToJson(fm);
				if (props_json.empty()) {
					FlatVector::Validity(output.data[col_to_out[COL_PROPERTIES]]).SetInvalid(count);
				} else {
					props_out[count] = StringVector::AddString(output.data[col_to_out[COL_PROPERTIES]], props_json);
				}
			}
		}

		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<NodeStatistics> ObsidianNotesCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = bind_data->Cast<ObsidianNotesScanData>();
	idx_t n = data.files.size();
	return make_uniq<NodeStatistics>(n, n);
}

static void ConfigureObsidianNotesFunction(TableFunction &fn) {
	fn.projection_pushdown = true;
	fn.cardinality = ObsidianNotesCardinality;
}

static void LoadInternal(ExtensionLoader &loader) {
	ExtensionHelper::TryAutoLoadExtension(loader.GetDatabaseInstance(), "json");

	// Register obsidian_notes table function with two overloads:
	// obsidian_notes()            — uses current working directory
	// obsidian_notes(vault_path)  — uses the provided path
	TableFunctionSet obsidian_notes_set("obsidian_notes");

	TableFunction no_args("obsidian_notes", {}, ObsidianNotesFunction, ObsidianNotesBind, ObsidianNotesInitGlobal,
	                      ObsidianNotesInitLocal);
	ConfigureObsidianNotesFunction(no_args);
	obsidian_notes_set.AddFunction(no_args);

	TableFunction with_path("obsidian_notes", {LogicalType::VARCHAR}, ObsidianNotesFunction, ObsidianNotesBind,
	                        ObsidianNotesInitGlobal, ObsidianNotesInitLocal);
	ConfigureObsidianNotesFunction(with_path);
	obsidian_notes_set.AddFunction(with_path);

	loader.RegisterFunction(obsidian_notes_set);
}

void ObsidianExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string ObsidianExtension::Name() {
	return "obsidian";
}

std::string ObsidianExtension::Version() const {
#ifdef EXT_VERSION_OBSIDIAN
	return EXT_VERSION_OBSIDIAN;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(obsidian, loader) {
	duckdb::LoadInternal(loader);
}
}
