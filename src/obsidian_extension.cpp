#define DUCKDB_EXTENSION_MAIN

#include "obsidian_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <cmark-gfm.h>
#include <ryml/ryml.hpp>
#include <ryml/ryml_std.hpp>

#include <algorithm>
#include <stdexcept>
#include <mutex>

namespace duckdb {

// ryml calls this instead of abort() when it encounters a parse error.
// We throw so that our try/catch can handle malformed YAML gracefully.
static void RymlErrorCallback(const char *msg, size_t msg_len, ryml::Location /*loc*/, void * /*userdata*/) {
	throw std::runtime_error(std::string(msg, msg_len));
}

//===--------------------------------------------------------------------===//
// obsidian_notes table function
//===--------------------------------------------------------------------===//

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

struct ParsedFrontmatter {
	string yaml_block; // owns the buffer that ryml::Tree points into
	ryml::Tree tree;
	size_t body_offset = 0; // byte offset in the original file where body starts
};

// Parse YAML frontmatter block. Returns nullptr if absent or malformed.
static unique_ptr<ParsedFrontmatter> ParseFrontmatter(const string &s) {
	if (s.size() < 3 || s.compare(0, 3, "---") != 0) {
		return nullptr;
	}
	size_t end = s.find("\n---", 3);
	if (end == string::npos) {
		return nullptr;
	}

	auto result = make_uniq<ParsedFrontmatter>();
	result->yaml_block = s.substr(3, end - 3);
	if (!result->yaml_block.empty() && result->yaml_block[0] == '\n') {
		result->yaml_block = result->yaml_block.substr(1);
	}

	// body starts after the closing "---\n"
	result->body_offset = end + 4;
	if (result->body_offset < s.size() && s[result->body_offset] == '\n') {
		result->body_offset++;
	}

	try {
		ryml::Callbacks callbacks = ryml::get_callbacks();
		callbacks.m_error = RymlErrorCallback;
		result->tree = ryml::Tree(callbacks);
		ryml::EventHandlerTree evth(callbacks);
		ryml::Parser parser(&evth);
		ryml::parse_in_place(&parser, ryml::to_substr(result->yaml_block), &result->tree);

		if (!result->tree.rootref().is_map()) {
			return nullptr;
		}
	} catch (...) {
		return nullptr;
	}

	return result;
}

struct InternalLink {
	string target;
	string display_name; // empty = absent
	string header;       // empty = absent
	string block_ref;    // empty = absent
};

// Extract [[wiki-links]] from a plain text string and append to links vector.
// Hand-rolled parser — avoids std::regex heap allocations on every NFA step.
static void ExtractWikiLinks(const string &text, vector<InternalLink> &links) {
	const char *s = text.c_str();
	const size_t len = text.size();
	size_t i = 0;

	while (i + 1 < len) {
		// Scan for '[['
		if (s[i] != '[' || s[i + 1] != '[') {
			i++;
			continue;
		}

		// Find matching ']]', reject nested brackets
		size_t start = i + 2;
		size_t j = start;
		bool found = false;
		while (j + 1 < len) {
			if (s[j] == ']' && s[j + 1] == ']') {
				found = true;
				break;
			}
			if (s[j] == '[' || s[j] == ']') {
				break;
			}
			j++;
		}

		if (!found || j == start) {
			i = j + 1;
			continue;
		}

		// inner = s[start..j)
		const char *inner = s + start;
		size_t inner_len = j - start;

		InternalLink link;

		// Split on '|' → display_name
		const char *pipe = static_cast<const char *>(memchr(inner, '|', inner_len));
		const char *target_start;
		size_t target_len;
		if (pipe) {
			link.display_name.assign(pipe + 1, inner + inner_len - (pipe + 1));
			target_start = inner;
			target_len = pipe - inner;
		} else {
			target_start = inner;
			target_len = inner_len;
		}

		// Split target on '#' → header / block_ref
		const char *hash = static_cast<const char *>(memchr(target_start, '#', target_len));
		if (hash) {
			link.target.assign(target_start, hash - target_start);
			const char *anchor = hash + 1;
			size_t anchor_len = target_start + target_len - anchor;
			if (anchor_len > 0 && anchor[0] == '^') {
				link.block_ref.assign(anchor + 1, anchor_len - 1);
			} else {
				link.header.assign(anchor, anchor_len);
			}
		} else {
			link.target.assign(target_start, target_len);
		}

		links.push_back(std::move(link));
		i = j + 2;
	}
}

struct ParsedHeading {
	int level;
	string text;
};

struct ParsedBody {
	string h1_heading;
	vector<ParsedHeading> headings;
	vector<InternalLink> links;
};

// Parse the document body once via cmark-gfm AST, extracting:
// - all headings (level + text)
// - first H1 heading text (for the first_header column)
// - all [[wiki-links]] from TEXT nodes (skipping code blocks)
// Also extracts wiki-links from frontmatter yaml_block if present.
static ParsedBody ParseBody(const string &contents, const unique_ptr<ParsedFrontmatter> &fm) {
	ParsedBody result;

	// 1. Frontmatter: extract wiki-links from raw yaml block
	if (fm) {
		ExtractWikiLinks(fm->yaml_block, result.links);
	}

	// 2. Body: skip frontmatter so cmark doesn't re-parse it as text
	const char *body_start = contents.c_str();
	size_t body_size = contents.size();
	if (fm) {
		body_start = contents.c_str() + fm->body_offset;
		body_size = contents.size() - fm->body_offset;
	}

	cmark_node *doc = cmark_parse_document(body_start, body_size, CMARK_OPT_DEFAULT);
	if (!doc) {
		return result;
	}

	cmark_iter *iter = cmark_iter_new(doc);
	cmark_event_type ev;
	int current_heading_level = 0;
	string current_heading_text;

	while ((ev = cmark_iter_next(iter)) != CMARK_EVENT_DONE) {
		cmark_node *node = cmark_iter_get_node(iter);
		cmark_node_type type = cmark_node_get_type(node);

		if (type == CMARK_NODE_HEADING) {
			if (ev == CMARK_EVENT_ENTER) {
				current_heading_level = cmark_node_get_heading_level(node);
				current_heading_text.clear();
			} else if (ev == CMARK_EVENT_EXIT) {
				if (!current_heading_text.empty()) {
					result.headings.push_back({current_heading_level, current_heading_text});
					if (current_heading_level == 1 && result.h1_heading.empty()) {
						result.h1_heading = current_heading_text;
					}
				}
				current_heading_level = 0;
				current_heading_text.clear();
			}
		} else if (ev == CMARK_EVENT_ENTER && type == CMARK_NODE_TEXT) {
			const char *lit = cmark_node_get_literal(node);
			if (lit) {
				ExtractWikiLinks(string(lit), result.links);
				if (current_heading_level > 0) {
					current_heading_text += lit;
				}
			}
		}
	}
	cmark_iter_free(iter);
	cmark_node_free(doc);

	return result;
}

static LogicalType HeadingStructType() {
	child_list_t<LogicalType> fields;
	fields.emplace_back("level", LogicalType::INTEGER);
	fields.emplace_back("text", LogicalType::VARCHAR);
	return LogicalType::STRUCT(std::move(fields));
}

static Value HeadingToValue(const ParsedHeading &heading, const LogicalType &struct_type) {
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

static Value InternalLinkToValue(const InternalLink &link, const LogicalType &struct_type) {
	child_list_t<Value> fields;
	fields.emplace_back("target", Value(link.target));
	fields.emplace_back("display_name",
	                    link.display_name.empty() ? Value(LogicalType::VARCHAR) : Value(link.display_name));
	fields.emplace_back("header", link.header.empty() ? Value(LogicalType::VARCHAR) : Value(link.header));
	fields.emplace_back("block_ref", link.block_ref.empty() ? Value(LogicalType::VARCHAR) : Value(link.block_ref));
	return Value::STRUCT(std::move(fields));
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

	return std::move(result);
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
	// Columns: 0=filename, 1=basename, 2=filepath, 3=relative_path,
	//          4=first_header, 5=headers, 6=properties, 7=internal_links.
	static constexpr idx_t NUM_COLS = 8;
	idx_t col_to_out[NUM_COLS];
	std::fill(col_to_out, col_to_out + NUM_COLS, idx_t(-1));
	const auto &col_ids = gstate.column_ids;
	for (idx_t i = 0; i < col_ids.size(); i++) {
		if (col_ids[i] < NUM_COLS) {
			col_to_out[col_ids[i]] = i;
		}
	}

	// Determine which expensive operations are required based on projected columns.
	// Reading the file is only needed when at least one content column is projected.
	// Parsing the body (cmark) is only needed for first_header, headers, or internal_links.
	// Emitting properties JSON is only needed when the properties column is projected.
	const bool need_file_read = col_to_out[4] != idx_t(-1) || col_to_out[5] != idx_t(-1) ||
	                            col_to_out[6] != idx_t(-1) || col_to_out[7] != idx_t(-1);
	const bool need_body      = col_to_out[4] != idx_t(-1) || col_to_out[5] != idx_t(-1) ||
	                            col_to_out[7] != idx_t(-1);
	const bool need_emit_json = col_to_out[6] != idx_t(-1);

	// Pre-fetch output data pointers for cheap VARCHAR columns (nullptr = not projected).
	auto *filename_out     = col_to_out[0] != idx_t(-1) ? FlatVector::GetData<string_t>(output.data[col_to_out[0]]) : nullptr;
	auto *basename_out     = col_to_out[1] != idx_t(-1) ? FlatVector::GetData<string_t>(output.data[col_to_out[1]]) : nullptr;
	auto *filepath_out     = col_to_out[2] != idx_t(-1) ? FlatVector::GetData<string_t>(output.data[col_to_out[2]]) : nullptr;
	auto *relpath_out      = col_to_out[3] != idx_t(-1) ? FlatVector::GetData<string_t>(output.data[col_to_out[3]]) : nullptr;
	auto *first_header_out = col_to_out[4] != idx_t(-1) ? FlatVector::GetData<string_t>(output.data[col_to_out[4]]) : nullptr;
	auto *props_out        = col_to_out[6] != idx_t(-1) ? FlatVector::GetData<string_t>(output.data[col_to_out[6]]) : nullptr;

	idx_t count = 0;
	for (idx_t i = batch_start; i < batch_end; i++) {
		string filepath = bind_data.files[i];
		std::replace(filepath.begin(), filepath.end(), '\\', '/');

		// Extract just the filename from the full path
		auto sep = filepath.find_last_of("/\\");
		string filename = (sep == string::npos) ? filepath : filepath.substr(sep + 1);

		// Filename stem (without .md extension)
		string basename = filename.size() > 3 ? filename.substr(0, filename.size() - 3) : filename;

		// Build relative path from vault root
		string relative_path = filepath;
		const string &vault_path = bind_data.vault_path;
		if (filepath.size() > vault_path.size() && filepath.compare(0, vault_path.size(), vault_path) == 0) {
			relative_path = filepath.substr(vault_path.size());
			if (!relative_path.empty() && (relative_path[0] == '/' || relative_path[0] == '\\')) {
				relative_path = relative_path.substr(1);
			}
		}

		// Write cheap VARCHAR columns directly — no file I/O needed.
		if (filename_out) filename_out[count] = StringVector::AddString(output.data[col_to_out[0]], filename);
		if (basename_out) basename_out[count] = StringVector::AddString(output.data[col_to_out[1]], basename);
		if (filepath_out) filepath_out[count] = StringVector::AddString(output.data[col_to_out[2]], filepath);
		if (relpath_out)  relpath_out[count]  = StringVector::AddString(output.data[col_to_out[3]], relative_path);

		// Expensive: file read + frontmatter/body parsing. Skipped entirely when
		// only path columns (filename, basename, filepath, relative_path) are projected.
		if (need_file_read) {
			string contents = ReadFileContents(fs, filepath);
			auto fm = ParseFrontmatter(contents);

			// Body parse (cmark) — skipped when only properties is projected.
			if (need_body) {
				auto body = ParseBody(contents, fm);

				// first_header [4]
				if (first_header_out) {
					if (body.h1_heading.empty()) {
						FlatVector::Validity(output.data[col_to_out[4]]).SetInvalid(count);
					} else {
						first_header_out[count] = StringVector::AddString(output.data[col_to_out[4]], body.h1_heading);
					}
				}

				// headers [5]
				if (col_to_out[5] != idx_t(-1)) {
					const LogicalType &hst = bind_data.heading_struct_type;
					vector<Value> hvs;
					hvs.reserve(body.headings.size());
					for (const auto &h : body.headings) {
						hvs.push_back(HeadingToValue(h, hst));
					}
					output.data[col_to_out[5]].SetValue(count, Value::LIST(hst, std::move(hvs)));
				}

				// internal_links [7]
				if (col_to_out[7] != idx_t(-1)) {
					const LogicalType &lst = bind_data.link_struct_type;
					vector<Value> lvs;
					lvs.reserve(body.links.size());
					for (const auto &link : body.links) {
						lvs.push_back(InternalLinkToValue(link, lst));
					}
					output.data[col_to_out[7]].SetValue(count, Value::LIST(lst, std::move(lvs)));
				}
			}

			// properties [6] — only serialize YAML to JSON when projected.
			if (need_emit_json) {
				string props_json = fm ? ryml::emitrs_json<string>(fm->tree) : string();
				if (props_json.empty()) {
					FlatVector::Validity(output.data[col_to_out[6]]).SetInvalid(count);
				} else {
					props_out[count] = StringVector::AddString(output.data[col_to_out[6]], props_json);
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

static void LoadInternal(ExtensionLoader &loader) {
	ExtensionHelper::TryAutoLoadExtension(loader.GetDatabaseInstance(), "json");

	// Register obsidian_notes table function with two overloads:
	// obsidian_notes()            — uses current working directory
	// obsidian_notes(vault_path)  — uses the provided path
	TableFunctionSet obsidian_notes_set("obsidian_notes");

	TableFunction no_args("obsidian_notes", {}, ObsidianNotesFunction, ObsidianNotesBind, ObsidianNotesInitGlobal,
	                      ObsidianNotesInitLocal);
	no_args.projection_pushdown = true;
	no_args.cardinality = ObsidianNotesCardinality;
	obsidian_notes_set.AddFunction(no_args);

	TableFunction with_path("obsidian_notes", {LogicalType::VARCHAR}, ObsidianNotesFunction, ObsidianNotesBind,
	                        ObsidianNotesInitGlobal, ObsidianNotesInitLocal);
	with_path.projection_pushdown = true;
	with_path.cardinality = ObsidianNotesCardinality;
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
