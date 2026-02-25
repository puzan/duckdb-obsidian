#define DUCKDB_EXTENSION_MAIN

#include "obsidian_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/vector.hpp"
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
	string title_property;
	vector<string> files;
	// Cached once at bind time; avoids re-constructing the LogicalType per row.
	LogicalType link_struct_type;
};

struct ObsidianNotesScanState : public GlobalTableFunctionState {
	std::mutex lock;
	idx_t position = 0;
	idx_t max_threads;

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

struct ParsedBody {
	string h1_heading;
	vector<InternalLink> links;
};

// Parse the document body once via cmark-gfm AST, extracting:
// - first H1 heading text
// - all [[wiki-links]] from TEXT nodes (skipping code blocks)
// Also extracts wiki-links from frontmatter yaml_block if present.
static ParsedBody ParseBody(const string &contents, const unique_ptr<ParsedFrontmatter> &fm) {
	ParsedBody result;

	// 1. Frontmatter: regex over raw yaml block
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
	while ((ev = cmark_iter_next(iter)) != CMARK_EVENT_DONE) {
		if (ev != CMARK_EVENT_ENTER) {
			continue;
		}
		cmark_node *node = cmark_iter_get_node(iter);
		cmark_node_type type = cmark_node_get_type(node);

		if (type == CMARK_NODE_TEXT) {
			const char *lit = cmark_node_get_literal(node);
			if (lit) {
				ExtractWikiLinks(string(lit), result.links);

				// Collect H1 heading text (first one only)
				if (result.h1_heading.empty()) {
					cmark_node *parent = cmark_node_parent(node);
					if (parent && cmark_node_get_type(parent) == CMARK_NODE_HEADING &&
					    cmark_node_get_heading_level(parent) == 1) {
						result.h1_heading += lit;
					}
				}
			}
		}
	}
	cmark_iter_free(iter);
	cmark_node_free(doc);

	return result;
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

// Resolve title: frontmatter <title_property> > first H1 heading > filename stem.
static string ResolveTitle(const string &filename_stem, const unique_ptr<ParsedFrontmatter> &fm,
                           const string &title_property, const string &h1_heading) {
	if (fm) {
		ryml::ConstNodeRef root = fm->tree.rootref();
		ryml::csubstr prop_key = ryml::to_csubstr(title_property);
		if (root.has_child(prop_key)) {
			auto title_node = root[prop_key];
			if (title_node.has_val()) {
				ryml::csubstr val = title_node.val();
				if (!val.empty()) {
					return string(val.str, val.len);
				}
			}
		}
	}

	if (!h1_heading.empty()) {
		return h1_heading;
	}

	return filename_stem;
}

static unique_ptr<FunctionData> ObsidianNotesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("obsidian_notes requires a vault path argument");
	}

	auto result = make_uniq<ObsidianNotesScanData>();
	result->vault_path = input.inputs[0].GetValue<string>();
	result->title_property = "title";
	auto it = input.named_parameters.find("title_property");
	if (it != input.named_parameters.end() && !it->second.IsNull()) {
		result->title_property = it->second.GetValue<string>();
	}

	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(result->vault_path)) {
		throw IOException("Vault path does not exist: " + result->vault_path);
	}

	string obsidian_config_dir = fs.JoinPath(result->vault_path, ".obsidian");
	if (!fs.DirectoryExists(obsidian_config_dir)) {
		throw IOException("Path is not an Obsidian vault (missing .obsidian directory): " + result->vault_path);
	}

	CollectMarkdownFiles(fs, result->vault_path, result->files);

	result->link_struct_type = InternalLinkStructType();

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("filename");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("filepath");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("relative_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("title");
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
	return make_uniq<ObsidianNotesScanState>(max_threads);
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

	// Write directly to flat vector buffers instead of going through Value boxing.
	// This avoids heap allocations for every cell in the four VARCHAR columns.
	auto filename_data = FlatVector::GetData<string_t>(output.data[0]);
	auto filepath_data = FlatVector::GetData<string_t>(output.data[1]);
	auto relpath_data = FlatVector::GetData<string_t>(output.data[2]);
	auto title_data = FlatVector::GetData<string_t>(output.data[3]);
	auto props_data = FlatVector::GetData<string_t>(output.data[4]);
	auto &props_validity = FlatVector::Validity(output.data[4]);

	idx_t count = 0;
	for (idx_t i = batch_start; i < batch_end; i++) {
		string filepath = bind_data.files[i];
		std::replace(filepath.begin(), filepath.end(), '\\', '/');

		// Extract just the filename from the full path
		auto sep = filepath.find_last_of("/\\");
		string filename = (sep == string::npos) ? filepath : filepath.substr(sep + 1);

		// Filename stem (without .md)
		string stem = filename;
		if (stem.size() > 3) {
			stem = stem.substr(0, stem.size() - 3);
		}

		// Build relative path from vault root
		string relative_path = filepath;
		const string &vault_path = bind_data.vault_path;
		if (filepath.size() > vault_path.size() && filepath.compare(0, vault_path.size(), vault_path) == 0) {
			relative_path = filepath.substr(vault_path.size());
			if (!relative_path.empty() && (relative_path[0] == '/' || relative_path[0] == '\\')) {
				relative_path = relative_path.substr(1);
			}
		}

		string contents = ReadFileContents(fs, filepath);
		auto fm = ParseFrontmatter(contents);
		auto body = ParseBody(contents, fm);
		string title = ResolveTitle(stem, fm, bind_data.title_property, body.h1_heading);
		string properties_json = fm ? ryml::emitrs_json<string>(fm->tree) : string();

		filename_data[count] = StringVector::AddString(output.data[0], filename);
		filepath_data[count] = StringVector::AddString(output.data[1], filepath);
		relpath_data[count] = StringVector::AddString(output.data[2], relative_path);
		title_data[count] = StringVector::AddString(output.data[3], title);
		if (properties_json.empty()) {
			props_validity.SetInvalid(count);
		} else {
			props_data[count] = StringVector::AddString(output.data[4], properties_json);
		}

		// Use cached link_struct_type instead of constructing it on every row.
		const LogicalType &struct_type = bind_data.link_struct_type;
		vector<Value> link_values;
		link_values.reserve(body.links.size());
		for (const auto &link : body.links) {
			link_values.push_back(InternalLinkToValue(link, struct_type));
		}
		output.data[5].SetValue(count, Value::LIST(struct_type, std::move(link_values)));
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

	// Register obsidian_notes table function
	TableFunction obsidian_notes_function("obsidian_notes", {LogicalType::VARCHAR}, ObsidianNotesFunction,
	                                      ObsidianNotesBind, ObsidianNotesInitGlobal, ObsidianNotesInitLocal);
	obsidian_notes_function.named_parameters["title_property"] = LogicalType::VARCHAR;
	obsidian_notes_function.cardinality = ObsidianNotesCardinality;
	loader.RegisterFunction(obsidian_notes_function);
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
