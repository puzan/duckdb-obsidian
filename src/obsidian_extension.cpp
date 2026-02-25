#define DUCKDB_EXTENSION_MAIN

#include "obsidian_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"

#include <cmark-gfm.h>
#include <ryml/ryml.hpp>
#include <ryml/ryml_std.hpp>

#include <algorithm>
#include <regex>
#include <stdexcept>

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
};

struct ObsidianNotesScanState : public GlobalTableFunctionState {
	idx_t position = 0;
};

static void CollectMarkdownFiles(FileSystem &fs, const string &dir, vector<string> &files) {
	fs.ListFiles(dir, [&](const string &name, bool is_dir) {
		// Skip hidden directories (e.g. .obsidian, .git)
		if (!name.empty() && name[0] == '.') {
			return;
		}
		string full_path = fs.JoinPath(dir, name);
		if (is_dir) {
			CollectMarkdownFiles(fs, full_path, files);
		} else if (name.size() > 3 && name.substr(name.size() - 3) == ".md") {
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
	if (s.size() < 3 || s.substr(0, 3) != "---") {
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
static void ExtractWikiLinks(const string &text, vector<InternalLink> &links) {
	static const std::regex wiki_link_re(R"(\[\[([^\[\]]+)\]\])");
	auto begin = std::sregex_iterator(text.begin(), text.end(), wiki_link_re);
	auto end = std::sregex_iterator();
	for (auto it = begin; it != end; ++it) {
		string inner = (*it)[1].str();

		InternalLink link;

		// Split on '|' to get optional display_name
		auto pipe_pos = inner.find('|');
		string target_part;
		if (pipe_pos != string::npos) {
			link.display_name = inner.substr(pipe_pos + 1);
			target_part = inner.substr(0, pipe_pos);
		} else {
			target_part = inner;
		}

		// Split target_part on '#' to get optional anchor
		auto hash_pos = target_part.find('#');
		if (hash_pos != string::npos) {
			link.target = target_part.substr(0, hash_pos);
			string anchor = target_part.substr(hash_pos + 1);
			if (!anchor.empty() && anchor[0] == '^') {
				link.block_ref = anchor.substr(1);
			} else {
				link.header = anchor;
			}
		} else {
			link.target = target_part;
		}

		links.push_back(std::move(link));
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
	return_types.emplace_back(LogicalType::LIST(InternalLinkStructType()));
	names.emplace_back("internal_links");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ObsidianNotesInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	return make_uniq<ObsidianNotesScanState>();
}

static void ObsidianNotesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ObsidianNotesScanData>();
	auto &state = data_p.global_state->Cast<ObsidianNotesScanState>();
	auto &fs = FileSystem::GetFileSystem(context);

	idx_t count = 0;
	while (state.position < bind_data.files.size() && count < STANDARD_VECTOR_SIZE) {
		string filepath = bind_data.files[state.position];
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
		if (filepath.size() > vault_path.size() && filepath.substr(0, vault_path.size()) == vault_path) {
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

		output.data[0].SetValue(count, Value(filename));
		output.data[1].SetValue(count, Value(filepath));
		output.data[2].SetValue(count, Value(relative_path));
		output.data[3].SetValue(count, Value(title));
		if (properties_json.empty()) {
			output.data[4].SetValue(count, Value(nullptr));
		} else {
			output.data[4].SetValue(count, Value(properties_json));
		}

		auto struct_type = InternalLinkStructType();
		vector<Value> link_values;
		link_values.reserve(body.links.size());
		for (const auto &link : body.links) {
			link_values.push_back(InternalLinkToValue(link, struct_type));
		}
		output.data[5].SetValue(count, Value::LIST(struct_type, std::move(link_values)));
		count++;
		state.position++;
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
	                                      ObsidianNotesBind, ObsidianNotesInitGlobal);
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
