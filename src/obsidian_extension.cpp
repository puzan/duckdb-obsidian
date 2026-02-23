#define DUCKDB_EXTENSION_MAIN

#include "obsidian_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"

#include <cmark-gfm.h>
#include <ryml/ryml.hpp>
#include <ryml/ryml_std.hpp>

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

// Extract title: frontmatter title > first H1 heading > filename stem.
static string ExtractTitle(const string &contents, const string &filename_stem,
                           const unique_ptr<ParsedFrontmatter> &fm) {
	if (fm) {
		ryml::ConstNodeRef root = fm->tree.rootref();
		if (root.has_child("title")) {
			auto title_node = root["title"];
			if (title_node.has_val()) {
				ryml::csubstr val = title_node.val();
				if (!val.empty()) {
					return string(val.str, val.len);
				}
			}
		}
	}

	// --- Try first H1 heading via cmark-gfm ---
	cmark_node *doc = cmark_parse_document(contents.c_str(), contents.size(), CMARK_OPT_DEFAULT);
	if (doc) {
		string heading_text;
		cmark_node *node = cmark_node_first_child(doc);
		while (node) {
			if (cmark_node_get_type(node) == CMARK_NODE_HEADING && cmark_node_get_heading_level(node) == 1) {
				cmark_node *child = cmark_node_first_child(node);
				while (child) {
					if (cmark_node_get_type(child) == CMARK_NODE_TEXT) {
						const char *lit = cmark_node_get_literal(child);
						if (lit) {
							heading_text += lit;
						}
					}
					child = cmark_node_next(child);
				}
				break;
			}
			node = cmark_node_next(node);
		}
		cmark_node_free(doc);
		if (!heading_text.empty()) {
			return heading_text;
		}
	}

	// --- Fallback: filename without .md extension ---
	return filename_stem;
}

static unique_ptr<FunctionData> ObsidianNotesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("obsidian_notes requires a vault path argument");
	}

	auto result = make_uniq<ObsidianNotesScanData>();
	result->vault_path = input.inputs[0].GetValue<string>();

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
		const string &filepath = bind_data.files[state.position];

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
		string title = ExtractTitle(contents, stem, fm);
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
		count++;
		state.position++;
	}
	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register obsidian_notes table function
	TableFunction obsidian_notes_function("obsidian_notes", {LogicalType::VARCHAR}, ObsidianNotesFunction,
	                                      ObsidianNotesBind, ObsidianNotesInitGlobal);
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
