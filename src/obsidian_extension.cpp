#define DUCKDB_EXTENSION_MAIN

#include "obsidian_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void ObsidianScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Obsidian " + name.GetString() + " 🐥");
	});
}

inline void ObsidianOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Obsidian " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
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

	CollectMarkdownFiles(fs, result->vault_path, result->files);

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("filename");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("filepath");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ObsidianNotesInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	return make_uniq<ObsidianNotesScanState>();
}

static void ObsidianNotesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ObsidianNotesScanData>();
	auto &state = data_p.global_state->Cast<ObsidianNotesScanState>();

	idx_t count = 0;
	while (state.position < bind_data.files.size() && count < STANDARD_VECTOR_SIZE) {
		const string &filepath = bind_data.files[state.position];

		// Extract just the filename from the full path
		auto sep = filepath.find_last_of("/\\");
		string filename = (sep == string::npos) ? filepath : filepath.substr(sep + 1);

		output.data[0].SetValue(count, Value(filename));
		output.data[1].SetValue(count, Value(filepath));
		count++;
		state.position++;
	}
	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto obsidian_scalar_function = ScalarFunction("obsidian", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ObsidianScalarFun);
	loader.RegisterFunction(obsidian_scalar_function);

	// Register another scalar function
	auto obsidian_openssl_version_scalar_function = ScalarFunction("obsidian_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, ObsidianOpenSSLVersionScalarFun);
	loader.RegisterFunction(obsidian_openssl_version_scalar_function);

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
