#pragma once

#include "duckdb.hpp"
#include <ryml/ryml.hpp>

namespace duckdb {

struct ParsedFrontmatter {
	string yaml_block; // owns the buffer that ryml::Tree points into
	ryml::Tree tree;
	size_t body_offset = 0; // byte offset in the original file where body starts
};

// Parse YAML frontmatter block. Returns nullptr if absent or malformed.
unique_ptr<ParsedFrontmatter> ParseFrontmatter(const string &s);

// Serialize the parsed frontmatter tree to JSON. Returns empty string if fm is null.
string FrontmatterToJson(const unique_ptr<ParsedFrontmatter> &fm);

} // namespace duckdb
