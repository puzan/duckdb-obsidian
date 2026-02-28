#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct InternalLink {
	string target;
	string display_name; // empty = absent
	string header;       // empty = absent
	string block_ref;    // empty = absent
};

// Extract [[wiki-links]] from a plain text string and append to links vector.
// Hand-rolled parser — avoids std::regex heap allocations on every NFA step.
void ExtractWikiLinks(const string &text, vector<InternalLink> &links);

} // namespace duckdb
