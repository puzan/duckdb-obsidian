#pragma once

#include "obsidian_frontmatter.hpp"
#include "obsidian_wikilinks.hpp"

namespace duckdb {

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
ParsedBody ParseBody(const string &contents, const unique_ptr<ParsedFrontmatter> &fm);

} // namespace duckdb
