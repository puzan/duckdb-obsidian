#include "obsidian_body.hpp"

#include <cmark-gfm.h>

namespace duckdb {

ParsedBody ParseBody(const string &contents, const unique_ptr<ParsedFrontmatter> &fm) {
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

} // namespace duckdb
