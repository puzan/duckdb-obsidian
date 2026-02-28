#include "obsidian_wikilinks.hpp"

#include <cstring>

namespace duckdb {

void ExtractWikiLinks(const string &text, vector<InternalLink> &links) {
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

} // namespace duckdb
