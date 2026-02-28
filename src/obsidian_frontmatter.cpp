#include "obsidian_frontmatter.hpp"

#include <ryml/ryml_std.hpp>
#include <stdexcept>

namespace duckdb {

// ryml calls this instead of abort() when it encounters a parse error.
// We throw so that our try/catch can handle malformed YAML gracefully.
static void RymlErrorCallback(const char *msg, size_t msg_len, ryml::Location /*loc*/, void * /*userdata*/) {
	throw std::runtime_error(std::string(msg, msg_len));
}

unique_ptr<ParsedFrontmatter> ParseFrontmatter(const string &s) {
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

} // namespace duckdb
