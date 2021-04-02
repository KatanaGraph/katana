#include "katana/Strings.h"

bool
katana::HasPrefix(const std::string& s, const std::string& prefix) {
  size_t prefix_len = prefix.length();
  size_t s_len = s.length();

  if (prefix_len > s_len) {
    return false;
  }

  return s.compare(0, prefix_len, prefix) == 0;
}

std::string
katana::TrimPrefix(const std::string& s, const std::string& prefix) {
  if (HasPrefix(s, prefix)) {
    size_t prefix_len = prefix.length();
    return s.substr(prefix_len, s.length() - prefix_len);
  }
  return s;
}

bool
katana::HasSuffix(const std::string& s, const std::string& suffix) {
  size_t suffix_len = suffix.length();
  size_t s_len = s.length();

  if (suffix_len > s_len) {
    return false;
  }

  return s.compare(s_len - suffix_len, suffix_len, suffix) == 0;
}

std::string
katana::TrimSuffix(const std::string& s, const std::string& suffix) {
  if (HasSuffix(s, suffix)) {
    size_t suffix_len = suffix.length();
    return s.substr(0, s.length() - suffix_len);
  }
  return s;
}

std::string
katana::FromBase64(std::string input)
{
  using namespace boost::archive::iterators;
  typedef transform_width<binary_from_base64<remove_whitespace
      <std::string::const_iterator> >, 8, 6> ItBinaryT;

  try
  {
    // If the input isn't a multiple of 4, pad with =
    size_t num_pad_chars((4 - input.size() % 4) % 4);
    input.append(num_pad_chars, '=');

    size_t pad_chars(std::count(input.begin(), input.end(), '='));
    std::replace(input.begin(), input.end(), '=', 'A');
    std::string output(ItBinaryT(input.begin()), ItBinaryT(input.end()));
    output.erase(output.end() - pad_chars, output.end());
    return output;
  }
  catch (std::exception const&)
  {
    return std::string("");
}
}

std::string
katana::ToBase64(std::string message)
{
	using namespace boost::archive::iterators;

	std::stringstream os;
	using base64_text = insert_linebreaks<base64_from_binary<transform_width<const char *, 6, 8>>, 72>;

	std::copy(
			base64_text(message.c_str()),
			base64_text(message.c_str() + message.size()),
			std::ostream_iterator<char>(os)
	);

	return os.str();
}
