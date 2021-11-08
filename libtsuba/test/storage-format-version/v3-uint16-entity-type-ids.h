#ifndef KATANA_LIBTSUBA_STORAGEFORMATVERSION_V3UINT16ENTITYTYPEIDS_H_
#define KATANA_LIBTSUBA_STORAGEFORMATVERSION_V3UINT16ENTITYTYPEIDS_H_

#include <limits>
#include <string>
#include <vector>

#include "katana/EntityTypeManager.h"

// Support functions

/// Creates a num_strings sized vector of unique strings
/// of the form:
/// [a, b, c, ... aa, ab, ac, ... ba, bb, bc, ..... aaa, aab, aac,...]
std::vector<std::string>
vector_unique_strings(size_t num_strings) {
  const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  std::vector<std::string> strings;
  std::string new_str;

  // prime the set
  for (size_t i = 0; i < sizeof(charset); i++) {
    new_str = std::string();
    new_str.push_back(charset[i]);
    strings.emplace_back(new_str);
  }

  // goooooooo
  for (size_t i = 0, vector_index = 0, char_index = 0; i < num_strings;
       i++, char_index++) {
    if (char_index >= sizeof(charset) - 1) {
      char_index = 0;
      vector_index++;
    }
    new_str = strings.at(vector_index);
    new_str.push_back(charset[char_index]);
    strings.emplace_back(new_str);
  }

  return strings;
}

#endif
