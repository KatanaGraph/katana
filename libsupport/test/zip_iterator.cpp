#include <iostream>
#include <vector>

#include "katana/Iterators.h"
#include "katana/Logging.h"

int
main() {
  std::vector<int> vec_a{5, 3, 4, 2, 1};
  std::vector<char> vec_b{'e', 'c', 'd', 'b', 'a'};

  std::vector<std::pair<int, char>> zip_using_vec;
  for (size_t i = 0; i < vec_a.size(); ++i) {
    zip_using_vec.emplace_back(vec_a[i], vec_b[i]);
  }
  std::sort(zip_using_vec.begin(), zip_using_vec.end());

  const auto beg = katana::make_zip_iterator(vec_a.begin(), vec_b.begin());
  const auto end = katana::make_zip_iterator(vec_a.end(), vec_b.end());

  std::sort(beg, end, [&](const auto& tup1, const auto& tup2) {
    return std::get<0>(tup1) < std::get<0>(tup2);
  });

  std::cout << "[";
  for (auto i = beg; i != end; ++i) {
    std::cout << "(" << std::get<0>(*i) << "," << std::get<1>(*i) << "), ";
  }
  std::cout << "]" << std::endl;

  std::vector<std::pair<int, char>> zip_using_iter;
  for (auto i = beg; i < end; ++i) {
    zip_using_iter.emplace_back(std::get<0>(*i), std::get<1>(*i));
  }

  KATANA_LOG_VASSERT(
      zip_using_vec == zip_using_iter,
      "both vectors should be sorted and equal");

  return 0;
}
