#include <iostream>
#include <numeric>
#include <vector>
#include <utility>

#include "katana/Iterators.h"
#include "katana/Logging.h"

int main() {

  size_t VEC_SIZE = 7ull;
  size_t VAL_A = 10ull;
  size_t VAL_B = 20ull;

  std::vector<size_t> vec_a(VEC_SIZE, VAL_A);
  std::vector<size_t> vec_b(2 * VEC_SIZE, VAL_B);

  auto iter_pair_a = std::make_pair(vec_a.begin(), vec_a.end());
  auto iter_pair_b = std::make_pair(vec_b.begin(), vec_b.end());

  auto beg = katana::make_disjoint_ranges_begin(iter_pair_a, iter_pair_b);
  auto end = katana::make_disjoint_ranges_end(iter_pair_a, iter_pair_b);

  size_t sum = std::accumulate(beg, end, 0);

  KATANA_LOG_VASSERT(sum == vec_a.size() * VAL_A + vec_b.size() * VAL_B, "incorrect sum produced");

  return 0;
}
