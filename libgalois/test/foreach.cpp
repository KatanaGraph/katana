/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

#include <iostream>
#include <vector>

#include "katana/Bag.h"
#include "katana/Galois.h"

void
function_pointer(int x, katana::UserContext<int>&) {
  std::cout << x << "\n";
}

struct function_object {
  void operator()(int x, katana::UserContext<int>& ctx) const {
    function_pointer(x, ctx);
  }
};

int
main() {
  katana::SharedMemSys Katana_runtime;
  std::vector<int> v(10);
  katana::InsertBag<int> b;

  katana::for_each(
      katana::iterate(v), &function_pointer, katana::loopname("func-pointer"));
  katana::for_each(
      katana::iterate(v), function_object(),
      katana::loopname("with function object and options"));
  katana::do_all(katana::iterate(v), [&b](int x) { b.push(x); });
  katana::for_each(katana::iterate(b), function_object());

  return 0;
}
