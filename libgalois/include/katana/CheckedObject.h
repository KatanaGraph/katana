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

#ifndef KATANA_LIBGALOIS_KATANA_CHECKEDOBJECT_H_
#define KATANA_LIBGALOIS_KATANA_CHECKEDOBJECT_H_

#include <type_traits>

#include "katana/Context.h"
#include "katana/config.h"

namespace katana {

/**
 * Conflict-checking wrapper for any type.  Performs global conflict detection
 * on the enclosed object.  This enables arbitrary types to be managed by the
 * Galois runtime.
 */
template <typename T>
class GChecked : public katana::Lockable {
  T val;

public:
  template <
      typename... Args,
      std::enable_if_t<std::is_constructible_v<T, Args...>, bool> = false>
  explicit GChecked(Args&&... args) : val(std::forward<Args>(args)...) {}

  T& get(katana::MethodFlag m = MethodFlag::WRITE) {
    katana::acquire(this, m);
    return val;
  }

  const T& get(katana::MethodFlag m = MethodFlag::WRITE) const {
    katana::acquire(const_cast<GChecked*>(this), m);
    return val;
  }
};

template <>
class GChecked<void> : public katana::Lockable {
public:
  void get(katana::MethodFlag m = MethodFlag::WRITE) const {
    katana::acquire(const_cast<GChecked*>(this), m);
  }
};

}  // namespace katana

#endif
