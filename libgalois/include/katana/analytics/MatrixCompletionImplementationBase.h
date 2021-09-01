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

#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_MATRIXCOMPLETIONIMPLEMENTATIONBASE_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_MATRIXCOMPLETIONIMPLEMENTATIONBASE_H_

#include "katana/AtomicHelpers.h"

#define LATENT_VECTOR_SIZE 20

namespace katana::analytics {

template <typename _Graph>
struct MatrixCompletionImplementationBase {
  using Graph = _Graph;
  using GNode = typename Graph::Node;

  // returns the result of the inner product of 2 NodeLatentVectors
  // assumes both vectors are of seize equal to LATENT_VECTOR_SIZE
  template <typename NodeIndex>
  double InnerProduct(
      katana::PropertyReferenceType<NodeIndex> first_vector,
      katana::PropertyReferenceType<NodeIndex> second_vector) {
    double res = 0;
    for (int i = 0; i < LATENT_VECTOR_SIZE; i++) {
      res += first_vector[i] * second_vector[i];
    }
    return res;
  }

  template <typename NodeIndex>
  double PredictionError(
      katana::PropertyReferenceType<NodeIndex> item_latent_vector,
      katana::PropertyReferenceType<NodeIndex> user_latent_vector,
      double actual) {
    return actual -
           InnerProduct<NodeIndex>(item_latent_vector, user_latent_vector);
  }

  /*
     * Generate a number [-1, 1] using node id
     * for deterministic runs
     */
  double GenVal(GNode n) { return 2.0 * ((double)n / (double)RAND_MAX) - 1.0; }

  template <typename T, unsigned Size>
  struct ExplicitFiniteChecker {};

  template <typename T>
  struct ExplicitFiniteChecker<T, 4U> {
    static_assert(
        std::numeric_limits<T>::is_iec559, "Need IEEE floating point");
    bool IsFinite(T v) {
      union {
        T value;
        uint32_t bits;
      } a = {v};
      if (a.bits == 0x7F800000) {
        return false;  // +inf
      } else if (a.bits == 0xFF800000) {
        return false;  // -inf
      } else if (a.bits >= 0x7F800001 && a.bits <= 0x7FBFFFFF) {
        return false;  // signaling NaN
      } else if (a.bits >= 0xFF800001 && a.bits <= 0xFFBFFFFF) {
        return false;  // signaling NaN
      } else if (a.bits >= 0x7FC00000 && a.bits <= 0x7FFFFFFF) {
        return false;  // quiet NaN
      } else if (a.bits >= 0xFFC00000 && a.bits <= 0xFFFFFFFF) {
        return false;  // quiet NaN
      }
      return true;
    }
  };

  template <typename T>
  struct ExplicitFiniteChecker<T, 8U> {
    static_assert(
        std::numeric_limits<T>::is_iec559, "Need IEEE floating point");
    bool IsFinite(T v) {
      union {
        T value;
        uint64_t bits;
      } a = {v};
      if (a.bits == 0x7FF0000000000000) {
        return false;  // +inf
      } else if (a.bits == 0xFFF0000000000000) {
        return false;  // -inf
      } else if (a.bits >= 0x7FF0000000000001 && a.bits <= 0x7FF7FFFFFFFFFFFF) {
        return false;  // signaling NaN
      } else if (a.bits >= 0xFFF0000000000001 && a.bits <= 0xFFF7FFFFFFFFFFFF) {
        return false;  // signaling NaN
      } else if (a.bits >= 0x7FF8000000000000 && a.bits <= 0x7FFFFFFFFFFFFFFF) {
        return false;  // quiet NaN
      } else if (a.bits >= 0xFFF8000000000000 && a.bits <= 0xFFFFFFFFFFFFFFFF) {
        return false;  // quiet NaN
      }
      return true;
    }
  };

  template <typename T>
  bool IsFinite(T v) {
#ifdef __FAST_MATH__
    return ExplicitFiniteChecker<T, sizeof(T)>().IsFinite(v);
#else
    return std::isfinite(v);
#endif
  }
};

}  // namespace katana::analytics
#endif  // CLUSTERING_H
