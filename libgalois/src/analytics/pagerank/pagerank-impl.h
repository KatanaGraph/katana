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

#ifndef KATANA_LIBGALOIS_ANALYTICS_PAGERANK_PAGERANKIMPL_H_
#define KATANA_LIBGALOIS_ANALYTICS_PAGERANK_PAGERANKIMPL_H_

#include <iostream>

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "katana/PropertyGraph.h"
#include "katana/analytics/Utils.h"
#include "katana/analytics/pagerank/pagerank.h"

typedef float PRTy;
using NodeValue = katana::PODProperty<PRTy>;

katana::Result<void> PagerankPullTopological(
    katana::PropertyFileGraph* pfg, const std::string& output_property_name,
    katana::analytics::PagerankPlan plan);

katana::Result<void> PagerankPullResidual(
    katana::PropertyFileGraph* pfg, const std::string& output_property_name,
    katana::analytics::PagerankPlan plan);

katana::Result<void> PagerankPushAsynchronous(
    katana::PropertyFileGraph* pfg, const std::string& output_property_name,
    katana::analytics::PagerankPlan plan);

katana::Result<void> PagerankPushSynchronous(
    katana::PropertyFileGraph* pfg, const std::string& output_property_name,
    katana::analytics::PagerankPlan plan);

#endif
