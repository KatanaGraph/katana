#include "galois/graphs/PropertyFileGraph.h"
#include "galois/Logging.h"

#include <iostream>
namespace galois {
constexpr uint32_t CHUNK_SIZE_64 = 64;

class UpdateRequestIndexer {
public:
  uint32_t shift;

  UpdateRequestIndexer(uint32_t _shift) : shift(_shift) {}
  template <typename R>
  unsigned int operator()(const R& req) const {
    unsigned int t = req.dist >> shift;
    return t;
  }
};

template <typename GNode, typename Dist>
struct UpdateRequest {
  GNode src;
  Dist dist;
  UpdateRequest(const GNode& N, Dist W) : src(N), dist(W) {}
  UpdateRequest() : src(), dist(0) {}

  friend bool operator<(const UpdateRequest& left, const UpdateRequest& right) {
    return left.dist == right.dist ? left.src < right.src
                                   : left.dist < right.dist;
  }
};

struct ReqPushWrap {
  template <typename C, typename GNode, typename Dist>
  void operator()(C& cont, const GNode& n, const Dist& dist) const {
    cont.push(UpdateRequest<GNode, Dist>(n, dist));
  }
};

/*
 * Extra helper functions for PropertyFileGraph
 */
std::shared_ptr<arrow::Table> MakeTable(const std::string& name,
                                        const std::vector<uint32_t>& data) {
  arrow::NumericBuilder<arrow::UInt32Type> builder;

  auto append_status = builder.AppendValues(data.begin(), data.end());
  GALOIS_LOG_ASSERT(append_status.ok());

  std::shared_ptr<arrow::Array> array;

  auto finish_status = builder.Finish(&array);
  GALOIS_LOG_ASSERT(finish_status.ok());

  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field(name, arrow::uint32())});

  return arrow::Table::Make(schema, {array});
}

// template <typename T>
// std::vector<std::shared_ptr<T>>& GetData(const shared_ptr<PropertyFileGraph>
// pfg, int pid) {

//}

} // namespace galois
