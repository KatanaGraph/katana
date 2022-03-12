#ifndef KATANA_LIBGRAPH_KATANA_HASHINDEXEDPROPERTY_H_
#define KATANA_LIBGRAPH_KATANA_HASHINDEXEDPROPERTY_H_

namespace katana {

class KATANA_EXPORT HashIndexedProperty {
public:
  using HashMap = std::unordered_map<int64_t, int64_t>;

  static Result<HashIndexedProperty> Deflate(const arrow::Array&);

  Result<std::shared_ptr<arrow::Array>> Inflate() const;

private:
  HashIndexedProperty(HashMap index, std::shared_ptr<arrow::Array> data)
      : index_(std::move(index)), data_(std::move(data)) {}
  HashMap index_;
  std::shared_ptr<arrow::Array> data_;
};

}  // namespace katana

#endif
