#include "katana/Properties.h"

template <typename ViewType, typename T, typename U>
void
Compare(
    const std::vector<std::optional<T>>& vec, const std::shared_ptr<U>& array) {
  KATANA_LOG_ASSERT(vec.size() == (size_t)array->length());
  auto res = ViewType::Make(*array);
  KATANA_LOG_ASSERT(res);
  auto view = std::move(res.value());
  for (size_t i = 0, n = vec.size(); i < n; ++i) {
    if (vec[i]) {
      KATANA_LOG_ASSERT(view.IsValid(i));
      KATANA_LOG_ASSERT(*vec[i] == view[i]);
    } else {
      KATANA_LOG_ASSERT(!view.IsValid(i));
      KATANA_LOG_ASSERT(view[i] == T{});
    }
  }
}

template <typename T>
auto
MakeArray(const std::vector<std::optional<T>>& vec) {
  using ArrowType = typename arrow::CTypeTraits<T>::ArrowType;
  using BuilderType = typename arrow::TypeTraits<ArrowType>::BuilderType;
  using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
  BuilderType builder;
  for (auto v : vec) {
    if (v) {
      KATANA_LOG_ASSERT(builder.Append(*v).ok());
    } else {
      KATANA_LOG_ASSERT(builder.AppendNull().ok());
    }
  }
  std::shared_ptr<ArrayType> array;
  KATANA_LOG_ASSERT(builder.Finish(&array).ok());
  KATANA_LOG_ASSERT(array);
  return array;
}

template <typename ViewType, typename T, typename U>
void
TestSliced(
    const std::vector<std::optional<T>>& vec, const std::shared_ptr<U>& array,
    size_t offset, size_t length) {
  KATANA_LOG_ASSERT(offset + length <= vec.size());
  auto begin = vec.begin() + offset;
  std::vector<std::optional<T>> slice_vec(begin, begin + length);
  auto slice_array = array->Slice(offset, length);
  auto slice_typed = std::static_pointer_cast<U>(slice_array);
  Compare<ViewType>(slice_vec, slice_typed);
}

template <typename T>
void
TestPOD() {
  using VecType = std::vector<std::optional<T>>;
  using ViewType = typename katana::PODProperty<T>::ViewType;
  VecType vec{1, 2, std::nullopt, 3, std::nullopt, std::nullopt, 6, 7,
              8, 9, std::nullopt};
  auto array = MakeArray(vec);
  TestSliced<ViewType>(vec, array, 0, vec.size());
  TestSliced<ViewType>(vec, array, 3, vec.size() - 3);
  TestSliced<ViewType>(vec, array, 1, vec.size() - 6);
}

void
TestString() {
  using VecType = std::vector<std::optional<std::string>>;
  using ViewType = katana::StringReadOnlyProperty::ViewType;
  VecType vec{"1", "2", std::nullopt, "3", std::nullopt, std::nullopt,
              "6", "7", "8",          "9", std::nullopt};
  auto array = MakeArray(vec);
  TestSliced<ViewType>(vec, array, 0, vec.size());
  TestSliced<ViewType>(vec, array, 3, vec.size() - 3);
  TestSliced<ViewType>(vec, array, 1, vec.size() - 6);
}

void
TestBool() {
  using VecType = std::vector<std::optional<bool>>;
  using ViewType = katana::BooleanReadOnlyProperty::ViewType;
  VecType vec{true,  false, std::nullopt, true, std::nullopt, std::nullopt,
              false, false, false,        true, std::nullopt};
  auto array = MakeArray(vec);
  TestSliced<ViewType>(vec, array, 0, vec.size());
  TestSliced<ViewType>(vec, array, 3, vec.size() - 3);
  TestSliced<ViewType>(vec, array, 1, vec.size() - 6);
}

int
main() {
  TestPOD<int8_t>();
  TestPOD<uint8_t>();
  TestPOD<int16_t>();
  TestPOD<uint16_t>();
  TestPOD<int32_t>();
  TestPOD<uint32_t>();
  TestPOD<int64_t>();
  TestPOD<uint64_t>();
  TestPOD<float>();
  TestPOD<double>();
  TestString();
  TestBool();
  KATANA_LOG_VERBOSE("success");
  return 0;
}
