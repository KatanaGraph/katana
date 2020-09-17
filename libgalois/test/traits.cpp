#include "galois/Traits.h"

#include <iostream>
#include <utility>

#include "galois/gIO.h"

struct A {};

struct B : public A {
  std::string name_;
  B(std::string name) : name_(std::move(name)) {}
  B() : B("") {}
};

struct Unrelated {};

template <size_t... Ints, typename Tuple>
void
Print(std::index_sequence<Ints...>, Tuple tup) {
  (..., (std::cout << typeid(std::get<Ints>(tup)).name() << " ")) << "\n";
}

template <typename Tuple>
void
Print(Tuple tup) {
  Print(std::make_index_sequence<std::tuple_size<Tuple>::value>(), tup);
}

void
TestGet() {
  auto pull_from_default = galois::get_default_trait_values(
      std::make_tuple(Unrelated{}), std::make_tuple(A{}), std::make_tuple(B{}));
  static_assert(
      std::is_same<decltype(pull_from_default), std::tuple<B>>::value);

  auto no_pull_from_default_when_same = galois::get_default_trait_values(
      std::make_tuple(A{}), std::make_tuple(A{}), std::make_tuple(B{}));
  static_assert(std::is_same<
                decltype(no_pull_from_default_when_same), std::tuple<>>::value);

  auto no_pull_from_default_when_derived = galois::get_default_trait_values(
      std::make_tuple(B{}), std::make_tuple(A{}), std::make_tuple(B{}));
  static_assert(
      std::is_same<
          decltype(no_pull_from_default_when_derived), std::tuple<>>::value);

  auto empty_tuple = galois::get_default_trait_values(
      std::make_tuple(), std::make_tuple(), std::make_tuple());
  static_assert(std::is_same<decltype(empty_tuple), std::tuple<>>::value);

  auto value_from_default = galois::get_default_trait_values(
      std::make_tuple(), std::make_tuple(A{}), std::make_tuple(B{"name"}));
  GALOIS_ASSERT(std::get<0>(value_from_default).name_ == "name");

  auto get_value = galois::get_trait_value<A>(std::tuple<B>(B{"name"}));
  GALOIS_ASSERT(get_value.name_ == "name");
}

struct HasFunctionTraits {
  using function_traits = std::tuple<int>;
};

void
TestHasFunctionTraits() {
  static_assert(galois::has_function_traits_v<HasFunctionTraits>);
  static_assert(std::is_same<
                HasFunctionTraits::function_traits,
                galois::function_traits<HasFunctionTraits>::type>::value);
}

struct Functor {
  int v_;

  int operator()(int) { return v_; }
};

auto
MakePRValueArgument() {
  return galois::wl<galois::worklists::OrderedByIntegerMetric<Functor>>(1);
}

auto
MakeLValueArgument() {
  int v = 2;
  return galois::wl<galois::worklists::OrderedByIntegerMetric<Functor>>(v);
}

void
TestCopy() {
  std::cout << "making prvalue functor\n";
  std::cout << std::get<0>(MakePRValueArgument().args);
  std::cout << "\n";

  // If galois::wl incorrectly stores references to arguments, we will get an
  // uninitialized reference here, which will be detected statically by gcc 7
  // -Werror=uninitialized.
  auto args = MakeLValueArgument().args;
  std::cout << "making lvalue functor\n";
  std::cout << std::get<0>(args);
  std::cout << "\n";

  // For other compilers, directly check the sufficient condition.
  static_assert(!std::is_reference_v<std::tuple_element_t<0, decltype(args)>>);
}

int
main() {
  TestGet();
  TestHasFunctionTraits();
  TestCopy();
  return 0;
}
