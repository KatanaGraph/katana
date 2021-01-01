#include "katana/Traits.h"

#include <iostream>
#include <utility>

#include "katana/gIO.h"

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
  auto pull_from_default = katana::get_default_trait_values(
      std::make_tuple(Unrelated{}), std::make_tuple(A{}), std::make_tuple(B{}));
  static_assert(
      std::is_same<decltype(pull_from_default), std::tuple<B>>::value);

  auto no_pull_from_default_when_same = katana::get_default_trait_values(
      std::make_tuple(A{}), std::make_tuple(A{}), std::make_tuple(B{}));
  static_assert(std::is_same<
                decltype(no_pull_from_default_when_same), std::tuple<>>::value);

  auto no_pull_from_default_when_derived = katana::get_default_trait_values(
      std::make_tuple(B{}), std::make_tuple(A{}), std::make_tuple(B{}));
  static_assert(
      std::is_same<
          decltype(no_pull_from_default_when_derived), std::tuple<>>::value);

  auto empty_tuple = katana::get_default_trait_values(
      std::make_tuple(), std::make_tuple(), std::make_tuple());
  static_assert(std::is_same<decltype(empty_tuple), std::tuple<>>::value);

  auto value_from_default = katana::get_default_trait_values(
      std::make_tuple(), std::make_tuple(A{}), std::make_tuple(B{"name"}));
  KATANA_ASSERT(std::get<0>(value_from_default).name_ == "name");

  auto get_value = katana::get_trait_value<A>(std::tuple<B>(B{"name"}));
  KATANA_ASSERT(get_value.name_ == "name");
}

struct HasFunctionTraits {
  using function_traits = std::tuple<int>;
};

void
TestHasFunctionTraits() {
  static_assert(katana::has_function_traits_v<HasFunctionTraits>);
  static_assert(std::is_same<
                HasFunctionTraits::function_traits,
                katana::function_traits<HasFunctionTraits>::type>::value);
}

struct Functor {
  int v_;

  int operator()(int) { return v_; }
};

auto
MakePRValueArgument() {
  return katana::wl<katana::OrderedByIntegerMetric<Functor>>(1);
}

auto
MakeLValueArgument() {
  int v = 2;
  return katana::wl<katana::OrderedByIntegerMetric<Functor>>(v);
}

void
TestCopy() {
  std::cout << "making prvalue functor\n";
  std::cout << std::get<0>(MakePRValueArgument().args);
  std::cout << "\n";

  // If katana::wl incorrectly stores references to arguments, we will get an
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
