#ifndef KATANA_LIBSUPPORT_KATANA_COMPILETIMEINTROSPECTION_H_
#define KATANA_LIBSUPPORT_KATANA_COMPILETIMEINTROSPECTION_H_

#include <type_traits>

namespace katana {
// Implementation taken from the Walter Brown's Detection proposal.
struct nonesuch {
  ~nonesuch() = delete;
  nonesuch(nonesuch const&) = delete;
  void operator=(nonesuch const&) = delete;
};

namespace detail {
template <
    class Default, class AlwaysVoid, template <class...> class Op,
    class... Args>
struct detector {
  using value_t = std::false_type;
  using type = Default;
};

template <class Default, template <class...> class Op, class... Args>
struct detector<Default, std::void_t<Op<Args...>>, Op, Args...> {
  using value_t = std::true_type;
  using type = Op<Args...>;
};

}  // namespace detail

/*
This is the C++ detection idiom that will eventually be included in the standard library.
It can be useful if you need to detect the presense of a class method at compile time.

For example, if we have 2 structs:

struct Foo {
    void foo(int) {}
};

struct Bar {
    void bar() {}
};

And we want to detect if they implement the foo(int) method, we will first define the following
type template:

template <typename T>
using has_foo_t = decltype(std::declval<T>().foo(int()));

And then use it as follows:

static_assert(is_detected_v<has_foo_t, Foo>, "Foo has foo(int)");

*/

template <template <class...> class Op, class... Args>
using is_detected =
    typename detail::detector<nonesuch, void, Op, Args...>::value_t;

template <template <class...> class Op, class... Args>
constexpr inline bool is_detected_v = is_detected<Op, Args...>::value;

/// A type variable dependent value that is always false.
/// This can be passed to static assert to trigger failure only a template is
/// fully instantiated.
template <typename... T>
constexpr bool always_false = false;

}  // namespace katana

#endif
