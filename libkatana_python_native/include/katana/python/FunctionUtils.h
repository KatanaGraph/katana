#ifndef KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_FUNCTIONUTILS_H_
#define KATANA_LIBKATANAPYTHONNATIVE_KATANA_PYTHON_FUNCTIONUTILS_H_

#include <katana/python/NumbaSupport.h>

namespace katana {

template <auto FuncA, auto FuncB, typename Self, typename... Args>
struct SubObjectCallImpl {
  static auto Func(Self self, Args... args) {
    return std::invoke(FuncB, std::invoke(FuncA, self), args...);
  }
};

template <
    auto FuncA, auto FuncB, typename Return, typename Self,
    typename Intermediate, typename... Args>
auto
SubObjectCallInferer(
    Intermediate (Self::*)(), Return (Intermediate::*)(Args...)) {
  return SubObjectCallImpl<FuncA, FuncB, Self*, Args...>{};
}

template <
    auto FuncA, auto FuncB, typename Return, typename Self,
    typename Intermediate, typename... Args>
auto
SubObjectCallInferer(
    Intermediate (Self::*)() const,
    Return (katana::detail::remove_cvref_t<Intermediate>::*)(Args...) const) {
  return SubObjectCallImpl<FuncA, FuncB, Self*, Args...>{};
}

/// @c SubObjectCall<A, B>::Func is a static function which is roughly
/// equivalent to:
/// @code [](self, args...) { self.(*A)().(*B)(args...) } @endcode
/// This handles both static function pointers. taking self as the first
/// argument and member function pointers.
template <auto FuncA, auto FuncB>
using SubObjectCall =
    decltype(SubObjectCallInferer<FuncA, FuncB>(FuncA, FuncB));

}  // namespace katana

#endif
