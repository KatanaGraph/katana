#ifndef KATANA_LIBSUPPORT_KATANA_BITMATH_H_
#define KATANA_LIBSUPPORT_KATANA_BITMATH_H_

#include <type_traits>
namespace katana {

template <typename T, std::enable_if_t<std::is_integral<T>::value, bool> = true>
constexpr bool
IsPowerOf2(T val) {
  return (val != 0) && ((val & (val - 1)) == 0);
}

// Round \param val up to the next multiple of sizeof \param U
template <
    typename T, typename U,
    std::enable_if_t<std::is_integral<U>::value, bool> = true>
constexpr U
AlignUp(U val) {
  if constexpr (IsPowerOf2(sizeof(T))) {
    return (val + (sizeof(T) - 1)) & ~(sizeof(T) - 1);
  }
  U mod = val % sizeof(T);
  return mod == 0 ? val : val + (sizeof(T) - mod);
}

// Round \param val down to the next multiple of sizeof \param U
template <
    typename T, typename U,
    std::enable_if_t<std::is_integral<U>::value, bool> = true>
constexpr U
AlignDown(U val) {
  if constexpr (IsPowerOf2(sizeof(T))) {
    return val & ~(sizeof(T) - 1);
  }
  U mod = val % sizeof(T);
  return mod == 0 ? val : val - mod;
}

}  // namespace katana

#endif
