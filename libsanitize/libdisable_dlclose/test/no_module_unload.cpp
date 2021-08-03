#include <dlfcn.h>

#include <iostream>

#include <gnu/lib-names.h>

int
main() {
  const char* const module_name = LIBBROKENLOCALE_SO;
  void* handle = dlopen(module_name, RTLD_LAZY);
  if (handle == nullptr) {
    std::cerr << "Couldn't load " << module_name << std::endl;
    return 1;
  }
  char response[1 << 12];
  int res1 = dlinfo(handle, RTLD_DI_ORIGIN, response);
  if (res1 != 0) {
    std::cerr << "Couldn't dlinfo() the loaded module " << module_name
              << " at address " << handle << std::endl;
    return 2;
  }
  int res2 = dlclose(handle);
  if (res2 != 0) {
    std::cerr << "Couldn't dlclose() the module " << module_name
              << " at address " << handle << std::endl;
    return 3;
  }

  // Finally, test that dlclose() is disabled
  response[0] = 0;
  // Note that "use after free" error here should not be fixed: it means that
  // the .so has been unloaded, and that must be fixed in turn.
  int res3 = dlinfo(handle, RTLD_DI_ORIGIN, response);
  if (res3 != 0) {
    std::cerr << "The module " << module_name
              << " seems to have been unloaded by dlclose(), i.e. the latter "
                 "is not disabled."
              << std::endl;
    return 4;
  }

  std::cout << "OK: The module " << module_name
            << " is still loaded: " << response << std::endl;
  return 0;
}
