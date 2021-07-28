#include <stdio.h>

// This function prevents unloading of shared libraries, thus the sanitizers
// can always find the address that belonged to a shared library.
// See https://github.com/google/sanitizers/issues/89#issuecomment-484435084
int dlclose(void* ptr) { return 0; }
