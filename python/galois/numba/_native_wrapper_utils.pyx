from libc.stdint cimport uint64_t

def call_callback(uint64_t func, uint64_t arg, uint64_t userdata):
    (<void (*)(uint64_t, void*)>func)(arg, <void*>userdata)
