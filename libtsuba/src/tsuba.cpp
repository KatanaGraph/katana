#include "tsuba/tsuba.h"

#include "s3.h"

namespace tsuba {

galois::Result<void> Init() { return S3Init(); }
galois::Result<void> Fini() { return S3Fini(); }

} // namespace tsuba
