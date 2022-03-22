#include "katana/RDGStorageFormatVersion.h"

/// The feature flag determines the capabilities of the software,
/// while the unstable_storage_format_ flag determines the state of the RDG
/// If "UnstableRDGStorageFormat" is found in the envar "KATANA_ENABLE_EXPERIMENTAL":
///   1) katana software will store RDGs
///     - as "latest_storage_format_version"
///     - with the "unstable_storage_format" part header flag set
///   2) RDGs with the "unstable_storage_format" flag will be allowed to be loaded
/// When "UnstableRDGStorageFormat" is not found:
///   1) RDGs will be stored as the "latest_storage_format_version"
///   2) RDGs with the "unstable_storage_format" flag will not be allowed to be loaded
///
/// This feature flag can be set in the environment:
/// KATANA_ENABLE_EXPERIMENTAL="UnstableRDGStorageFormat"
///
/// This feature flag can be checked by using
/// KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat);
///
KATANA_EXPERIMENTAL_FEATURE(UnstableRDGStorageFormat);
