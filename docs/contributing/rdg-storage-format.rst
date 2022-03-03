=========================
Katana RDG Storage Format
=========================

Katana persists its graphs on disk in a format called `RDG: Resilient Distributed Graph`. This `RDG` is a directory filled with files that define the graph.
The `storage_format_version` of an RDG determines the layout of the RDG
An example of an RDG can be found here
https://github.com/KatanaGraph/test-datasets/tree/master/rdg_datasets/ldbc_003/storage_format_version_3

Katana software uses the `storage_format_version` of a loaded RDG to determine some of the features it supports

Katana software always stores the RDG as the latest `storage_format_version`, sometimes with the `unstable_storage_format` modifier (covered below)
RDGs stored in a newer `storage_format_versions` are incompatible with older software.
All versions of Katana after a `storage_format_version` is introduced should support loading/reading RDGs stores as that `storage_format_version`.

Early RDGs from before `storage_format_version` was introduced are assumed to be `storage_format_version=1`

Rarely, `storage_format_versions` can be deprecated. When this occurs tooling will be provided to convert all RDGs stored in that `storage_format_version` to the next supported `storage_format_version`.

Unstable RDG Storage Format
===========================

Developers should use the unstable storage format while developing features for Katana software which alter, add, or remove, RDG data structures to avoid disturbing everyone else that relies on the RDG format to be stable. 

Katana software executed *with* the `KATANA_ENABLE_EXPERIMENTAL="UnstableRDGStorageFormat"` environment variable supports loading/storing RDGs in the unstable RDG storage format.
Katana software executed *without* the `KATANA_ENABLE_EXPERIMENTAL="UnstableRDGStorageFormat"` environment variable *does not* support loading/storing RDGs in the unstable RDG storage format.

Katana software uses the boolean flag `unstable_storage_format` in the RDGs part header to identify unstable format RDGs

If Katana software is executed *with* the `KATANA_ENABLE_EXPERIMENTAL="UnstableRDGStorageFormat"` environment variable:
#. katana software stores RDGs:
 
   #. with `storage_format_version = latest_storage_format_version`
   #. with `unstable_storage_format = true`

#. RDGs with the "unstable_storage_format" are loaded


If Katana software is executed *without* the `KATANA_ENABLE_EXPERIMENTAL="UnstableRDGStorageFormat"` environment variable:
#. katana software stores RDGs:

   #. with `storage_format_version = latest_storage_format_version`
   #. with `unstable_storage_format = false`

#. RDGs with the "unstable_storage_format" are not loaded, and an error is thrown


In Katana software the feature flag can be checked by using
```
KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat);
```

Developing a new revision of the RDG Storage Format
===================================================

This process will be largely determined by what new data structures are being persisted in the RDG, but the following are some guidelines.

#. Figure out the in-memory representation of the feature. Figure out how the in-memory representation will be generated from when an RDG without the new data structures is loaded.
#. Add support for persisting the in-memory representation on disk in the RDG. Gate persisting these new data structures behind `KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat);`. When `UnstableRDGStorageFormat` is not set, the persisted RDG should look exactly like an RDG without support for the new data structures.
#. Add support for loading the new data structures. Gate loading these new data structures behind `KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat);`. When `UnstableRDGStorageFormat` is not set, the code should behave like it loaded an RDG without the new data structures.
#. Write tests for storing/loading the new feature.
#. Stabilize the features in-memory representation
#. Stabilize the features on-disk representation
#. When the representations are sufficiently stable

   #. increase the `kLatestPartitionStorageFormatVersion` in `RDGStorageFormatVersion.h`.
   #. Mirror this change for `KATANA_RDG_STORAGE_FORMAT_VERSION` in `TestDatasets.cmake`
   #. uprev the rdgs in the `test-datasets` repo, follow the README in https://github.com/KatanaGraph/test-datasets
   #. update the version of `test-datasets` used by Katana
   #. update the `DATASETS_SHA` with the newest master sha from `test-datasets` in `example_data.py`
   #. let the red-team/QA know that the storage format version was bumped, so that they may regenerate their datasets

