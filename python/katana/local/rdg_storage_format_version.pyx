from katana.cpp.libtsuba cimport RDGStorageFormatVersion


def get_latest_storage_format_version():
    return RDGStorageFormatVersion.kLatestPartitionStorageFormatVersion
