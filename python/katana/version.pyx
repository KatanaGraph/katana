from .cpp.libgalois.Galois cimport getVersion, getRevision, getVersionMajor, getVersionMinor, getVersionPatch, getCopyrightYear

def get_katana_version():
    return getVersion().decode()

def get_katana_revision():
    return getRevision().decode()

def get_katana_version_major():
    return getVersionMajor()

def get_katana_version_minor():
    return getVersionMajor()

def get_katana_version_patch():
    return getVersionPatch()

def get_katana_copyright_year():
    return getCopyrightYear()
