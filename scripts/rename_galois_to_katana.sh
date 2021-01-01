#!/bin/bash

set -xeuo pipefail

MODE=${1:-}

if [[ ${MODE} == "contents" ]]; then
  while read -d '' filename; do
    # Otherwise perl -i will clobber symlinks
    file="$(readlink -e -- "${filename}")"

    # C++ namespaces

    # (?<!...) negative lookback
    perl -p -i \
      -e 's/galois::/katana::/g;' \
      -e 's/namespace\s+galois/namespace katana/g;' \
      \
      -e 's/katana::runtime/katana/g;' \
      -e 's/typename\s+runtime:://g;' \
      -e 's/(?<![\w:])runtime:://g;' \
      -e 's/namespace\s+runtime.*{.*//g;' \
      -e 's/}.*namespace runtime.*//g;' \
      \
      -e 's/katana::worklists/katana/g;' \
      -e 's/typename\s+worklists:://g;' \
      -e 's/(?<![\w:])worklists:://g;' \
      -e 's/namespace\s+worklists.*{.*//g;' \
      -e 's/}.*namespace worklists.*//g;' \
      \
      -e 's/katana::substrate/katana/g;' \
      -e 's/typename\s+substrate:://g;' \
      -e 's/(?<![\w:])substrate:://g;' \
      -e 's/namespace\s+substrate.*{.*//g;' \
      -e 's/}.*namespace substrate.*//g;' \
      \
      -e 's/katana::graphs/katana/g;' \
      -e 's/typename\s+graphs:://g;' \
      -e 's/(?<![\w:])graphs:://g;' \
      -e 's/namespace\s+graphs.*{.*//g;' \
      -e 's/}.*namespace graphs.*//g;' \
      "${file}"

    # C Macros
    perl -p -i -e 's/GALOIS_/KATANA_/g' "${file}"

    # CMake stuff
    perl -p -i \
      -e 's/(?<![:.])galois_/katana_/g;' \
      -e 's/Galois_/Katana_/g;' \
      -e 's/project\(Galois-enterprise\)/project(Katana-enterprise)/g;' \
      -e 's/project\(Galois\)/project(Katana)/g;' \
      -e 's/Galois::/Katana::/g;' \
      -e 's/GaloisTargets/KatanaTargets/g;' \
      -e 's/GaloisConfig/KatanaConfig/g;' \
      -e 's/the Galois package/the Katana package/g;' \
      -e 's/cmake-galois-build/cmake-katana-build/g;' \
      -e 's|cmake/Galois|cmake/Katana|g;' \
      "${file}"

    # Python and build/package support
    perl -p -i \
      -e 's/import galois/import katana/g;' \
      -e 's/from galois/from katana/g;' \
      -e 's/(?<![\w:.])galois\./katana\./g;' \
      -e 's/galois={{/katana={{/g;' \
      -e 's/namespace "galois"/namespace "katana"/g;' \
      -e 's|python/galois|python/katana|g;' \
      -e 's/add_subdirectory\(galois\)/add_subdirectory(katana)/g;' \
      -e 's/find_package\(Galois/find_package(Katana/g;' \
      -e 's/galois-python/katana-python/g;' \
      -e 's/Galois-python/Katana-python/g;' \
      -e 's/galois-dev/katana-dev/g;' \
      -e 's/`galois`/`katana`/g;' \
      -e 's/name="galois"/name="katana"/g;' \
      -e 's/name: galois/name: katana/g;' \
      -e 's/{"galois":/{"katana":/g;' \
      "${file}"

    # RST
    perl -p -i -e 's/automodule:: galois/automodule:: katana/g' "${file}"

    # Include paths
    perl -p -i \
      -e 's|(?<![\w:])galois/|katana/|g;' \
      -e 's|katana/runtime/|katana/|g;' \
      -e 's|katana/worklists/|katana/|g;' \
      -e 's|katana/graphs/|katana/|g;' \
      -e 's|katana/substrate/|katana/|g;' \
      -e 's|"querying/|"katana/query/|g;' \
      "${file}"

    # YAML and MD
    perl -p -i \
      -e 's|/pkgs/galois-|/pkgs/katana-|g;' \
      -e 's/Build galois Package/Build katana Package/g;' \
      -e 's/galois-env/katana-env/g;' \
      -e 's/Galois-Python/Katana-Python/g;' \
      "${file}"

    # After everything else, allow some uses of galois that should remain but
    # are tricky to single out in the above passes
    perl -p -i \
      -e 's/katana_shmem/katana_galois/g;' \
      -e 's/Katana::shmem/Katana::galois/g;' \
      -e 's/SHMEM/GALOIS/g;' \
      -e 's/(?<![\w:])shmem/galois/g;' \
      "${file}"
  done < <(find . -name external -prune -o -name .git -prune -o \( \
    -name 'CMakeLists.txt' \
    -o -name 'Pipfile' \
    -o -name '*.py' \
    -o -name '*.py.in' \
    -o -name '*.pyx' \
    -o -name '*.pxd' \
    -o -name '*.jinja' \
    -o -name '*.ipynb' \
    -o -name '*.go' \
    -o -name 'config-galois.sh' \
    -o -name 'config-katana.sh' \
    -o -name 'setup_dev_macos.sh' \
    -o -name 'configure_cpp.sh' \
    -o -name 'build.sh' \
    -o -name '*.cmake' \
    -o -name '*.cmake.in' \
    -o -name '*.yaml' \
    -o -name '*.md' \
    -o -name '*.dox' \
    -o -name '*.rst' \
    -o -name '*.cpp' \
    -o -name '*.cpp.in' \
    -o -name '*.h' \
    -o -name '*.hh' \
    -o -name '*.cuh' \
    -o -name '*.h.in' \) -a -print0)

  # One-off change to external
  for f in external/azure-storage-cpplite/fix_build.patch; do
    if [[ ! -e ${f} ]]; then
      continue
    fi
    perl -p -i -e 's/GaloisTargets/KatanaTargets/g;' ${f}
  done
fi

if [[ ${MODE} == "manual" ]]; then
  cat <<'EOF' | git apply -
diff --git a/libgalois/include/katana/Allocators.h b/libgalois/include/katana/Allocators.h
index e6496f825..3c0fd5c0a 100644
--- a/libgalois/include/katana/Allocators.h
+++ b/libgalois/include/katana/Allocators.h
@@ -38,6 +38,8 @@
 #include "katana/SimpleLock.h"
 #include "katana/config.h"
 
+// TODO(ddn): Merge with Mem.h. Users should not include this file directly.
+
 namespace katana {
 
 extern unsigned activeThreads;
@@ -657,7 +659,8 @@ public:
 // Now adapt to standard std allocators
 ////////////////////////////////////////////////////////////////////////////////
 
-//! A fixed size block allocator
+//! Scalable fixed-sized allocator for T that conforms to STL allocator
+//! interface but does not support variable sized allocations
 template <typename Ty>
 class FixedSizeAllocator;
 
diff --git a/libgalois/include/katana/Mem.h b/libgalois/include/katana/Mem.h
index 60182d15e..1187d3611 100644
--- a/libgalois/include/katana/Mem.h
+++ b/libgalois/include/katana/Mem.h
@@ -48,11 +48,6 @@ typedef katana::BumpWithMallocHeap<katana::FreeListHeap<katana::SystemHeap>>
 typedef katana::ExternalHeapAllocator<char, IterAllocBaseTy> PerIterAllocTy;
 //! [PerIterAllocTy example]
 
-//! Scalable fixed-sized allocator for T that conforms to STL allocator
-//! interface but does not support variable sized allocations
-template <typename Ty>
-using FixedSizeAllocator = katana::FixedSizeAllocator<Ty>;
-
 //! Scalable variable-sized allocator for T that allocates blocks of sizes in
 //! powers of 2 Useful for small and medium sized allocations, e.g. small or
 //! medium vectors, strings, deques
EOF
fi

if [[ ${MODE} == "files" ]]; then
  ### Include directories
  for f in lib*/include/galois; do
    if [[ ! -e ${f} ]]; then
      # Glob failed
      continue
    fi
    # use rsync so we get consistent behavior even if target directory exists
    rsync -a --remove-source-files ${f}/. $(dirname ${f})/katana
  done

  for f in lib*/include/querying; do
    if [[ ! -e ${f} ]]; then
      continue
    fi

    rsync -a --remove-source-files ${f}/. $(dirname ${f})/katana/query
  done

  for f in cmake/Galois*; do
    if [[ ! -e ${f} ]]; then
      continue
    fi
    mv $f ${f/Galois/Katana}
  done

  # Flatten some directories to parent 
  for dir in worklists runtime graphs substrate; do
    for f in lib*/include/*/${dir}/*; do
      if [[ ! -e ${f} ]]; then
        continue
      fi

      if [[ -d ${f} ]]; then
        rsync -a --remove-source-files ${f}/. $(dirname ${f})/../$(basename ${f})
      else 
        mv ${f} $(dirname ${f})/../$(basename ${f})
      fi
    done
  done

  ### Python files
  for dir in python/docs python; do
    for f in ${dir}/galois*; do
      if [[ ! -e ${f} ]]; then
        continue
      fi
      mv $f ${f/galois/katana}
    done
  done

  for f in python/*/shmem.*; do
    if [[ ! -e ${f} ]]; then
      continue
    fi
    mv $f ${f/shmem/galois}
  done

  ### Docs
  for f in docs/figs/galois*; do
    if [[ ! -e ${f} ]]; then
      continue
    fi
    mv $f ${f/galois/katana}
  done

  ### Scripts
  for f in scripts/docker/msan/config-galois.sh; do
    if [[ ! -e ${f} ]]; then
      continue
    fi
    mv $f ${f/galois/katana}
  done
fi
