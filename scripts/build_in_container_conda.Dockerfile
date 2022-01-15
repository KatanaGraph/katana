ARG DEBIAN_VERSION=buster-slim
FROM debian:${DEBIAN_VERSION}

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=America/Chicago \
    APT_GET="apt-get --yes --quiet"
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN set -eux; \
    ${APT_GET} update; \
    ${APT_GET} install curl bzip2 ca-certificates git libnuma1 libxml2 texlive-font-utils wget; \
    ${APT_GET} clean

ENV CONDA_INSTALL=/opt/conda
ARG MAMBAFORGE_VERSION="4.11.0-0"
ARG MAMBAFORGE_PLATFORM=Linux-x86_64

RUN set -eux; \
    curl --location --output /tmp/mambaforge.sh "https://github.com/conda-forge/miniforge/releases/download/${MAMBAFORGE_VERSION}/Mambaforge-${MAMBAFORGE_VERSION}-${MAMBAFORGE_PLATFORM}.sh"; \
    echo "49268ee30d4418be4de852dda3aa4387f8c95b55a76f43fb1af68dcbf8b205c3  /tmp/mambaforge.sh" | sha256sum -c 

RUN set -eux; \
    bash -x /tmp/mambaforge.sh -b -p $CONDA_INSTALL; \
    rm /tmp/mambaforge.sh; \
    ln -s $CONDA_INSTALL/etc/profile.d/conda.sh /etc/profile.d/conda.sh; \
    echo "conda activate base" >> /etc/profile.d/z-conda-activate.sh; \
    $CONDA_INSTALL/bin/conda config --set channel_priority strict; \
    $CONDA_INSTALL/bin/mamba update --quiet --yes --name base --all; \
    $CONDA_INSTALL/bin/mamba install --quiet --yes --name base python=3.8; \
    $CONDA_INSTALL/bin/conda clean --quiet --all --force-pkgs-dirs --yes

ENV PATH="$CONDA_INSTALL/bin:${PATH}"

# The * is a misuse of globs that makes open_environment.yml optional. The glob
# will match nothing if the file is missing.
COPY environment.yml open_environment.yml* ctx/

RUN set -eux; \
    [ -f ctx/open_environment.yml ] && \
        mamba env update --quiet --name base --file ctx/open_environment.yml; \
    mamba env update --quiet --name base --file ctx/environment.yml; \
    rm -rf ctx; \
    conda clean --quiet --all --force-pkgs-dirs --yes

VOLUME /source /build
WORKDIR /build

ENTRYPOINT ["bash", "-l", "-c", "\"$@\"", "\"$0\""]

ENV CMAKE_SETTINGS="-DKATANA_LANG_BINDINGS=python -DBUILD_DOCS=internal" \
    CMAKE_BUILD_TYPE="Release" \
    CMAKE_GENERATOR="Unix Makefiles"

CMD set -eux; \
    cmake -G "${CMAKE_GENERATOR}" -B /build -S /source -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ${CMAKE_SETTINGS}; \
    cmake --build /build --parallel $(nproc) ${TARGETS:+--target ${TARGETS}}
