ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS base_with_conda

COPY download_miniconda.sh /
COPY activate_miniconda.sh /

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Chicago
ENV APT_GET="apt-get --yes --quiet"
ENV YUM="yum -y"

RUN set -eux; \
    if command -v yum > /dev/null; then \
        ${YUM} update; \
        ${YUM} install curl; \
        ${YUM} clean all; \
    else \
        ${APT_GET} update; \
        ${APT_GET} dist-upgrade; \
        ${APT_GET} install curl; \
        ${APT_GET} clean; \
    fi

RUN set -eu; \
    bash /download_miniconda.sh ubuntu-xxx; \
    . /activate_miniconda.sh;

ARG CONDA_CLEAN="conda clean --quiet --yes --all"

RUN set -eu; \
    . /activate_miniconda.sh; \
    mamba update --quiet --yes --all; \
    ${CONDA_CLEAN}

FROM base_with_conda AS pre_install

COPY packages /packages

FROM pre_install AS test_python
ARG CONDA_CLEAN="conda clean --quiet --yes --all"
ARG MAMBA_INSTALL="mamba install --quiet --yes --channel /packages"

RUN set -eu ; . /activate_miniconda.sh; set -x ; \
    ${MAMBA_INSTALL} katana-python; \
    ${CONDA_CLEAN}

RUN set -eu ; . /activate_miniconda.sh; set -x ; \
    python -c "import katana.analytics; katana.analytics.bfs; print(katana.__version__)"

FROM pre_install AS test_tools
ARG CONDA_CLEAN="conda clean --quiet --yes --all"
ARG MAMBA_INSTALL="mamba install --quiet --yes --channel /packages"

RUN set -eu ; . /activate_miniconda.sh; set -x ; \
    ${MAMBA_INSTALL} katana-tools; \
    ${CONDA_CLEAN}

RUN set -eu ; . /activate_miniconda.sh; set -x ; \
    graph-convert --version
