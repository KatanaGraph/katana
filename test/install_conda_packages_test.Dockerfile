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

ARG CHANNELS

ARG CONDA_CLEAN="conda clean --quiet --yes --all"
# TODO(ddn): --no-channel-priority disables the the effect of "conda config
# --set channel_priority strict" in activate_miniconda.sh. Strict priority (aka
# --strict-channel-priority) will be the default in Conda 5.0 and avoids
# dependency errors when pulling libraries from incompatible channels;
# unfortunately, strict priority renders some package/channel sets
# unsatisfiable.
ARG MAMBA_INSTALL="mamba install --quiet --yes --override-channels --no-channel-priority --channel /packages ${CHANNELS}"
ARG PYTHON_PACKAGE="katana-python"

RUN set -eu ; . /activate_miniconda.sh; set -x ; \
    ${MAMBA_INSTALL} ${PYTHON_PACKAGE}; \
    ${CONDA_CLEAN}

RUN set -eu ; . /activate_miniconda.sh; set -x ; \
    python -c "import katana.local.analytics; katana.local.analytics.bfs; print(katana.__version__)"

FROM pre_install AS test_tools
ARG CHANNELS

ARG CONDA_CLEAN="conda clean --quiet --yes --all"
ARG MAMBA_INSTALL="mamba install --quiet --yes --override-channels --no-channel-priority --channel /packages ${CHANNELS}"
ARG TOOLS_PACKAGE="katana-tools"
ARG TOOLS_TEST_COMMAND="graph-convert --version"

RUN set -eu ; . /activate_miniconda.sh; set -x ; \
    ${MAMBA_INSTALL} ${TOOLS_PACKAGE}; \
    ${CONDA_CLEAN}

RUN set -eu ; . /activate_miniconda.sh; set -x ; \
    ${TOOLS_TEST_COMMAND}
