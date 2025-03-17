# syntax = docker/dockerfile:1.2
FROM prefecthq/prefect:2.14.21-python3.10 as base

ARG PYPI_URL
ARG PYPI_USERNAME

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=UTC

ENV SKLEARN_ALLOW_DEPRECATED_SKLEARN_PACKAGE_INSTALL=True

RUN apt-get update \
    && apt-get install -y make gcc curl python3-pip tzdata git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV POETRY_HOME=/opt/poetry \
    PATH="/root/.local/bin:/opt/poetry/bin:$PATH"

RUN curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.6.1 python3 - \
    && poetry config virtualenvs.create false \
    && rm -rf /root/.cache/pypoetry

RUN --mount=type=secret,id=PYPI_PASSWORD,uid=1000 \
    poetry config repositories.df-pypi ${PYPI_URL} \
    && poetry config http-basic.df-pypi ${PYPI_USERNAME} `cat /run/secrets/PYPI_PASSWORD` 

COPY . /src
WORKDIR /src

FROM base as release
RUN make install

FROM release as test
RUN make install/test

