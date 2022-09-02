ARG GO_VERSION=1.19

FROM golang:1.19-buster as builder

WORKDIR /
RUN apt-get update -y -qq && apt-get install -y -qq \
  build-essential \
  git \
  musl-tools \
  netcat \
  unixodbc \
  unixodbc-dev \
  && rm -rf /var/lib/apt/lists/*
RUN ["git", "clone", "https://github.com/edenhill/librdkafka.git"]
WORKDIR /librdkafka
RUN ./configure --prefix /usr && \
  make && \
  make install

WORKDIR /featurebase

COPY . .

ARG MAKE_FLAGS
ARG GO_BUILD_FLAGS

WORKDIR /featurebase/idk/

RUN make build GO_BUILD_FLAGS="-mod=vendor ${GO_BUILD_FLAGS}" ${MAKE_FLAGS}

FROM ubuntu:20.04 as runner

RUN apt-get update -y -qq && apt-get install -y -qq \
  ca-certificates \
  musl-tools \
  netcat \
  unixodbc-dev \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /featurebase/idk/bin/* /usr/local/bin/
COPY ./idk/docker-sasl/ssl_keys /etc/kafka/secrets

# Verify that the linker can find everything.
FROM runner as linkcheck
RUN if [ -e /usr/local/bin/molecula-consumer-sql-odbc ] ; then ldd /usr/local/bin/molecula-consumer-sql-odbc; fi

FROM runner
