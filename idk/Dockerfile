ARG GO_VERSION=1.17.8

FROM golang:1.17.8-stretch as builder

WORKDIR /
RUN ["apt-get","update","-y"]
RUN ["apt-get","install","-y","git","unixodbc","unixodbc-dev","netcat", "build-essential","musl-tools"]
RUN ["git", "clone", "https://github.com/edenhill/librdkafka.git"]
WORKDIR /librdkafka
RUN ["./configure", "--prefix", "/usr"]
RUN ["make"]
RUN ["make", "install"]

WORKDIR /featurebase

COPY . .

ARG MAKE_FLAGS
ARG GO_BUILD_FLAGS

WORKDIR /featurebase/idk/

RUN make build GO_BUILD_FLAGS="-mod=vendor ${GO_BUILD_FLAGS}" ${MAKE_FLAGS}

FROM ubuntu:20.04 as runner

RUN ["apt-get","update","-y"]
RUN ["apt-get","install","-y","unixodbc-dev","netcat","ca-certificates", "musl-tools"]

COPY --from=builder /idk/bin/* /usr/local/bin/

COPY /docker-sasl/ssl_keys /etc/kafka/secrets

# Verify that the linker can find everything.
FROM runner as linkcheck
RUN if [ -e /usr/local/bin/molecula-consumer-sql-odbc ] ; then ldd /usr/local/bin/molecula-consumer-sql-odbc; fi

FROM runner
