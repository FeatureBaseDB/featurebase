ARG GO_VERSION=latest

#######################
### Lattice builder ###
#######################

FROM ghcr.io/featurebasedb/nodejs:0.0.1 as lattice-builder
WORKDIR /lattice

COPY lattice/package.json ./
COPY lattice/yarn.lock ./
RUN yarn install

COPY lattice ./
RUN yarn build

######################
### Pilosa builder ###
######################

FROM golang:${GO_VERSION} as pilosa-builder
ARG MAKE_FLAGS
ARG SOURCE_DATE_EPOCH

WORKDIR /pilosa

RUN go install github.com/rakyll/statik@v0.1.7

COPY . ./
COPY --from=lattice-builder /lattice/build /lattice
RUN /go/bin/statik -src=/lattice -dest=/pilosa

ENV SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH
RUN make build FLAGS="-o build/featurebase" ${MAKE_FLAGS}

#####################
### Pilosa runner ###
#####################

FROM alpine:3.13.2 as runner

LABEL maintainer "dev@molecula.com"

RUN apk add --no-cache curl jq tree

COPY --from=pilosa-builder /pilosa/build/featurebase /

COPY NOTICE /NOTICE

EXPOSE 10101
VOLUME /data

ENV PILOSA_DATA_DIR /data
ENV PILOSA_BIND 0.0.0.0:10101
ENV PILOSA_BIND_GRPC 0.0.0.0:20101

ENTRYPOINT ["/featurebase"]
CMD ["server"]
