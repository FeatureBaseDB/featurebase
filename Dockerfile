FROM golang:latest as builder

ARG BUILD_FLAGS
ARG MAKE_FLAGS

COPY . pilosa

RUN cd pilosa && make install FLAGS="-a -mod=vendor ${BUILD_FLAGS}" ${MAKE_FLAGS}

FROM alpine:3.12.1

LABEL maintainer "dev@pilosa.com"

RUN apk add --no-cache curl jq

COPY --from=builder /go/bin/pilosa /pilosa

COPY LICENSE /LICENSE
COPY NOTICE /NOTICE

EXPOSE 10101
VOLUME /data

ENV PILOSA_DATA_DIR /data
ENV PILOSA_BIND 0.0.0.0:10101
ENV PILOSA_BIND_GRPC 0.0.0.0:20101

ENTRYPOINT ["/pilosa"]
CMD ["server"]
