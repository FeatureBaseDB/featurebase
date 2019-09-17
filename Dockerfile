FROM golang:1.13.0 as builder

COPY . pilosa

RUN cd pilosa && CGO_ENABLED=0 make install FLAGS="-a"

FROM alpine:3.9.4

LABEL maintainer "dev@pilosa.com"

RUN apk add --no-cache curl jq

COPY --from=builder /go/bin/pilosa /pilosa

COPY LICENSE /LICENSE
COPY NOTICE /NOTICE

EXPOSE 10101
VOLUME /data

ENTRYPOINT ["/pilosa"]
CMD ["server", "--data-dir", "/data", "--bind", "http://0.0.0.0:10101"]
