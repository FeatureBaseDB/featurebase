FROM golang:1.10.3 as builder

COPY . /go/src/github.com/pilosa/pilosa/

RUN cd /go/src/github.com/pilosa/pilosa \
    && CGO_ENABLED=0 make install-dep install FLAGS="-a"

FROM scratch

LABEL maintainer "dev@pilosa.com"

COPY --from=builder /go/bin/pilosa /pilosa

COPY LICENSE /LICENSE
COPY NOTICE /NOTICE

EXPOSE 10101
VOLUME /data

ENTRYPOINT ["/pilosa"]
CMD ["server", "--data-dir", "/data", "--bind", "http://0.0.0.0:10101"]
