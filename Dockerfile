FROM golang:1.10.2 as builder

COPY . /go/src/github.com/pilosa/pilosa/

RUN cd /go/src/github.com/pilosa/pilosa \
    && CGO_ENABLED=0 make install-dep install-statik install FLAGS="-a"

FROM scratch

LABEL maintainer "dev@pilosa.com"

COPY --from=builder /go/bin/pilosa /pilosa

COPY NOTICE /NOTICE

EXPOSE 10101
VOLUME /data

ENTRYPOINT ["/pilosa"]
CMD ["server", "--data-dir", "/data", "--bind", "http://0.0.0.0:10101"]
