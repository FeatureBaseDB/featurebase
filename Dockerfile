FROM golang:1.9.2 as builder

ARG ldflags=''

COPY . /go/src/github.com/pilosa/pilosa

RUN cd /go/src/github.com/pilosa/pilosa \
    && make vendor \
    && CGO_ENABLED=0 go install -a -ldflags "$ldflags" github.com/pilosa/pilosa/cmd/pilosa

FROM scratch

LABEL maintainer "dev@pilosa.com"

COPY --from=builder /go/bin/pilosa /pilosa

EXPOSE 10101
VOLUME /data

ENTRYPOINT ["/pilosa"]
CMD ["server", "--data-dir", "/data", "--bind", "http://0.0.0.0:10101"]
