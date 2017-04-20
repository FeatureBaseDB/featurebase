FROM golang:1.8.0

MAINTAINER Pilosa Corp. <dev@pilosa.com>

ARG ldflags=''
ARG GLIDE="https://github.com/Masterminds/glide/releases/download/v0.12.3/glide-v0.12.3-linux-amd64.tar.gz"
ARG GLIDE_HASH="d6d3816c70fba716466e7381a9c06cb31565a3b87acb5bad9dd3beb0a9f9b0f8"

EXPOSE 10101
VOLUME /data

COPY . /go/src/github.com/pilosa/pilosa

RUN wget ${GLIDE} -O /go/glide.tar.gz -q \
    && tar xf /go/glide.tar.gz \
    && mv /go/linux-amd64/glide /go/bin \
    && [ "$(sha256sum /go/bin/glide | cut -d' ' -f1)" = "$GLIDE_HASH" ] \
    && cd /go/src/github.com/pilosa/pilosa \
    && make vendor \
    && CGO_ENABLED=0 go install -a -ldflags "$ldflags" github.com/pilosa/pilosa/cmd/pilosa \
    && mv /go/bin/pilosa /pilosa \
    && rm -rf /go

ENTRYPOINT ["/pilosa"]
CMD ["server", "--data-dir", "/data"]
