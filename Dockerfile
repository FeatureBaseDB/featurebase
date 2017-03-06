FROM golang:1.7.5

MAINTAINER Pilosa Corp. <dev@pilosa.com>

EXPOSE 15000
VOLUME /data

RUN echo 'data-dir = "/data"' > /config

RUN git clone --depth 1 https://github.com/Masterminds/glide.git /go/src/github.com/Masterminds/glide \
    && cd /go/src/github.com/Masterminds/glide \
    && git fetch --tags --depth 1 \
    && git checkout tags/v0.12.3 -b build \
    && make build \
    && mv ./glide /go/bin \
    && cd / \
    && rm -r /go/src/github.com/Masterminds/glide

COPY . /go/src/github.com/pilosa/pilosa

RUN cd /go/src/github.com/pilosa/pilosa \
    && make vendor \
    && CGO_ENABLED=0 go install -a github.com/pilosa/pilosa/cmd/pilosa

ENTRYPOINT ["/go/bin/pilosa"]
CMD ["-config", "/config"]
