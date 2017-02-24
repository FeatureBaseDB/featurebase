FROM golang:1.7.5

MAINTAINER Pilosa Corp. <dev@pilosa.com>

EXPOSE 15000
VOLUME /data

RUN echo 'data-dir = "/data"' > /config

COPY . /go/src/github.com/pilosa/pilosa
RUN CGO_ENABLED=0 go install -a github.com/pilosa/pilosa/cmd/pilosa

ENTRYPOINT ["/go/bin/pilosa"]
CMD ["-config", "/config"]
