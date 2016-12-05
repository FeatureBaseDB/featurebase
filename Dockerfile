FROM golang:1.7
ENV DATA_DIR /tmp/pil
ENV HOSTNAMEVAR localhost
ENV PORT 15000
ENV REPLICAS 0
ENV NODES 1
ADD docker/docker-entrypoint.sh /usr/bin/entrypoint.sh
ADD . /go/src/github.com/pilosa/pilosa/
RUN go install github.com/pilosa/pilosa/cmd/pilosa
RUN mkdir /tmp/pil
EXPOSE 15000
ENTRYPOINT ["/bin/bash", "/usr/bin/entrypoint.sh"]
CMD ["pilosa", "-config", "/etc/pilosa.conf"]
