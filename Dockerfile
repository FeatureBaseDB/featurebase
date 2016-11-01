FROM golang:1.7
ADD docker-pilosa.conf /etc/pilosa.conf 
ADD docker-entrypoint.sh /usr/bin/entrypoint.sh
ADD . /go/src/github.com/umbel/pilosa/
RUN go install github.com/umbel/pilosa/cmd/pilosa
RUN mkdir /tmp/pil
ENTRYPOINT ["/bin/sh", "/usr/bin/entrypoint.sh"]
CMD ["pilosa", "-config", "/etc/pilosa.conf"]
