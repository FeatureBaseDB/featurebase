FROM scratch

MAINTAINER Pilosa Corp. <engineering@pilosa.com>

ENV DATA_DIR /data
EXPOSE 15000
VOLUME /data

COPY ./pilosa /pilosa

ENTRYPOINT ["/pilosa"]
