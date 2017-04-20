FROM scratch

MAINTAINER Pilosa Corp. <dev@pilosa.com>

EXPOSE 10101
VOLUME /data

COPY pilosa /pilosa

ENTRYPOINT ["/pilosa"]
CMD ["server", "--data-dir", "/data"]
