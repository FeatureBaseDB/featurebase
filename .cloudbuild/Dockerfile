FROM alpine:3.14.2

LABEL maintainer "dev@molecula.com"
LABEL org.opencontainers.image.authors="dev@molecula.com"

RUN apk add --no-cache curl jq

COPY NOTICE /NOTICE
ARG SHORT_SHA

COPY .cloudbuild/featurebase_linux_amd64 /

RUN mv /featurebase_linux_amd64 /pilosa
RUN chmod ugo+x /pilosa
EXPOSE 10101
VOLUME /data

ENV PILOSA_DATA_DIR /data
ENV PILOSA_BIND 0.0.0.0:10101
ENV PILOSA_BIND_GRPC 0.0.0.0:20101

ENTRYPOINT ["/pilosa"]
CMD ["server"]
