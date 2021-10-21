FROM alpine:3.14.2

LABEL maintainer "dev@molecula.com"
LABEL org.opencontainers.image.authors="dev@molecula.com"

WORKDIR /featurebase

RUN apk add --no-cache curl jq
RUN pwd
RUN ls .

COPY LICENSE .
COPY NOTICE .
COPY .gitlab/featurebase_linux_amd64 .

ARG SHORT_SHA

# RUN mv /featurebase_linux_amd64 /featurebase
RUN chmod ugo+x .
EXPOSE 10101
VOLUME /data

ENV PILOSA_DATA_DIR /data
ENV PILOSA_BIND 0.0.0.0:10101
ENV PILOSA_BIND_GRPC 0.0.0.0:20101

ENTRYPOINT ["/featurebase"]
CMD ["server"]
