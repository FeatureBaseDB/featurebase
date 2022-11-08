ARG FEATUREBASE_URL=https://github.com/FeatureBaseDB/featurebase/releases/download/v1.1.0-community/featurebase-v1.1.0-community-linux-amd64.tar.gz

# builder is responsible for downloads and conforming to naming conventions
FROM alpine:latest as builder
ARG FEATUREBASE_URL

RUN wget -O featurebase.tar.gz ${FEATUREBASE_URL}
RUN tar -zxvf featurebase.tar.gz
RUN mv featurebase-v*/ featurebase/
RUN mv idk-v*/ idk/

###
# runner for featurebase binary
FROM alpine:latest as featurebase
COPY --from=builder /featurebase /usr/local/bin
WORKDIR /home/featurebase

###
# runner for IDK
FROM alpine:latest as ingest
COPY --from=builder /idk /idk
WORKDIR idk

# Alpine tar command seems to leave meta files behind, this removes them
RUN rm \._*
RUN chmod ug+x *

RUN mv /idk/* /usr/local/bin
RUN rm -rf /idk

WORKDIR /home/ingester
