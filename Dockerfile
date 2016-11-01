FROM golang:1.7
ADD docker-pilosa.conf /etc/pilosa.conf 
ADD docker-setup-run.sh /usr/bin/
ADD . /go/src/github.com/umbel/pilosa/
RUN go install github.com/umbel/pilosa/cmd/pilosa
RUN mkdir /tmp/pil
ENV DCHOSTNAME=SetThisThroughCompose
CMD /usr/bin/docker-setup-run.sh 
