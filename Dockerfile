# FROM golang:1.7
# ADD docker-pilosa.conf /etc/pilosa.conf
# ADD ./pilosa /usr/bin/pilosa
# RUN mkdir /tmp/pil
# ARG dchostname
# RUN sed -i -e s/ZZZ/$dchostname/ /etc/pilosa.conf
# CMD /usr/bin/pilosa -config /etc/pilosa.conf

FROM golang:1.7
ADD docker-pilosa.conf /etc/pilosa.conf
ADD . /go/src/github.com/umbel/pilosa/
RUN mkdir /tmp/pil
ARG dchostname
RUN sed -i -e s/ZZZ/$dchostname/ /etc/pilosa.conf
RUN go install github.com/umbel/pilosa/cmd/pilosa
CMD pilosa -config /etc/pilosa.conf
