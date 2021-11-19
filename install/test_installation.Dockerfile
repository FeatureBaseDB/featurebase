FROM fedora:rawhide

RUN yum -y install systemd procps

ARG release_tarball

COPY $release_tarball .

COPY test_installation.sh test_installation.sh

RUN mkdir install && \
    tar -xf $release_tarball -C install --strip-components 1 && \
    cd install && \
    cp featurebase /usr/local/bin/featurebase && \
    cp featurebase.redhat.service /etc/systemd/system/featurebase.service && \
    cp featurebase.conf /etc/featurebase.conf

CMD ["/bin/bash", "test_installation.sh"]
