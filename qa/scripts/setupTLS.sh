#!/bin/bash

NODEIP=$1

if [[ $NODEIP = "" ]]; then
    echo "usage: "
    echo "./setupTLS.sh <ip address>"
    exit 1
fi

ifErr() {
    res=$?
    if (( res != 0 )); then
        echo "error: $1"
        exit $res
    fi
}

echo "installing wget"
sudo yum install wget -y
ifErr "installing wget"

echo "installing nss-tools"
sudo yum install nss-tools -y
ifErr "installing nss-tools"

echo "installing git"
sudo yum install git -y
ifErr "installing git"

sudo wget -q https://go.dev/dl/go1.19.linux-arm64.tar.gz
ifErr "downloading golang"

sudo tar -C /usr/local -xzf go1.19.linux-arm64.tar.gz
ifErr "unpacking golang"

echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bashrc
ifErr "adding go to path"
export PATH=$PATH:/usr/local/go/bin
ifErr "setting PATH"

echo $PATH
ifErr "echoing path"

go version
if (( $? != 0 )); then
    echo "go not installed!"
    sudo yum install golang -y
    ifErr "golang not installing! >:("
fi

echo "setting up certs"
cd ~

sudo yum install nss-tools
ifErr "installing nss-tools"

git clone https://github.com/FiloSottile/mkcert && cd mkcert
ifErr "cloning mkcert"

go build -ldflags "-X main.Version=$(git describe --tags)"
ifErr "building mkcert"

sudo ./mkcert -install
ifErr "installing root CA"

sudo ./mkcert $NODEIP
ifErr "creating cert for $NODEIP"

sudo mv $NODEIP.pem /data/cert.crt
ifErr "moving cert"

sudo mv $NODEIP-key.pem /data/key.key
ifErr "moving key"

sudo chown molecula /data/key.key
ifErr "chown-ing /data/key.key"

sudo chown molecula /data/cert.crt
ifErr "chown-ing /data/cert.crt"

echo "setting up fake IDP"
cd /etc/fakeidp

go build .
ifErr "building fakeidp server"

echo "starting fakeidp"
nohup ./fakeidp > /dev/null 2>&1 &
