#!/bin/bash -ex
# generate log 
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Install packages 
yum update -y
# yum install golang -y # latest verion in ec2 is 1.15.14 
yum install postgresql -y

# Configure host system  
echo 'cat /proc/sys/fs/file-max'
sysctl -w fs.file-max=262144
sysctl -p
echo 'cat /proc/sys/fs/file-max'

# install go 1.16.9
curl -O https://dl.google.com/go/go1.16.9.linux-amd64.tar.gz
tar xvf go1.16.9.linux-amd64.tar.gz
chown -R root:root ./go
mv go /usr/local
echo "export PATH=$PATH:/usr/local/go/bin" | sudo tee -a /etc/profile > /dev/null
source /etc/profile
