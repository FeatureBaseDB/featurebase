#!/bin/bash -ex
# generate log 
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Install packages 
yum update -y
yum install postgresql -y

# Configure host system  
echo 'cat /proc/sys/fs/file-max'
sysctl -w fs.file-max=262144
sysctl -p
echo 'cat /proc/sys/fs/file-max'

# yum install golang -y # latest verion in ec2 is 1.15.14 
# install go 1.16.9 manually
curl -O https://dl.google.com/go/go1.16.9.linux-amd64.tar.gz
tar xvf go1.16.9.linux-amd64.tar.gz
chown -R root:root ./go
mv go /usr/local
echo "export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/home/ec2-user/.local/bin:/home/ec2-user/bin:/usr/local/go/bin" | tee -a /etc/profile.d > /dev/null
source /etc/profile.d
