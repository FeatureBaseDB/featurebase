#!/bin/bash 
set +x # get debug log
set -e # exit on errors 

echo "----------------------------"
echo "Running delete_cloud_init.sh"

# create and initialize a raid0 array 
# for m5ad.16xlarge instance type, there are 4 NVMe SSDs. 
# Note JALLISON 20221214 - we are now using m6i instance types for cost savings
sudo mdadm --create --verbose /dev/md0 --level=0 --raid-devices=4 /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1

# format filesystem 
sudo mkfs.ext4 /dev/md0

# create a mount directory & mount the raid drive to this directory
sudo mkdir data
sudo mount /dev/md0 /data

# change permissions from root to ec2-user for data directory 
sudo chown -R ec2-user:ec2-user /data