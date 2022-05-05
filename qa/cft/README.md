# How to deploy featurebase cluster with cloudformation?

- first login to your AWS profile for the FeatureBase-CI AWS account and then cd to featurebase repo root directory. here we assume that your profile is called `fb-ci`
```
aws sso login --profile fb-ci
cd <feature base repo root dir>
```
- then run cloudformation deploy. You can customize stuff by overriding the parameters using the `--parameter-overrides` flag. Please see the `FeatureBaseClusterCFT.yaml` for the complete list of parameters that can be customized.
```
aws cloudformation deploy \
--stack-name jon-test-stack \
--template-file qa/cft/FeatureBaseClusterCFTTremor.yaml \
--parameter-overrides \
FBInstanceCount=1 \
ConsumerInstanceCount=1 \
ProducerEBSVolumeSize=2400 \
ProducerEBSIops=3000 \
TestName=some-test-name \
--capabilities CAPABILITY_NAMED_IAM \
--profile fb-ci
```

## How to manually ssh to the FeatureBase Cluster, Consumer, or Producer nodes?

You will need to setup the aws profile with the credentials to access the FeatureBase-CI AWS account in the job, and then call the above cloudformation command in the job.

After deploying you will need to get one of the ips of the featurebase nodes for ssh. The script `getASGInstanceIPs.sh` takes 2 arguments:
- auto scaling group name, which is 
  - for featurebase nodes: `${STACK_NAME}-asg`
  - for producer nodes: `${STACK_NAME}-producer-asg` (given our current architecture, we can only have 1 producer node, which also serves as the kafka host)
  - for consumer nodes: `${STACK_NAME}-consumer-asg`
- aws profile name

The output of the script is the list of ips of the instances in the auto scaling group
```
.qa/scriptsgetASGInstanceIPs.sh jon-test-stack2-asg fb-ci
```
Now you will need to use VPN to ssh to one of the nodes. To setup VPN, follow this [link](https://molecula.atlassian.net/wiki/spaces/EN/pages/697892918/How+to+Setup+VPN+Acccess+to+MCloud).
After setting up the VPN, you can then ssh to the ec2 instance with its private IPv4 address
```
ssh -i <the "gitlab-featurebase-ci AWS key" pem file in 1Password> ec2-user@<the private IPv4 address of the ec2 instance>
```

## How to setup producer (& kafka) and consumers?
go to the top level directory of this featurebase repo
run the following command (assuming you have already run the previous cloudformation command)
- arguments for setupFeatureBaseClusterCFTTremor.sh:
  1) stack name
  2) aws profile name
  3) feature base replica count
  4) feature base instance count
  5) datadog tag for the ticket
example use of the script:
```
./qa/scripts/setupFeatureBaseClusterCFTTremor.sh some-test-stack fb-ci 3 5 some-datadog-tag
```



## How to restore data from the ebs volume?
As of now 4/25/2022, i made an ebs volume (vol-0f9af1a7bc5dd63e0, tremor-integration-test-restore-records) in *us-east-2a* and backed up ~1B tremor records in the /tremor_backup directory. FYI, you cannot attach this ebs volume outside of us-east-2a.

After setting up featurebase, and ensuring that there is a featurebase data node in the us-east-2a AZ, then we can start the following process to restore those billions of tremor records stored in ebs volume to featurebase for testing.
- First, attach the ebs volume to the us-east-2a featurebase data node. Note that this ebs volume must be available (not attached to any other instances) for the following command to work.
```
aws ec2 attach-volume --instance-id <instance in us-east-2a>  --volume-id vol-0f9af1a7bc5dd63e0 --device /dev/sdf --region us-east-2 --profile fb-ci
```
- then, ssh to the featurebase data node and mount the ebs volume to the directory /restore, like so:
```
sudo mkdir /restore
sudo mount /dev/sdf /restore
sudo chown -R ec2-user:ec2-user /restore
```
- now we can perform the featurebase restore, this may take ~20min for 1B tremor records
```
featurebase restore --concurrency 64 --source /restore/tremor_backup
```
- after restore, please unmount the ebs device 
```
sudo umount /restore
```
- Exit your ssh session, and then Please detach the ebs volume, so that other processes can use it!!!
```
aws ec2 detach-volume --instance-id i-015fca53e6a50c5bf --volume-id vol-0f9af1a7bc5dd63e0 --region us-east-2 --profile fb-ci
```

## How to destroy old backup and create new backup data to this ebs volume
In the future, we may want to remove the obsolete back up data and store a new dataset in the ebs volume, we can do the following steps:
- First, attach the ebs volume to the us-east-2a featurebase data node. Note that this ebs volume must be available (not attached to any other instances) for the following command to work.
```
aws ec2 attach-volume --instance-id <instance in us-east-2a>  --volume-id vol-0f9af1a7bc5dd63e0 --device /dev/sdf --region us-east-2 --profile fb-ci
```
- then, ssh to the featurebase data node and mount the ebs volume to the directory /restore, like so: (note that the `mkfs.ext4` command will destroy all data in the ebs volume!)
```
sudo mkdir /restore
sudo mkfs.ext4 /dev/sdf
sudo mount /dev/sdf /restore
sudo chown -R ec2-user:ec2-user /restore
```
- now we can perform the featurebase backup, this may take ~56min for 1B tremor records & a 3 node cluster
```
featurebase backup --index tremor -o /restore/tremor-backup --concurrency 64 
```
- after backup, please unmount the ebs device 
```
sudo umount /restore
```
- Exit your ssh session, and then Please detach the ebs volume, so that other processes can use it!!!
```
aws ec2 detach-volume --instance-id i-015fca53e6a50c5bf --volume-id vol-0f9af1a7bc5dd63e0 --region us-east-2 --profile fb-ci
```

## How to launch multiple consumers?
You can just specify ConsumerInstanceCount to be 2 when you run cloudformation deploy, like so:
```
aws cloudformation deploy \
--stack-name jon-test-stack \
--template-file qa/cft/FeatureBaseClusterCFTTremor.yaml \
--parameter-overrides \
FBInstanceCount=1 \
ConsumerInstanceCount=1 \
ProducerEBSVolumeSize=2400 \
ProducerEBSIops=3000 \
TestName=some-test-name \
--capabilities CAPABILITY_NAMED_IAM \
--profile fb-ci
```
The setup scripts are written to support multiple consumers, so you can just run the same setup script as mentioned previously.
But the performance is not better than just using 1 consumer as of now, so some fine tuning is required i think...

## Other things to be mindful of:
- If you are creating multiple featurebase clusters to parallelize testing, make sure you give them different stack names!
- Note that this cloudformation template FeatureBaseClusterCFT.yaml is still using the same smoke vpc and subnets.


