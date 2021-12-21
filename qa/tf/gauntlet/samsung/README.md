With terraform installed (`brew install terraform` if not)...

You can do `terraform plan` -> `terraform apply` to spin up a cluster, `terraform destroy` to tear one down.

## Other prerequisites:
Please read these carefully.

Be in the `tf` directory (e.g., when you try to run a `terraform` command, the output of `pwd` should be `.../molecula/featurebase/qa/tf`)

Currently, the path to the terraform module is using a local reference, i.e., in `main.tf`, the source line is assuming that you have `molecula-terraform` project installed locally, such that the `molecular-terraform` project and `featurebase` have the same parent directory (e.g., `...A/featurebase/qa/tf` and `...A/molecular-terraform/aws/.modules/featurebase-cluster` should both be valid paths).

In addition, you must currently have a local copy of the `fb901` branch for the `molecular-terraform` project (located in the previously specified directory).

Last thing, there is a key that is currently in 1Password (in the `Shared` vault, called `gitlab-featurebase-ci AWS key`) that must be in `~/.ssh/`, `chmod 400`, named `gitlab-featurebase-ci.pem`. You need this key to SSH to these instances. Assuming an `~/.ssh/config` like the following (append to the top of yours)
```
Host test_*
  User ec2-user
  IdentityFile ~/.ssh/gitlab-featurebase-ci.pem
Host test_ingest
  HostName 3.143.237.165
Host test_node
  HostName 10.0.1.142
  ProxyJump test_ingest
```
except with the `test_ingest`'s `HostName` being the public, `ingest_ips` output from `terraform output` and `test_node`'s `HostName` being one of the private, `data_node_ips` output from `terraform output`. (Hopefully the rationale to use the ssh config to do the jumping like this makes sense; you can do `ssh test_ingest` or `ssh test_node` with minimal further fiddling.)

OR specify cert to us directly thus:

`ssh -A -i ~/.ssh/gitlab-featurebase-ci.pem ec2-user@ip_address`

-A is used to ensure key forwarding.

### TODOs
* We need a `user-data.sh` script which sets up/installs featurebase (possibly installs go, most likely pulls the artifacts from GitLab; sets up featurebase on both the node and data workers).
* Logs get sent to DataDog?
