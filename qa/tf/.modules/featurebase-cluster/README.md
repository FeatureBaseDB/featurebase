# Summary

This module provisions a VPC, subnets, instances, keys, and security groups needed for a basic featurebase cluster running in AWS. It is meant to be used as a module. For example:

```hcl
module "featurebase" {
  source "/path/to/module/"
  cluster_prefix = "sprockets"
  azs = ["us-east-1a", "us-east-1b", "us-east-1c"]
}
```

The path to the module is wherever the `featurebase-cloud` directory is. So if you have put it in `/var/opt/terraform/modules/featurebase-cloud` then calling the module would look like:

```hcl
module "featurebase" {
  source "/var/opt/terraform/modules/featurebase-cloud"
  cluster_prefix = "sprockets"
}
```

Much more is configurable; for a complete list, look in `variables.tf`. Reasonable defaults have been set.

## AWS Access

Please make sure you have set up your AWS access in either environment variables, or in the credentials file.

Some useful links for this are:

AWS Environment Variables <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html>

## State

State is currently kept locally, for as this is intended for PoCs. It can be stored in a remote s3 or GCS bucket if desired.


 