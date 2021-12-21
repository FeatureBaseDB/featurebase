# Deploy testing environments with this one wierd trick

This directiory contains Terraform to deploy test environments both ad-hoc and as part of CI/CD pipelines.

The .modules contains the guts of the operation, the things you probably want are in the other directories, each with a README.

## How to Terraform

With terraform installed (`brew install terraform` if not)...

You can do `terraform plan` -> `terraform apply` to spin up a cluster, `terraform destroy` to tear one down.

## Other prerequisites:
Please read these carefully.


