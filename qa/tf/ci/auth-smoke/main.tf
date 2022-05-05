module "ci-cluster" {
  source              = "../../.modules/featurebase-cluster"
  cluster_prefix      = var.cluster_prefix
  region              = var.region
  profile             = var.profile
  fb_data_node_type   = "m6g.large"
  fb_data_node_count  = 3
  fb_ingest_type      = "m6g.large"
  vpc_id              = "vpc-05a26a122f961dc2b"
  vpc_cidr_block      = "10.0.0.0/16"
  vpc_public_subnets  = ["subnet-066b4b922b54e51a2", "subnet-037b8884269a69025", "subnet-08482631514426210", ]
  vpc_private_subnets = ["subnet-0319dde319380326f", "subnet-0517ca9a646d80f88", "subnet-05a7b685ed27eb1cf", ]
  user_data = "../../.modules/featurebase-cluster/cloud-init.sh"
}
