
module "ci-cluster" {
    source = "../../.modules/featurebase-cluster"
    cluster_prefix = var.cluster_prefix
    region = var.region
    profile = var.profile
    fb_data_node_type = "m6g.large"
    fb_data_node_count = 1
    gitlab_token = var.gitlab_token
    branch = var.branch 
}

resource "aws_vpc_peering_connection" "smoketest-to-vpn" {
#  peer_vpc_id   = aws_vpc.bar.id
  vpc_id        = module.ci-cluster.vpc_id
  peer_vpc_id   = "vpc-0cb7cf76f2079aa0e"
  auto_accept = true
}