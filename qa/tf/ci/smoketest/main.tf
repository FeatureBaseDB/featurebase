
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

<<<<<<< HEAD
resource "aws_vpc_peering_connection" "gauntlet-to-vpn" {
=======
resource "aws_vpc_peering_connection" "smoketest-to-vpn" {
#  peer_vpc_id   = aws_vpc.bar.id
>>>>>>> 574be963ec450073b549be1c854a18d4b9632720
  vpc_id        = module.ci-cluster.vpc_id
  peer_vpc_id   = "vpc-0cb7cf76f2079aa0e"
  auto_accept = true
}