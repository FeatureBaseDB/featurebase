module "samsung-cluster" {
    source = "../../.modules/featurebase-cluster"
    cluster_prefix = "samsung-gauntlet"
    region = var.region
    profile = var.profile
    fb_data_node_type = "m6g.xlarge"
    fb_data_disk_iops = 10000
    fb_data_node_count = 3
    fb_ingest_type = "m6g.large"
    fb_ingest_disk_iops = 10000
    fb_ingest_node_count = 1
    gitlab_token = var.gitlab_token
    branch = var.branch
}

resource "aws_vpc_peering_connection" "gauntlet-to-vpn" {
  vpc_id        = module.samsung-cluster.vpc_id
  peer_vpc_id   = "vpc-0cb7cf76f2079aa0e"
  auto_accept = true
}