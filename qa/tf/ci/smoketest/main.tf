
module "ci-cluster" {
    source = "../../.modules/featurebase-cluster"
    cluster_prefix = var.cluster_prefix
    region = var.region
    profile = var.profile
    fb_data_node_type = "m6g.large"
    fb_data_node_count = 1
    vpc_id = "vpc-05a26a122f961dc2b"
    vpc_cidr_block = "10.0.0.0/16"
    vpc_public_subnets = ["subnet-066b4b922b54e51a2","subnet-037b8884269a69025","subnet-08482631514426210",]
    vpc_private_subnets = ["subnet-050b1219d78f2db1b","subnet-07155281789c6d33b","subnet-0d623c769e086e46e",]
}