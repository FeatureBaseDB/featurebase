module "ci-cluster" {
    source = "../../.modules/featurebase-cluster"
    cluster_prefix = var.cluster_prefix
    region = var.region
    profile = var.profile
    fb_cluster_arch = ["x86_64"]
    fb_data_node_type = "m5ad.16xlarge" 
    fb_data_node_count = 5
    ebs_volumes = []
    fb_ingest_type = "m4.16xlarge"
    fb_ingest_node_count = 2
    fb_ingest_disk_size_gb = 1200
    vpc_id = "vpc-05a26a122f961dc2b"
    vpc_cidr_block = "10.0.0.0/16"
    vpc_public_subnets = ["subnet-066b4b922b54e51a2","subnet-037b8884269a69025","subnet-08482631514426210",]
    vpc_private_subnets = ["subnet-0319dde319380326f","subnet-0517ca9a646d80f88","subnet-05a7b685ed27eb1cf",]
    user_data = "./delete_cloud_init.sh"
    use_spot_instances = true
}