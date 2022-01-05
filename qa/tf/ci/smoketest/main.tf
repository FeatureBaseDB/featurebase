
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
