
module "ci-cluster" {
    source = "../../.modules/featurebase-cluster"
    cluster_prefix = "smoke"
    region = var.region
    profile = var.profile
    fb_data_node_type = "m6g.large"
    gitlab_token = var.gitlab_token
}
