output "ingest_ips" {
    description = "List of ingest IPs"
    value = module.ci-cluster.ingest_ips
}

output "data_node_ips" {
  description = "List of data node IPs"
  value = module.ci-cluster.data_node_ips
}

output "cluster_prefix" {
  description = "The cluster prefix used"
  value = module.ci-cluster.cluster_prefix
}

output "fb_cluster_replica_count" {
  description = "The cluster replica count used"
  value = module.ci-cluster.fb_cluster_replica_count
}