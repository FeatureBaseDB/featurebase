output "ingest_ips" {
    description = "List of ingest IPs"
    value = module.s-cluster.ingest_ips
}

output "data_node_ips" {
  description = "List of data node IPs"
  value = module.s-cluster.data_node_ips
}

output "cluster_prefix" {
  description = "The cluster prefix used"
  value = module.s-cluster.cluster_prefix
}

output "fb_cluster_replica_count" {
  description = "The cluster replica count used"
  value = module.s-cluster.fb_cluster_replica_count
}
