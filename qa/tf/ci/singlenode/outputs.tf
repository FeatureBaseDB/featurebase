output "ingest_ips" {
    description = "List of ingest IPs"
    value = module.ci-cluster.ingest_ips
}

output "data_node_ips" {
  description = "List of data node IPs"
  value = module.ci-cluster.data_node_ips
}