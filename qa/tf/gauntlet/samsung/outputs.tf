output "ingest_ips" {
    description = "List of ingest IPs"
    value = module.samsung-cluster.ingest_ips
}

output "data_node_ips" {
  description = "List of data node IPs"
  value = module.samsung-cluster.data_node_ips
}

output "vpc_id" {
  description = "ID of the gauntlet VPC"
  value = module.samsung-cluster.vpc_id
}