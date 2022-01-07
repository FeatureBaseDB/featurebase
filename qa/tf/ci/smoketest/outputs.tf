output "ingest_ips" {
    description = "List of ingest IPs"
    value = module.ci-cluster.ingest_ips
}

output "data_node_ips" {
  description = "List of data node IPs"
  value = module.ci-cluster.data_node_ips
}

output "vpc_id" {
<<<<<<< HEAD
  description = "ID of the gauntlet VPC"
  value = module.samsung-cluster.vpc_id
=======
  description = "ID of the VPC"
  value = module.ci-cluster.vpc_id
>>>>>>> 574be963ec450073b549be1c854a18d4b9632720
}