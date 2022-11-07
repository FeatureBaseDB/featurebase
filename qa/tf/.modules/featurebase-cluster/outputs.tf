output "ingest_ips" {
  value = data.aws_instances.ingest_nodes.private_ips
}

output "data_node_ips" {
  value = data.aws_instances.data_nodes.private_ips
}

output "cluster_prefix" {
  value = var.cluster_prefix
}

output "fb_cluster_replica_count" {
  value = var.fb_cluster_replica_count
}

// Provide the ingest and data node information for module outputs
data "aws_instances" "ingest_nodes" {
  instance_tags = {
    Prefix = var.cluster_prefix
    Role   = "ingest_node"
  }
}

data "aws_instances" "data_nodes" {
  instance_tags = {
    Prefix = var.cluster_prefix
    Role   = "cluster_node"
  }
}
