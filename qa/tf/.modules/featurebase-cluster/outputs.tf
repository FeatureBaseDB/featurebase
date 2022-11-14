output "ingest_ips" {
  value = var.use_spot_instances ? data.aws_instances.ingest_nodes.private_ips : aws_instance.fb_ingest.*.private_ip
}

output "data_node_ips" {
  value = var.use_spot_instances ? data.aws_instances.data_nodes.private_ips : aws_instance.fb_cluster_nodes.*.private_ip
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
