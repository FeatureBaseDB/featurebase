output "ingest_ips" {
    value = aws_instance.fb_ingest.*.public_ip
}

output "data_node_ips" {
    value = aws_instance.fb_cluster_nodes.*.private_ip
}

output "cluster_prefix" {
    value = var.cluster_prefix
}

output "fb_cluster_replica_count" {
    value = var.fb_cluster_replica_count
}
