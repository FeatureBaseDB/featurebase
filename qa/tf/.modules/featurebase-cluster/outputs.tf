output "ingest_ips" {
    value = var.use_spot_instances ? aws_spot_instance_request.fb_ingest.*.private_ip : aws_instance.fb_ingest.*.private_ip
}

output "data_node_ips" {
    value = var.use_spot_instances ? aws_spot_instance_request.fb_cluster_nodes.*.private_ip : aws_instance.fb_cluster_nodes.*.private_ip 
}

output "cluster_prefix" {
    value = var.cluster_prefix
}

output "fb_cluster_replica_count" {
    value = var.fb_cluster_replica_count
}
