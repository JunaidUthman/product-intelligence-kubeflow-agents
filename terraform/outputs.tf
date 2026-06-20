output "cluster_name" {
  description = "The name of the EKS cluster"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "Endpoint for EKS control plane"
  value       = module.eks.cluster_endpoint
}

output "kubeconfig_command" {
  description = "Run this command in your terminal to connect your local kubectl to the new AWS cluster"
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${module.eks.cluster_name}"
}

output "efs_file_system_id" {
  description = "The ID of the EFS File System. You will need this to configure the EFS CSI Driver in Kubernetes."
  value       = aws_efs_file_system.kubeflow_data.id
}

output "rds_endpoint" {
  description = "The Endpoint address of the Managed AWS RDS Database. Put this in your k8s/secrets.yaml"
  value       = module.db.db_instance_endpoint
}

output "rds_password" {
  description = "The auto-generated password for the Managed AWS RDS Database. Put this in your k8s/secrets.yaml"
  value       = random_password.db_password.result
  sensitive   = true
}
