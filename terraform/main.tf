provider "aws" {
  region = var.aws_region
}

# ==========================================================
# 1. VPC (Networking)
# ==========================================================
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "product-intel-vpc"
  cidr = "10.0.0.0/16"

  azs              = ["${var.aws_region}a", "${var.aws_region}b"]
  private_subnets  = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets   = ["10.0.101.0/24", "10.0.102.0/24"]
  intra_subnets    = ["10.0.201.0/24", "10.0.202.0/24"]
  
  # NEW: Subnets exclusively for the Managed Database
  database_subnets = ["10.0.151.0/24", "10.0.152.0/24"]
  create_database_subnet_group = true

  enable_nat_gateway = true
  single_nat_gateway = true
}

# ==========================================================
# 2. Minimalist EKS Cluster
# ==========================================================
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = "product-intel-cluster"
  cluster_version = "1.30"
  
  cluster_endpoint_public_access = true

  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.private_subnets
  control_plane_subnet_ids = module.vpc.intra_subnets

  eks_managed_node_groups = {
    minimal = {
      instance_types = ["t3.large"] 
      min_size     = 1
      max_size     = 3
      desired_size = 2
    }
  }
}

# ==========================================================
# 3. Shared File System (EFS) for Parallel Scraping
# ==========================================================
resource "aws_efs_file_system" "kubeflow_data" {
  creation_token = "product-intel-efs"
  tags = {
    Name = "product-intel-efs"
  }
}

resource "aws_efs_mount_target" "mounts" {
  count           = length(module.vpc.private_subnets)
  file_system_id  = aws_efs_file_system.kubeflow_data.id
  subnet_id       = module.vpc.private_subnets[count.index]
  security_groups = [module.eks.node_security_group_id]
}

# ==========================================================
# 4. Managed Database (AWS RDS MySQL)
# ==========================================================
resource "aws_security_group" "rds_sg" {
  name        = "product-intel-rds-sg"
  description = "Allow EKS nodes to access RDS MySQL"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    # ONLY allow the EKS cluster to talk to the database (Highly Secure)
    security_groups = [module.eks.node_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

module "db" {
  source  = "terraform-aws-modules/rds/aws"
  version = "~> 6.0"

  identifier = "product-intel-db"

  engine               = "mysql"
  engine_version       = "8.0"
  family               = "mysql8.0"
  major_engine_version = "8.0"
  
  # t3.micro is the cheapest minimalist DB instance
  instance_class       = "db.t3.micro"

  allocated_storage     = 20
  max_allocated_storage = 100

  db_name  = "product_intelligence"
  username = "admin"
  port     = 3306
  
  # Terraform will automatically generate a highly secure password
  manage_master_user_password = false
  password = random_password.db_password.result

  multi_az               = false
  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]

  # Minimalist/Dev settings: Disable deletion protection so you can easily destroy it
  deletion_protection = false
  skip_final_snapshot = true
}

resource "random_password" "db_password" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}
