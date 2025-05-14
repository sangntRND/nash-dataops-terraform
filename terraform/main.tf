# Main Terraform configuration file for AWS DataOps ETL Demo
# This file sets up the core infrastructure components
terraform {
  backend "s3" {
    bucket  = "dataops-glue-etl-tfstate"
    key     = "terraform/state"
    region  = "us-east-1"
    profile = "cloud-user"
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = merge(
      {
        Environment = var.environment
        Project     = "DataOps-ETL-Demo"
        ManagedBy   = "Terraform"
      },
      var.tags
    )
  }
  profile = "cloud-user"
}

# Create S3 bucket for data storage
resource "aws_s3_bucket" "data_bucket" {
  bucket = var.data_bucket_name

}

# Configure bucket for server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_bucket_encryption" {
  bucket = aws_s3_bucket.data_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Create IAM role for Glue
resource "aws_iam_role" "glue_role" {
  name = "glue-etl-demo-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Attach policies to Glue role
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Add Redshift access policy for Glue
resource "aws_iam_role_policy" "glue_redshift_access" {
  name = "glue-redshift-access-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "redshift:*",
          "redshift-data:*"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-access-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Create Glue Database
resource "aws_glue_catalog_database" "demo_db" {
  name        = "etl_demo_db_${var.environment}"
  description = "Database for ETL demo in ${var.environment} environment"
}

# Upload 2024-01 fhvhv_tripdata data to S3
resource "aws_s3_object" "fhvhv_tripdata_sample_data" {
  bucket = aws_s3_bucket.data_bucket.id
  key    = "raw/fhvhv_trips/2024/01/fhvhv_tripdata.parquet"
  source = "${path.module}/../data/fhvhv_trips/2024/01/fhvhv_tripdata.parquet"
}

# Upload 2024-02 fhvhv_tripdata data to S3
# resource "aws_s3_object" "fhvhv_tripdata_sample_data_2" {
#   bucket = aws_s3_bucket.data_bucket.id
#   key    = "raw/fhvhv_trips/2024/02/fhvhv_tripdata.parquet"
#   source = "${path.module}/../data/fhvhv_trips/2024/02/fhvhv_tripdata.parquet"
# }

# Upload taxi_zone_lookup data to S3
resource "aws_s3_object" "taxi_zone_lookup_sample_data" {
  bucket = aws_s3_bucket.data_bucket.id
  key    = "raw/taxi_zone_lookup.csv"
  source = "${path.module}/../data/taxi_zone_lookup.csv"
  etag   = filemd5("${path.module}/../data/taxi_zone_lookup.csv")
}

# Create S3 directories for processed data
resource "aws_s3_object" "processed_fhvhv_directory" {
  bucket       = aws_s3_bucket.data_bucket.id
  key          = "processed/fhvhv_trips/"
  content_type = "application/x-directory"
}

# Create requirements.txt for Glue Python jobs
resource "aws_s3_object" "glue_requirements" {
  bucket  = aws_s3_bucket.data_bucket.id
  key     = "scripts/requirements.txt"
  content = <<-EOF
  psycopg2-binary==2.9.5
  boto3>=1.24.0
  EOF
}

# Get the default subnet for the default VPC
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Create security group for Redshift access
resource "aws_security_group" "redshift_security_group" {
  name        = "redshift-sg-${var.environment}"
  description = "Security group for Redshift access"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # In production, restrict this to specific IPs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Data resource to get the default VPC
data "aws_vpc" "default" {
  default = true
}

resource "aws_redshift_cluster" "dataops_redshift" {
  cluster_identifier     = "dataops-demo-cluster"
  node_type              = "dc2.large"
  number_of_nodes        = 1
  master_username        = var.redshift_username
  master_password        = var.redshift_password
  iam_roles              = [aws_iam_role.glue_role.arn]
  cluster_type           = "single-node"
  encrypted              = true
  publicly_accessible    = true
  vpc_security_group_ids = [aws_security_group.redshift_security_group.id]

  tags = {
    Name = "dataops-demo-redshift"
  }
}
