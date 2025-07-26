variable "region" {
  description = "AWS region to deploy resources"
  default     = "ap-southeast-1"
  type        = string
}

variable "data_bucket_name" {
  description = "S3 bucket name for storing data and scripts"
  type        = string
}

variable "environment" {
  description = "Environment tag (e.g., dev, staging, prod)"
  default     = "dev"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

# Redshift credentials
variable "redshift_username" {
  description = "Redshift master username"
  type        = string
}

variable "redshift_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
  validation {
    condition     = length(var.redshift_password) >= 8
    error_message = "Redshift password must be at least 8 characters long."
  }
}

# Redshift configuration
variable "redshift_database" {
  description = "Redshift database name"
  type        = string
  default     = "dev"
}

variable "redshift_schema" {
  description = "Redshift schema name for taxi data"
  type        = string
  default     = "nyc_taxi"
}

variable "redshift_table" {
  description = "Redshift table name for FHVHV data"
  type        = string
  default     = "fhvhv_trips"
}

# Adding common tags variable for consistent resource tagging
variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}
