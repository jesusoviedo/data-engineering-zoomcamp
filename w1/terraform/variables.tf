variable "credentials" {
  description = "My Credentials"
  default     = "../keys-cloud/gcp_credentials"
}

variable "project" {
  description = "Project"
  default     = "data-enginnering-rj92"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My bucket name"
  default     = "data-enginnering-rj92-demo-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}