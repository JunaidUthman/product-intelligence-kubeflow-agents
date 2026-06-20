variable "aws_region" {
  description = "The AWS region to deploy all resources into"
  type        = string
  default     = "eu-west-1" # Change this to your preferred region (e.g., us-east-1)
}

# Note: In the future, you can pass your secrets into Terraform via environment 
# variables (TF_VAR_deepseek_api_key) to automatically populate Kubernetes secrets.
variable "deepseek_api_key" {
  description = "DeepSeek API Key for scraping agents"
  type        = string
  sensitive   = true
  default     = ""
}

variable "hf_token" {
  description = "Hugging Face API Token for model training"
  type        = string
  sensitive   = true
  default     = ""
}
