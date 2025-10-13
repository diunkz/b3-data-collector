variable "aws_region" {
  description = "Região da AWS onde a infraestrutura será criada"
  type        = string
  default     = "us-east-1" # region
}

variable "project_name" {
  description = "Nome base para os recursos do projeto"
  type        = string
  default     = "data-pipeline" 
}