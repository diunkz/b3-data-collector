terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" # Use uma versão recente
    }
  }
}

# Configura o provedor AWS (aqui ele pegará suas credenciais)
provider "aws" {
  region = var.aws_region # Define a região da AWS
}