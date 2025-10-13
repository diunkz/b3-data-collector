variable "aws_region" {
  description = "Região da AWS onde a infraestrutura será criada"
  type        = string
  default     = "us-east-1" # Defina sua região padrão
}

variable "project_name" {
  description = "Nome base para os recursos do projeto"
  type        = string
  default     = "data-pipeline" 
}

variable "lab_execution_role_name" {
    description = "O NOME da Role de execução do Laboratório que já existe (ex: LabRole)."
    type        = string
    # 🚨 AJUSTE NECESSÁRIO: Substitua "LabRole" se o nome da sua role for diferente.
    default     = "LabRole" 
}