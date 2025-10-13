# 1. Nome do S3 Bucket (CRUCIAL para verificar o salvamento dos Parquets)
output "s3_bucket_name" {
  description = "O ID do S3 Data Lake criado."
  # Referencia o nome do recurso no main.tf (aws_s3_bucket.data_lake_bucket)
  value       = aws_s3_bucket.data_lake_bucket.id
}