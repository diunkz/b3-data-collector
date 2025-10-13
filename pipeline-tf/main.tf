# Um S3 Bucket é o "ponto de partida" para Glue e Athena
resource "aws_s3_bucket" "data_lake_bucket" {
  # Mude o nome do bucket para refletir que ele contém todo o Data Lake
  bucket = "${var.project_name}-data-lake-bucket"
  
  tags = {
    Name = "B3 Data Lake - Raw and Refined"
  }
}