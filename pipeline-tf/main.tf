# ----------------------------------------------------------------------
# 0. DATA SOURCE (Busca o ID da Conta AWS)
# ----------------------------------------------------------------------

data "aws_caller_identity" "current" {}


# ----------------------------------------------------------------------
# 1. ARMAZENAMENTO (S3 DATA LAKE)
# ----------------------------------------------------------------------

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = "${data.aws_caller_identity.current.account_id}-${var.project_name}-data-lake-bucket"
  
  tags = {
    Name = "B3 Data Lake - Raw and Refined"
  }
}


# ----------------------------------------------------------------------
# 2. AWS LAMBDA LAYER (Para Pandas/NumPy > 50MB)
# ----------------------------------------------------------------------

# 2.1. Upload do ZIP GRANDE para o S3 (Staging)
# Isto permite que o Terraform lide com arquivos maiores que 50MB.
resource "aws_s3_object" "lambda_layer_upload" {
  # Referencia o Bucket que acabamos de criar
  bucket = aws_s3_bucket.data_lake_bucket.id 
  
  # Onde o ZIP será salvo dentro do S3
  key    = "layers/python_dependencies_layer.zip" 
  
  # Nome do arquivo ZIP local (na sua máquina)
  source = "../lambda_package.zip" 
  
  # O Terraform recarregará o Layer se o conteúdo do ZIP mudar
  etag   = filemd5("../lambda_package.zip")
}

# 2.2. Registro do Layer no Serviço Lambda
# Este recurso aponta para o arquivo que está no S3.
resource "aws_lambda_layer_version" "dependencies_layer" {
  layer_name          = "${var.project_name}-python-dependencies"
  
  # Referencia o S3 Bucket e a Chave do objeto que acabamos de subir
  s3_bucket           = aws_s3_bucket.data_lake_bucket.id
  s3_key              = aws_s3_object.lambda_layer_upload.key
  
  compatible_runtimes = ["python3.13"]
  compatible_architectures = ["x86_64"] # Use a arquitetura do seu build Docker (amd64)
  
  # Garante que o objeto S3 seja carregado antes de tentar criar o Layer
  depends_on = [
    aws_s3_object.lambda_layer_upload
  ]
}