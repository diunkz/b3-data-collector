# run: terraform import aws_athena_workgroup.primary_workgroup primary

# ----------------------------------------------------------------------
# 0. DATA SOURCE E LOCALS (Busca IDs e Constrói ARNs)
# ----------------------------------------------------------------------

# Busca o ID da conta do usuário que está rodando o Terraform
data "aws_caller_identity" "current" {}

locals {
  # ARN do Layer Público AWSSDKPandas (Python 3.13)
  pandas_layer_arn_dynamic = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313:4" 
  # pandas_layer_arn_dynamic = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:31"
  # pandas_layer_arn_dynamic = "arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p312-pandas:17"
  # Constrói o ARN da LabRole dinamicamente
  lab_execution_role_arn_dynamic = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.lab_execution_role_name}"
}


# ----------------------------------------------------------------------
# 1. ARMAZENAMENTO (S3 DATA LAKE)
# ----------------------------------------------------------------------

resource "aws_s3_bucket" "data_lake_bucket" {
  # Usa o ID da conta para garantir a unicidade global
  bucket = "${data.aws_caller_identity.current.account_id}-${var.project_name}-data-lake-bucket"
  
  tags = {
    Name = "B3 Data Lake - Raw and Refined"
  }
}

# ----------------------------------------------------------------------
# 2. UPLOAD DE SCRIPTS E CÓDIGO PARA S3 (STAGING)
# ----------------------------------------------------------------------

# 2.1. Upload do Script GLUE ETL (etl_processor.py)
resource "aws_s3_object" "glue_etl_script_upload" {
  bucket = aws_s3_bucket.data_lake_bucket.id
  key    = "glue-scripts/etl_processor.py"
  source = "../etl_processor.py" 
  etag   = filemd5("../etl_processor.py") 
}

# 2.2. Upload do Código Lambda COLLECTOR (Scraper - Solução para o limite de 50MB)
resource "aws_s3_object" "lambda_collector_code_upload" {
  bucket = aws_s3_bucket.data_lake_bucket.id
  key    = "lambda-code/b3-collector-code.zip"
  source = "../lambda_package.zip" 
  etag   = filemd5("../lambda_package.zip")
}

# # 2.3. Upload do Código Lambda TRIGGER (O orquestrador do Glue)
# resource "aws_s3_object" "lambda_trigger_code_upload" {
#   bucket = aws_s3_bucket.data_lake_bucket.id
#   key    = "lambda-code/glue-trigger-code.zip"
#   source = "../glue_trigger_package.zip" # Este arquivo deve estar na pasta atual (pipeline-tf/)
#   etag   = filemd5("../glue_trigger_package.zip")
# }


# ----------------------------------------------------------------------
# 3. AWS LAMBDA FUNCTION (SCRAPING - Coleta Inicial)
# ----------------------------------------------------------------------

resource "aws_lambda_function" "b3_collector_lambda" {
  function_name    = "${var.project_name}-b3-collector"
  role             = local.lab_execution_role_arn_dynamic 
  handler          = "lambda_handler.lambda_handler" 
  runtime          = "python3.13"
  layers           = [local.pandas_layer_arn_dynamic]

  # Aponta para o ZIP que está no S3 (Staging)
  s3_bucket        = aws_s3_object.lambda_collector_code_upload.bucket
  s3_key           = aws_s3_object.lambda_collector_code_upload.key
  
  kms_key_arn      = "" 
  timeout          = 300
  memory_size      = 512 

  environment {
        variables = {
            S3_BUCKET_NAME = aws_s3_bucket.data_lake_bucket.id
            GLUE_JOB_NAME = aws_glue_job.ibov_etl_job.name 
        }
  }
  
  depends_on       = [aws_s3_object.lambda_collector_code_upload]
}

# ----------------------------------------------------------------------
# 4. AGENDAMENTO DIÁRIO (EVENTBRIDGE)
# ----------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "daily_schedule" {
  name                = "${var.project_name}-daily-run"
  schedule_expression = "cron(0 23 ? * MON-FRI *)" 
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.daily_schedule.name
  target_id = "B3CollectorLambda"
  arn       = aws_lambda_function.b3_collector_lambda.arn
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.b3_collector_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_schedule.arn
}

# # ----------------------------------------------------------------------
# # 5. NOVA FUNÇÃO LAMBDA (TRIGGER GLUE - Requisito 4)
# # ----------------------------------------------------------------------

# resource "aws_lambda_function" "glue_trigger_lambda" {
#   function_name    = "${var.project_name}-glue-trigger" 
#   role             = local.lab_execution_role_arn_dynamic 
#   handler          = "glue_trigger.lambda_handler"
#   runtime          = "python3.13"
#   timeout          = 300 

#   # Aponta para o ZIP que está no S3 (Staging)
#   s3_bucket        = aws_s3_object.lambda_trigger_code_upload.bucket
#   s3_key           = aws_s3_object.lambda_trigger_code_upload.key
  
#   environment {
#     variables = {
#       GLUE_JOB_NAME = aws_glue_job.ibov_etl_job.name
#     }
#   }
#   depends_on = [aws_s3_object.lambda_trigger_code_upload]
# }

# # ----------------------------------------------------------------------
# # 6. GATILHO S3 (S3 -> LAMBDA) - Requisito 3
# # ----------------------------------------------------------------------

# resource "aws_lambda_permission" "allow_s3_to_invoke_glue_trigger" {
#   statement_id  = "AllowExecutionFromS3"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.glue_trigger_lambda.function_name
#   principal     = "s3.amazonaws.com"
#   source_arn    = aws_s3_bucket.data_lake_bucket.arn 
#   source_account = data.aws_caller_identity.current.account_id # <<< Adiciona o ID da Conta
# }

# resource "aws_s3_bucket_notification" "trigger_glue_etl" {
#   bucket = aws_s3_bucket.data_lake_bucket.id

#   lambda_function {
#     lambda_function_arn = aws_lambda_function.glue_trigger_lambda.arn
#     events              = ["s3:ObjectCreated:*"] 
    
#     filter_prefix       = "raw/ibov/" 
#     filter_suffix       = ".parquet" 
#   }
  
#   depends_on = [aws_lambda_permission.allow_s3_to_invoke_glue_trigger]
# }

# ----------------------------------------------------------------------
# 7. GLUE ETL JOB (Requisito 5 e 6)
# ----------------------------------------------------------------------

resource "aws_glue_job" "ibov_etl_job" {
  name             = "${var.project_name}-ibov-etl-job"
  
  role_arn         = local.lab_execution_role_arn_dynamic 
  
  glue_version     = "4.0"
  worker_type      = "G.1X" 
  number_of_workers = 2 

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_lake_bucket.id}/glue-scripts/etl_processor.py" 
    python_version  = "3"
  }
  
  # A dependência do script já está implícita via script_location.
  depends_on       = [aws_s3_object.glue_etl_script_upload] 
}

# ----------------------------------------------------------------------
# 8. GLUE CRAWLER (Requisito 7 e 8)
# ----------------------------------------------------------------------

resource "aws_glue_crawler" "refined_data_crawler" {
  name             = "${var.project_name}-refined-crawler"
  
  role             = local.lab_execution_role_arn_dynamic 
  
  database_name    = "default" 

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake_bucket.id}/refined/ibov_cleaned/" 
  }
}

# ----------------------------------------------------------------------
# 9. ATHENA WORKGROUP (Configuração do Local de Saída)
# ----------------------------------------------------------------------

resource "aws_athena_workgroup" "primary_workgroup" {
  # O workgroup padrão na AWS é sempre 'primary'
  name = "primary"
  
  # Força o Terraform a importar o recurso existente se ele já foi criado
  force_destroy = false 
  
  # Configuração da saída de consultas
  configuration {
    result_configuration {
      # 💥 CRÍTICO: Define o local de saída da consulta 💥
      output_location = "s3://${aws_s3_bucket.data_lake_bucket.id}/athena-results/"
      
      # Opcional, mas recomendado: Criptografia dos resultados
      /*
      encryption_configuration {
        encryption_option = "SSE_S3" 
      }
      */
    }
  }
}