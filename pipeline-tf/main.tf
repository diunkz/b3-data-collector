# ----------------------------------------------------------------------
# 0. DATA SOURCE E LOCALS (Busca IDs e Constrói ARNs)
# ----------------------------------------------------------------------

# Busca o ID da conta do usuário que está rodando o Terraform
data "aws_caller_identity" "current" {}

locals {
  # 💥 SUBSTITUIÇÃO: ARN do Layer Público AWSSDKPandas (Python 3.13) definido explicitamente.
  # Isso resolve o erro de 'empty result' do Data Source.
  pandas_layer_arn_dynamic = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313:4" 
  
  # Constrói o ARN da LabRole dinamicamente (usando o nome da Role de variables.tf)
  lab_execution_role_arn_dynamic = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.lab_execution_role_name}"
}


# ----------------------------------------------------------------------
# 1. ARMAZENAMENTO (S3 DATA LAKE) - (Permanece inalterado)
# ----------------------------------------------------------------------

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = "${data.aws_caller_identity.current.account_id}-${var.project_name}-data-lake-bucket"
  
  tags = {
    Name = "B3 Data Lake - Raw and Refined"
  }
}

# ----------------------------------------------------------------------
# 2. AWS LAMBDA FUNCTION (Função principal e agendamento)
# ----------------------------------------------------------------------

resource "aws_lambda_function" "b3_collector_lambda" {
  function_name    = "${var.project_name}-b3-collector"
  
  # Usa o ARN da LabRole construído dinamicamente
  role             = local.lab_execution_role_arn_dynamic 
  
  handler          = "lambda_handler.lambda_handler" 
  runtime          = "python3.13" 
  
  # Anexa o Layer usando o ARN 'local' (que agora está hardcoded/estável)
  layers           = [local.pandas_layer_arn_dynamic]

  # Configuração para o upload do código ZIP
  filename         = "../lambda_package.zip"
  source_code_hash = filebase64sha256("../lambda_package.zip") 
  
  kms_key_arn      = "" 
  
  timeout          = 60
  memory_size      = 512 

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.data_lake_bucket.id
    }
  }
}

# ----------------------------------------------------------------------
# 3. AGENDAMENTO DIÁRIO (EVENTBRIDGE) - (Permanece inalterado)
# ----------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "daily_schedule" {
  name                = "${var.project_name}-daily-run"
  schedule_expression = "cron(0 12 * * ? *)" 
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