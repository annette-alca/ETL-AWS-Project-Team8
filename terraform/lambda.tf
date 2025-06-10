# --------------
# Extract Lambda  
# --------------

# Extract Lambda dependency layer
data "archive_file" "extract_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer_extract/"
  output_path      = "${path.module}/../layer_extract.zip"
}

resource "aws_lambda_layer_version" "extract_dependencies_layer" {
  layer_name          = "extract_dependencies_layer"
  compatible_runtimes = ["python3.13"]
  filename            = data.archive_file.extract_layer.output_path
}

# Extract lambda
data "archive_file" "lambda_extract" {
  type        = "zip"
  source_file = "${path.module}/../src/extract/lambda_extract.py" 
  output_path = "${path.root}/deployments/lambda_extract.zip"
}

resource "aws_lambda_function" "extract_lambda" {
  filename      = "${path.root}/deployments/lambda_extract.zip"
  function_name = "lambda_extract"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "lambda_extract.lambda_extract"
  source_code_hash = data.archive_file.lambda_extract.output_base64sha256
  runtime = "python3.13"
  layers = [aws_lambda_layer_version.extract_dependencies_layer.arn]
  timeout = 300


  environment {
    variables = {

      BACKEND_S3 = "bucket-to-hold-tf-state-for-terraform"
      INGESTION_S3 = aws_s3_bucket.ingestion_s3.bucket
      DBUSER = "project_team_08"
      DBNAME = "totesys"
      HOST = "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
      PORT = 5432
    }
  }
}

# ----------------
# Transform Lambda  
# ----------------

# Transform Lambda dependency layer
data "archive_file" "transform_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer_transform/" 
  output_path      = "${path.module}/../layer_transform.zip" 
}

resource "aws_lambda_layer_version" "transform_dependencies_layer" {
  layer_name          = "transform_dependencies_layer"
  compatible_runtimes = ["python3.13"]
  filename            = data.archive_file.transform_layer.output_path
}

# Transform Lambda
data "archive_file" "lambda_transform" {
  type        = "zip"
  source_file = "${path.module}/../src/transform/lambda_transform.py" 
  output_path = "${path.root}/deployments/lambda_transform.zip"
}

resource "aws_lambda_function" "transform_lambda" {
  filename      = "${path.root}/deployments/lambda_transform.zip"
  function_name = "lambda_transform"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "lambda_transform.lambda_transform"
  source_code_hash = data.archive_file.lambda_transform.output_base64sha256
  runtime = "python3.13"
  layers = [aws_lambda_layer_version.transform_dependencies_layer.arn,
  "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:2" 
  ]
  timeout = 300
  memory_size = 512

  environment {
    variables = {

      BACKEND_S3 = "bucket-to-hold-tf-state-for-terraform"
      INGESTION_S3 = aws_s3_bucket.ingestion_s3.bucket
      PROCESSED_S3 = aws_s3_bucket.processed_s3.bucket
    }
  }
}

# ----------------
# Load Lambda  
# ----------------

# Load Lambda dependency layer
data "archive_file" "load_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer_load/"
  output_path      = "${path.module}/../layer_load.zip"
}

resource "aws_lambda_layer_version" "load_dependencies_layer" {
  layer_name          = "load_dependencies_layer"
  compatible_runtimes = ["python3.13"]
  filename            = data.archive_file.load_layer.output_path
}

# Extract lambda
data "archive_file" "lambda_load" {
  type        = "zip"
  source_file = "${path.module}/../src/load/lambda_load.py" 
  output_path = "${path.root}/deployments/lambda_load.zip"
}

resource "aws_lambda_function" "load_lambda" {
  filename      = "${path.root}/deployments/lambda_load.zip"
  function_name = "lambda_load"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "lambda_load.lambda_load"
  source_code_hash = data.archive_file.lambda_extract.output_base64sha256
  runtime = "python3.13"
  layers = [aws_lambda_layer_version.load_dependencies_layer.arn, 
  "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:2" ]
  timeout = 300
  memory_size = 512


  environment {
    variables = {

      BACKEND_S3 = "bucket-to-hold-tf-state-for-terraform"
      PROCESSED_S3 = aws_s3_bucket.processed_s3.bucket
      DBUSER = "project_team_08"
      DBNAME_WH = "postgres"
      HOST_WH = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
      PORT = 5432
    }
  }
}