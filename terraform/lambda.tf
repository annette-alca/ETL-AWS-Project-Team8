# Extract lambda
data "archive_file" "lambda_extract" {
  type        = "zip"
  source_file = "${path.module}/../src/extract/lambda_extract.py" # test file
  output_path = "${path.root}/deployments/lambda_extract.zip"
}

resource "aws_lambda_function" "test_lambda" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.root}/deployments/lambda_extract.zip"
  function_name = "lambda_extract"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "lambda_extract.lambda_extract"
  source_code_hash = data.archive_file.lambda_extract.output_base64sha256
  runtime = "python3.13"

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

#lambda 2


#lambda 3