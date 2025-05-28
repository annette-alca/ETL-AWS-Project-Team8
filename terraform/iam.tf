# Assume policy document

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Creating Role for lambda

resource "aws_iam_role" "iam_for_lambda" {
  name = "iam_for_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

# Define Lambda S3 full access policy document

data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    effect = "Allow"
    actions = ["s3:*"]
    resources = ["arn:aws:s3:::${aws_s3_bucket.ingestion_s3.bucket}"]
  }

  statement {
    effect = "Allow"
    actions = ["s3:*"]
    resources = ["arn:aws:s3:::${aws_s3_bucket.processed_s3.bucket}"]
  }
}

 # Create Lambda S3 full access policy document
resource "aws_iam_policy" "s3_write_policy" {
  name = "s3-policy-lambda_functions-write"
  policy = data.aws_iam_policy_document.s3_data_policy_doc.json
}

# Attach Lambda S3 full access policy document
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  role = aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
  }