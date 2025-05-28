# -----------------------
# Lambda function section 
# -----------------------

# Assume lambda policy document
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

# ---------------------
# Step function section 
# ---------------------

# Assume role policy for step function 
data "aws_iam_policy_document" "state_machine_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = [
            "states.amazonaws.com",
            "events.amazonaws.com"
          ]
    }

    actions = ["sts:AssumeRole"]
  }
}

# State function IAM role 
resource "aws_iam_role" "iam_for_state_machine" {
  name = "iam_for_state_machine"
  assume_role_policy = data.aws_iam_policy_document.state_machine_assume_role.json
}

# Define step function full access to lambda policy document
data "aws_iam_policy_document" "state_machine_policy_doc" {
  statement {
    effect = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = ["arn:aws:lambda:eu-west-2:215184330991:function:lambda_extract"]
  }
}

# Create state machine access to lambda policy document
resource "aws_iam_policy" "state_machine_lambda_policy" {
  name = "state_machine_lambda_policy"
  policy = data.aws_iam_policy_document.state_machine_policy_doc.json
}

# Attach state machine ploicy document
resource "aws_iam_role_policy_attachment" "state_machine_lambda_policy_attachment" {
  role = aws_iam_role.iam_for_state_machine.name
  policy_arn = aws_iam_policy.state_machine_lambda_policy.arn
  }

# Define policy document to invoke step func 
data "aws_iam_policy_document" "step_function_invoke_policy" {
  statement {
    effect = "Allow"
    actions = [
      "states:StartExecution"
    ]
    resources = [
      aws_sfn_state_machine.totes-state-machine.arn
    ]
  }
}

# Create policy for invoke step func document 
resource "aws_iam_policy" "step_function_invoke_policy" {
  name = "step_function_invoke_policy"
  policy = data.aws_iam_policy_document.step_function_invoke_policy.json
}

# Attach invoke step func policy to role 
resource "aws_iam_role_policy_attachment" "attach_step_function_invoke_policy" {
  role = aws_iam_role.iam_for_state_machine.name
  policy_arn = aws_iam_policy.step_function_invoke_policy.arn
}

# ---------------------------
# Cloudwatch function section 
# ---------------------------

# CloudWatch logs policy document
data "aws_iam_policy_document" "cw_document" {
  statement {
    effect = "Allow"
    actions = [
        "logs:DescribeLogGroups",
        "logs:DescribeResourcePolicies",
        "logs:PutResourcePolicy",
        "logs:CreateLogDelivery",
        "logs:GetLogDelivery",
        "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery",
        "logs:ListLogDeliveries"
    ]
    resources = [
        "*"
    ]
  }

  statement {
    effect = "Allow"
     actions = [
        "logs:CreateLogStream",
        "logs:PutLogEvents",
    ]
    resources = [
        "${aws_cloudwatch_log_group.log_group_for_sfn.arn}"
    ]
  }
}

# CloudWatch log policy
resource "aws_iam_policy" "cw_policy" {
  name = "cw_policy"
  policy = data.aws_iam_policy_document.cw_document.json
}

# CloudWatch log policy attachment for step function 
resource "aws_iam_role_policy_attachment" "state_machine_cw_policy_attachment" {
  role = aws_iam_role.iam_for_state_machine.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

# # test 
# resource "aws_cloudwatch_log_resource_policy" "cloudwatch_policy" {
#   policy_document = data.aws_iam_policy_document.cw_document.json
#   policy_name     = "cloudwatch_policy"
# }