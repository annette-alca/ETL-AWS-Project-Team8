resource "aws_sfn_state_machine" "totes-state-machine" {
  name     = "totes-state-machine"
  role_arn = aws_iam_role.iam_for_state_machine.arn

  definition = <<EOF
{
  "Comment": "returns team 8 test",
  "StartAt": "lambda_extract",
  "States": {
    "lambda_extract": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.test_lambda.arn}",
      "End": true
    }
  }
}
EOF

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.log_group_for_sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

resource "aws_cloudwatch_log_group" "log_group_for_sfn" {
  name = "log_group_for_sfn"
}


# resource "null_resource" "enable_step_function_logging" {
#       triggers = {
#     state_machine_arn  = aws_sfn_state_machine.totes-state-machine.arn
#     logs_params=<<PARAMS
#     {
#         "level":"ALL",
#         "includeExecutionData":true,
#         "destinations":[
#             {
#                 "cloudWatchLogsLogGroup":{
#                     "logGroupArn":"${aws_cloudwatch_log_group.log_group_for_sfn.arn}:*"
#                     }
#                 }
#             ]
#             }
#     PARAMS
#     }
# }
