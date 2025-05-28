# defining the schedule 
resource "aws_cloudwatch_event_rule" "scheduler" {
  name = "trigger_step_function"
  description = "tigger step function every 20 mins"
  schedule_expression = "rate(20 minutes)" 
  state = "ENABLED"
}

# scheduler targeting state machine 
resource "aws_cloudwatch_event_target" "totes-state-machine" {
  target_id = "totes-state-machine"
  rule = aws_cloudwatch_event_rule.scheduler.name 
  arn = aws_sfn_state_machine.totes-state-machine.arn
  role_arn = aws_iam_role.iam_for_state_machine.arn
  input = jsonencode({})
}