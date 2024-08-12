resource "aws_iam_role" "lambda_role" {
    name_prefix         = "role-etl-lambda-"
    assume_role_policy  = data.aws_iam_policy_document.lambda_assume_role_policy.json
} # Lambda

resource "aws_iam_role" "iam_for_sfn" {
    name_prefix        = "role-etl-sfn-"
    assume_role_policy = data.aws_iam_policy_document.state_machine_assume_role_policy.json
} # Step Func

data "aws_iam_policy_document" "lambda_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
} # Lambda

data "aws_iam_policy_document" "state_machine_assume_role_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
} # Step Func

data "aws_iam_policy_document" "s3_list_bucket" {
  statement {

    actions = ["s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.processed_data_bucket.arn}",
      "${aws_s3_bucket.raw_data_bucket.arn}",
    ]
  }
} # Lambda - s3 buckets

data "aws_iam_policy_document" "s3_read_write_object" {
  statement {

    actions = ["s3:PutObject", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.processed_data_bucket.arn}/*",
      "${aws_s3_bucket.raw_data_bucket.arn}/*",
    ]
  }
} # Lambda - s3 objects

data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:*:*"
    ]
  }
} # Lambda - cloudwatch

data "aws_iam_policy_document" "lambda_invoke_document" {
  statement {
    actions = ["lambda:InvokeFunction"]

    resources = [
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.extract_lambda}:*",
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transform_lambda}:*",
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.load_lambda}:*",
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.extract_lambda}",
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transform_lambda}",
        "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.load_lambda}"
    ]
  }
} # Step func - Lambda invoke

data "aws_iam_policy_document" "x_ray_document" {
  statement {
     actions = ["xray:PutTraceSegments",
                "xray:PutTelemetryRecords",
                "xray:GetSamplingRules",
                "xray:GetSamplingTargets"]
     resources = ["*"]
  }
} # Step func - xray

resource "aws_iam_policy" "s3_read_write_object_policy" {
    name_prefix = "s3-object-policy-etl-lambdas-"
    policy = data.aws_iam_policy_document.s3_read_write_object.json
} # Lambda

resource "aws_iam_policy" "s3_list_bucket_policy" {
    name_prefix = "s3-bucket-policy-etl-lambdas-"
    policy = data.aws_iam_policy_document.s3_list_bucket.json
} # Lambda

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cloudwatch-policy-etl-lambdas-"
    policy = data.aws_iam_policy_document.cw_document.json
} # Lambda

resource "aws_iam_policy" "lambda_invoke_policy" {
  name_prefix = "step-func-lambda-invoke-"
  policy      = data.aws_iam_policy_document.lambda_invoke_document.json
} # Step Func

resource "aws_iam_policy" "xray_policy" {
  name_prefix = "step-func-xray-"
  policy      = data.aws_iam_policy_document.x_ray_document.json
} # Step Func


resource "aws_iam_role_policy_attachment" "s3_read_write_object_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_object_policy.arn
} # Lambda

resource "aws_iam_role_policy_attachment" "s3_list_bucket_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_list_bucket_policy.arn
} # Lambda

resource "aws_iam_role_policy_attachment" "cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
} # Lambda

resource "aws_iam_role_policy_attachment" "lambda_invoke_policy_attachment" {
  role       = aws_iam_role.iam_for_sfn.name
  policy_arn = aws_iam_policy.lambda_invoke_policy.arn
} # Step Func

resource "aws_iam_role_policy_attachment" "xray_policy_attachment" {
  role       = aws_iam_role.iam_for_sfn.name
  policy_arn = aws_iam_policy.xray_policy.arn
} # Step Func


