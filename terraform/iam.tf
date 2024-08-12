resource "aws_iam_role" "lambda_role" {
    name_prefix         = "role-etl-lambda-"
    assume_role_policy  = data.aws_iam_policy_document.lambda_assume_role_policy.json
}

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
}

data "aws_iam_policy_document" "s3_list_bucket" {
  statement {

    actions = ["s3:ListBucket"]

    resources = [
      "${aws_s3_bucket.processed_data_bucket.arn}",
      "${aws_s3_bucket.raw_data_bucket.arn}",
    ]
  }
}

data "aws_iam_policy_document" "s3_read_write_object" {
  statement {

    actions = ["s3:PutObject", "s3:GetObject"]

    resources = [
      "${aws_s3_bucket.processed_data_bucket.arn}/*",
      "${aws_s3_bucket.raw_data_bucket.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
}

resource "aws_iam_policy" "s3_read_write_object_policy" {
    name_prefix = "s3-object-policy-etl-lambdas-"
    policy = data.aws_iam_policy_document.s3_read_write_object.json
}

resource "aws_iam_policy" "s3_list_bucket_policy" {
    name_prefix = "s3-bucket-policy-etl-lambdas-"
    policy = data.aws_iam_policy_document.s3_list_bucket.json
}

resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cloudwatch-policy-etl-lambdas-"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "s3_read_write_object_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_object_policy.arn
}

resource "aws_iam_role_policy_attachment" "s3_list_bucket_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_list_bucket_policy.arn
}

resource "aws_iam_role_policy_attachment" "cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

